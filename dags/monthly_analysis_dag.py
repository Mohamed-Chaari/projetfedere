import sys
import os
import logging
from datetime import datetime, timedelta, timezone

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.exceptions import AirflowException
import pandas as pd
from psycopg2.extras import execute_batch

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from src.utils.db import get_connection, get_engine, log_pipeline_run

logger = logging.getLogger(__name__)

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='monthly_analysis',
    default_args=default_args,
    description='Compute monthly weather averages',
    schedule='0 7 * * *',
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['analysis', 'monthly'],
) as dag:

    def check_data_freshness_fn():
        conn = get_connection()
        try:
            with conn.cursor() as cursor:
                cursor.execute("SELECT MAX(date) FROM weather_historical")
                max_date = cursor.fetchone()[0]
                if not max_date:
                    raise AirflowException("Data stale")

                logger.info(f"Latest date in weather_historical: {max_date}")

                today = datetime.now(timezone.utc).date()
                two_days_ago = today - timedelta(days=2)

                if isinstance(max_date, datetime):
                    max_date = max_date.date()

                if max_date < two_days_ago:
                    raise AirflowException("Data stale")
        finally:
            conn.close()

    def compute_monthly_averages_fn():
        engine = get_engine()

        query = "SELECT * FROM weather_historical"
        df = pd.read_sql(query, engine)

        if df.empty:
            logger.info("No data to process.")
            return {'months_computed': 0, 'cities': 0}

        df['date'] = pd.to_datetime(df['date'])

        df['year'] = df['date'].dt.year
        df['month'] = df['date'].dt.month

        grouped = df.groupby(['city', 'governorate', 'region', 'year', 'month']).agg(
            avg_temp=('temperature', 'mean'),
            max_temp=('temperature', 'max'),
            min_temp=('temperature', 'min'),
            avg_humidity=('humidity', 'mean'),
            avg_precip=('precipitation', 'mean'),
            avg_wind=('wind_speed', 'mean'),
            record_count=('date', 'count')
        ).reset_index()

        cols_to_round = ['avg_temp', 'max_temp', 'min_temp', 'avg_humidity', 'avg_precip', 'avg_wind']
        grouped[cols_to_round] = grouped[cols_to_round].round(2)

        if (grouped['avg_temp'] < -5).any() or (grouped['avg_temp'] > 55).any():
            raise ValueError("avg_temp is outside the valid range [-5, 55]")

        upsert_query = """
            INSERT INTO monthly_averages
                (year, month, city, governorate, region,
                 avg_temp, max_temp, min_temp, avg_humidity,
                 avg_precip, avg_wind, record_count, computed_at)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW())
            ON CONFLICT (year, month, city) DO UPDATE SET
                avg_temp=EXCLUDED.avg_temp,
                max_temp=EXCLUDED.max_temp,
                min_temp=EXCLUDED.min_temp,
                avg_humidity=EXCLUDED.avg_humidity,
                avg_precip=EXCLUDED.avg_precip,
                avg_wind=EXCLUDED.avg_wind,
                record_count=EXCLUDED.record_count,
                computed_at=NOW()
        """

        records_to_insert = grouped[
            ['year', 'month', 'city', 'governorate', 'region',
             'avg_temp', 'max_temp', 'min_temp', 'avg_humidity',
             'avg_precip', 'avg_wind', 'record_count']
        ].values.tolist()

        conn = get_connection()
        try:
            with conn.cursor() as cursor:
                execute_batch(cursor, upsert_query, records_to_insert)
            conn.commit()
        finally:
            conn.close()

        return {'months_computed': len(grouped), 'cities': grouped['city'].nunique()}


    def verify_results_fn():
        conn = get_connection()
        try:
            with conn.cursor() as cursor:
                cursor.execute("SELECT COUNT(*) FROM monthly_averages")
                count = cursor.fetchone()[0]
                logger.info(f"Monthly averages table: {count} rows")
        finally:
            conn.close()

    def log_run_fn(**kwargs):
        started_at = kwargs['data_interval_start']
        rows = kwargs['ti'].xcom_pull(task_ids='compute_monthly_averages')
        rows_processed = rows.get('months_computed', 0) if rows else 0

        log_pipeline_run(
            dag_name='monthly_analysis',
            task_name='full_run',
            started_at=started_at,
            rows_processed=rows_processed,
            status='success'
        )

    t1 = PythonOperator(
        task_id='check_data_freshness',
        python_callable=check_data_freshness_fn,
    )

    t2 = PythonOperator(
        task_id='compute_monthly_averages',
        python_callable=compute_monthly_averages_fn,
    )

    t3 = PythonOperator(
        task_id='verify_results',
        python_callable=verify_results_fn,
    )

    t4 = PythonOperator(
        task_id='log_run',
        python_callable=log_run_fn,
    )

    t1 >> t2 >> t3 >> t4
