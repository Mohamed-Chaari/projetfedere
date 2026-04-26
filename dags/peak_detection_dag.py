import sys
import os
import logging
from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
import pandas as pd
import numpy as np
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
    dag_id='peak_detection',
    default_args=default_args,
    schedule='0 7 * * *',
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['analysis', 'peaks', 'anomaly'],
) as dag:

    def detect_peaks_fn():
        engine = get_engine()
        query = "SELECT * FROM weather_historical"
        df = pd.read_sql(query, engine)

        if df.empty:
            logger.info("No data in weather_historical to process.")
            return {'total': 0, 'moderate': 0, 'high': 0, 'extreme': 0}

        df['date'] = pd.to_datetime(df['date'])

        # METHOD A: Global
        global_mean = df['temperature'].mean()
        global_std = df['temperature'].std()

        df_global = df.copy()
        df_global['z_score'] = (df_global['temperature'] - global_mean) / global_std
        df_global['threshold'] = global_mean + 1.5 * global_std
        df_global['mean_temp'] = global_mean
        df_global['std_temp'] = global_std
        df_global['detection_method'] = 'global'

        peaks_global = df_global[df_global['z_score'] > 1.5].copy()

        # METHOD B: Monthly adjusted
        df_monthly = df.copy()
        df_monthly['month'] = df_monthly['date'].dt.month
        monthly_stats = df_monthly.groupby('month')['temperature'].agg(['mean', 'std']).reset_index()
        monthly_stats = monthly_stats.rename(columns={'mean': 'mean_temp', 'std': 'std_temp'})

        df_monthly = df_monthly.merge(monthly_stats, on='month', how='left')
        df_monthly['z_score'] = (df_monthly['temperature'] - df_monthly['mean_temp']) / df_monthly['std_temp']
        df_monthly['threshold'] = df_monthly['mean_temp'] + 1.5 * df_monthly['std_temp']
        df_monthly['detection_method'] = 'monthly_adjusted'

        peaks_monthly = df_monthly[df_monthly['z_score'] > 1.5].copy()
        peaks_monthly = peaks_monthly.drop(columns=['month'])

        # Combine
        combined_peaks = pd.concat([peaks_global, peaks_monthly], ignore_index=True)

        if combined_peaks.empty:
            logger.info("No peaks detected.")
            return {'total': 0, 'moderate': 0, 'high': 0, 'extreme': 0}

        # Determine severity
        def get_severity(z):
            if z > 2.5:
                return 'extreme'
            elif z > 2.0:
                return 'high'
            else:
                return 'moderate'

        combined_peaks['severity'] = combined_peaks['z_score'].apply(get_severity)

        # Sort and deduplicate by date+city, keeping highest z_score
        combined_peaks = combined_peaks.sort_values('z_score', ascending=False)
        deduplicated_peaks = combined_peaks.drop_duplicates(subset=['date', 'city'], keep='first')

        # Formatting values correctly
        # Extract date as string for psycopg2 or let psycopg2 handle datetime
        deduplicated_peaks['date'] = deduplicated_peaks['date'].dt.date

        # Convert to records
        cols = [
            'date', 'city', 'governorate', 'region', 'temperature',
            'mean_temp', 'std_temp', 'threshold', 'z_score',
            'severity', 'detection_method'
        ]

        records_to_insert = deduplicated_peaks[cols].values.tolist()

        upsert_query = """
            INSERT INTO temperature_peaks
                (date, city, governorate, region, temperature,
                 mean_temp, std_temp, threshold, z_score,
                 severity, detection_method, detected_at)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW())
            ON CONFLICT (date, city) DO UPDATE SET
                temperature      = EXCLUDED.temperature,
                mean_temp        = EXCLUDED.mean_temp,
                std_temp         = EXCLUDED.std_temp,
                threshold        = EXCLUDED.threshold,
                z_score          = EXCLUDED.z_score,
                severity         = EXCLUDED.severity,
                detection_method = EXCLUDED.detection_method,
                detected_at      = NOW()
        """

        conn = get_connection()
        try:
            with conn.cursor() as cursor:
                execute_batch(cursor, upsert_query, records_to_insert)
            conn.commit()
        finally:
            conn.close()

        counts = deduplicated_peaks['severity'].value_counts().to_dict()

        return {
            'total': len(deduplicated_peaks),
            'moderate': counts.get('moderate', 0),
            'high': counts.get('high', 0),
            'extreme': counts.get('extreme', 0)
        }

    def log_run_fn(**kwargs):
        started_at = kwargs['data_interval_start']
        result = kwargs['ti'].xcom_pull(task_ids='detect_peaks')
        rows_processed = result.get('total', 0) if result else 0

        log_pipeline_run(
            dag_name='peak_detection',
            task_name='full_run',
            started_at=started_at,
            rows_processed=rows_processed,
            status='success'
        )

    t1 = PythonOperator(
        task_id='detect_peaks',
        python_callable=detect_peaks_fn,
    )

    t2 = PythonOperator(
        task_id='log_run',
        python_callable=log_run_fn,
    )

    t1 >> t2
