import sys
import os
import logging
import calendar
from datetime import datetime, timedelta, timezone

from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
import pandas as pd
import numpy as np
import scipy.stats
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
    dag_id='annual_stats',
    default_args=default_args,
    schedule='0 9 1 1 *',
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['analysis', 'annual', 'trend'],
) as dag:

    def compute_annual_stats_fn():
        engine = get_engine()
        query = "SELECT * FROM weather_historical"
        df = pd.read_sql(query, engine)

        if df.empty:
            logger.info("No data in weather_historical to process.")
            return {'rows_upserted': 0, 'years_computed': [], 'cities': 0, 'trend': None}

        df['date'] = pd.to_datetime(df['date'])
        df['year'] = df['date'].dt.year

        # Group data
        def compute_group_stats(group):
            year = group['year'].iloc[0]
            expected_days = 366 if calendar.isleap(year) else 365

            return pd.Series({
                'avg_temp': group['temperature'].mean(),
                'max_temp': group['temperature'].max(),
                'min_temp': group['temperature'].min(),
                'avg_humidity': group['humidity'].mean(),
                'total_precip': group['precipitation'].sum(),
                'avg_wind': group['wind_speed'].mean(),
                'hot_days_count': (group['temperature'] > 35).sum(),
                'cold_days_count': (group['temperature'] < 10).sum(),
                'rainy_days_count': (group['precipitation'] > 1).sum(),
                'dry_days_count': (group['precipitation'] == 0).sum(),
                'data_completeness': len(group) / expected_days * 100
            })

        grouped = df.groupby(['city', 'governorate', 'region', 'year']).apply(compute_group_stats).reset_index()

        # Round floats to 2 decimals
        cols_to_round = ['avg_temp', 'max_temp', 'min_temp', 'avg_humidity', 'total_precip', 'avg_wind', 'data_completeness']
        grouped[cols_to_round] = grouped[cols_to_round].round(2)

        # Warming trend per city
        city_trends = []
        for city, city_df in df.groupby('city'):
            yearly_means = city_df.groupby('year')['temperature'].mean().reset_index()
            years_list = yearly_means['year'].sort_values().tolist()
            avg_per_year = yearly_means['temperature'].tolist()

            if len(years_list) < 2:
                trend = None
            else:
                slope, _, _, _, _ = scipy.stats.linregress(years_list, avg_per_year)
                trend = round(float(slope), 4)

            city_trends.append({'city': city, 'temp_trend_per_year': trend})

        trends_df = pd.DataFrame(city_trends)
        grouped = grouped.merge(trends_df, on='city', how='left')

        # Replace NaN in trend with None for DB NULL
        grouped['temp_trend_per_year'] = grouped['temp_trend_per_year'].replace({np.nan: None})

        # Upsert
        cols = [
            'year', 'city', 'governorate', 'region',
            'avg_temp', 'max_temp', 'min_temp',
            'total_precip', 'avg_wind', 'avg_humidity',
            'hot_days_count', 'cold_days_count',
            'rainy_days_count', 'dry_days_count',
            'data_completeness', 'temp_trend_per_year'
        ]

        records_to_insert = grouped[cols].values.tolist()

        upsert_query = """
            INSERT INTO annual_stats
                (year, city, governorate, region,
                 avg_temp, max_temp, min_temp,
                 total_precip, avg_wind, avg_humidity,
                 hot_days_count, cold_days_count,
                 rainy_days_count, dry_days_count,
                 data_completeness, temp_trend_per_year, computed_at)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW())
            ON CONFLICT (year, city) DO UPDATE SET
                governorate=EXCLUDED.governorate,
                region=EXCLUDED.region,
                avg_temp=EXCLUDED.avg_temp,
                max_temp=EXCLUDED.max_temp,
                min_temp=EXCLUDED.min_temp,
                total_precip=EXCLUDED.total_precip,
                avg_wind=EXCLUDED.avg_wind,
                avg_humidity=EXCLUDED.avg_humidity,
                hot_days_count=EXCLUDED.hot_days_count,
                cold_days_count=EXCLUDED.cold_days_count,
                rainy_days_count=EXCLUDED.rainy_days_count,
                dry_days_count=EXCLUDED.dry_days_count,
                data_completeness=EXCLUDED.data_completeness,
                temp_trend_per_year=EXCLUDED.temp_trend_per_year,
                computed_at=NOW()
        """

        conn = get_connection()
        try:
            with conn.cursor() as cursor:
                execute_batch(cursor, upsert_query, records_to_insert)
            conn.commit()
        finally:
            conn.close()

        # Log national warmest year and overall trend direction
        national_yearly_mean = grouped.groupby('year')['avg_temp'].mean()
        warmest_year = national_yearly_mean.idxmax()

        valid_trends = [t for t in grouped['temp_trend_per_year'].dropna().unique() if t is not None]
        overall_trend = None
        if valid_trends:
            overall_avg_trend = sum(valid_trends) / len(valid_trends)
            sign = "+" if overall_avg_trend > 0 else ""
            overall_trend = f"{sign}{overall_avg_trend:.4f}°C/year"
        else:
            overall_trend = "Insufficient data"

        logger.info(f"Warmest year nationally: {warmest_year}, Overall trend direction: {overall_trend}")

        return {
            'rows_upserted': len(grouped),
            'years_computed': grouped['year'].unique().tolist(),
            'cities': grouped['city'].nunique(),
            'trend': overall_trend
        }

    def log_run_fn(**kwargs):
        started_at = kwargs['data_interval_start']
        result = kwargs['ti'].xcom_pull(task_ids='compute_annual_stats')
        rows_processed = result.get('rows_upserted', 0) if result else 0

        log_pipeline_run(
            dag_name='annual_stats',
            task_name='full_run',
            started_at=started_at,
            rows_processed=rows_processed,
            status='success'
        )

    t1 = PythonOperator(
        task_id='compute_annual_stats',
        python_callable=compute_annual_stats_fn,
    )

    t2 = PythonOperator(
        task_id='log_run',
        python_callable=log_run_fn,
    )

    t1 >> t2
