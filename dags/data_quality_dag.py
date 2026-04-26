import sys
import os
import logging
import json
from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.exceptions import AirflowException

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from src.utils.db import get_connection, log_pipeline_run

logger = logging.getLogger(__name__)

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='data_quality_monitor',
    default_args=default_args,
    schedule='0 10 * * *',
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['quality', 'monitoring'],
) as dag:

    def run_quality_checks_fn():
        layers = {
            'historical': 'weather_historical',
            'current': 'weather_current',
            'forecast': 'weather_forecast'
        }

        conn = get_connection()
        today = datetime.utcnow().date()
        total_issues = 0

        try:
            for layer, table in layers.items():
                with conn.cursor() as cursor:
                    # 1. total_rows
                    cursor.execute(f"SELECT COUNT(*) FROM {table}")
                    total_rows = cursor.fetchone()[0]

                    # 2. missing_values_count
                    cursor.execute(f"SELECT COUNT(*) FROM {table} WHERE temperature IS NULL")
                    missing_values_count = cursor.fetchone()[0]

                    # 3. out_of_range_count
                    cursor.execute(f"SELECT COUNT(*) FROM {table} WHERE temperature < -5 OR temperature > 55")
                    out_of_range_count = cursor.fetchone()[0]

                    # 4. duplicate_count
                    if layer == 'historical':
                        cursor.execute(f"SELECT COUNT(*) - COUNT(DISTINCT (date, city)) FROM {table}")
                        duplicate_count = cursor.fetchone()[0]
                    else:
                        duplicate_count = 0

                    # 5. freshness_issue
                    cursor.execute(f"SELECT MAX(ingested_at) FROM {table}")
                    max_ingested = cursor.fetchone()[0]

                    freshness_issue = 0
                    if max_ingested:
                        # Assuming max_ingested is datetime with tzinfo
                        two_days_ago = datetime.now(max_ingested.tzinfo) - timedelta(days=2)
                        if max_ingested < two_days_ago:
                            freshness_issue = 1
                    else:
                        # If table is empty, we might flag it as a freshness issue depending on expectations, but 0 is safer for empty tables
                        pass

                    issues_found = missing_values_count + out_of_range_count + duplicate_count + freshness_issue
                    quality_score = max(0, 100 - issues_found * 10)

                    issues_detail_dict = {
                        "missing_values": missing_values_count,
                        "out_of_range": out_of_range_count,
                        "duplicates": duplicate_count,
                        "freshness_issue": freshness_issue
                    }
                    issues_detail = json.dumps(issues_detail_dict)

                    insert_query = """
                        INSERT INTO data_quality_log
                          (check_date, layer, total_rows, missing_values_count,
                           out_of_range_count, duplicate_count, quality_score,
                           issues_detail, checked_at)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, NOW())
                    """
                    cursor.execute(insert_query, (
                        today, layer, total_rows, missing_values_count,
                        out_of_range_count, duplicate_count, quality_score,
                        issues_detail
                    ))

                    if quality_score < 50:
                        raise AirflowException(f"CRITICAL quality failure in layer {layer}")
                    if quality_score < 80:
                        logger.warning(f"Quality warning: {layer} score={quality_score}")

            conn.commit()
            return total_issues

        finally:
            conn.close()


    def log_run_fn(**kwargs):
        started_at = kwargs['data_interval_start']
        log_pipeline_run(
            dag_name='data_quality_monitor',
            task_name='full_run',
            started_at=started_at,
            rows_processed=0, # Log doesn't expect rows processed here typically
            status='success'
        )

    t1 = PythonOperator(
        task_id='run_quality_checks',
        python_callable=run_quality_checks_fn,
    )

    t2 = PythonOperator(
        task_id='log_run',
        python_callable=log_run_fn,
    )

    t1 >> t2
