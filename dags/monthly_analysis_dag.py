import os, sys
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import logging
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

from shared_logic.analysis.monthly import check_data_freshness_fn, compute_monthly_averages_fn
from shared_logic.utils.db import get_connection, log_pipeline_run

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
