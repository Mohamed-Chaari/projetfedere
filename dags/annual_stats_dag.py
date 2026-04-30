import logging
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

from shared_logic.analysis.annual import compute_annual_stats_fn
from shared_logic.utils.db import log_pipeline_run

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
