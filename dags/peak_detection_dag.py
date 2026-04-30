import os, sys
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import logging
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

from shared_logic.analysis.peaks import detect_peaks_fn
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
    dag_id='peak_detection',
    default_args=default_args,
    schedule='0 7 * * *',
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['analysis', 'peaks', 'anomaly'],
) as dag:

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
