import os, sys
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import logging
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

from shared_logic.analysis.quality import run_quality_checks_fn
from shared_logic.utils.db import log_pipeline_run

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

    def log_run_fn(**kwargs):
        started_at = kwargs['data_interval_start']
        log_pipeline_run(
            dag_name='data_quality_monitor',
            task_name='full_run',
            started_at=started_at,
            rows_processed=0,
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
