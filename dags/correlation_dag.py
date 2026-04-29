import logging
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

from src.analysis.correlations import check_min_data_fn, compute_correlations_fn
from src.utils.db import get_connection, log_pipeline_run

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
    dag_id='correlation_analysis',
    default_args=default_args,
    schedule='0 8 * * 1',
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['analysis', 'statistics', 'correlation'],
) as dag:

    def verify_results_fn():
        conn = get_connection()
        try:
            with conn.cursor() as cursor:
                cursor.execute("SELECT COUNT(*) FROM correlations WHERE season='annual'")
                count = cursor.fetchone()[0]
                if count != 6:
                    logger.warning(f"Expected 6 annual correlation records, found {count}")
                else:
                    logger.info(f"Verified exactly 6 annual records.")
        finally:
            conn.close()

    def log_run_fn(**kwargs):
        started_at = kwargs['data_interval_start']
        result = kwargs['ti'].xcom_pull(task_ids='compute_correlations')
        rows_processed = result.get('pairs_computed', 0) if result else 0

        log_pipeline_run(
            dag_name='correlation_analysis',
            task_name='full_run',
            started_at=started_at,
            rows_processed=rows_processed,
            status='success'
        )

    t1 = PythonOperator(
        task_id='check_min_data',
        python_callable=check_min_data_fn,
    )

    t2 = PythonOperator(
        task_id='compute_correlations',
        python_callable=compute_correlations_fn,
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
