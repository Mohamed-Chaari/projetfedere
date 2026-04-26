import sys
import os
import logging
from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from meteo.src.utils.db import get_connection, log_pipeline_run

# Import callables
from dags.monthly_analysis_dag import (
    compute_monthly_averages_fn as compute_monthly_averages,
    check_data_freshness_fn as check_data_freshness
)
from dags.peak_detection_dag import (
    detect_peaks_fn as detect_peaks
)
from dags.correlation_dag import (
    compute_correlations_fn as compute_correlations,
    check_min_data_fn as check_min_data
)
from dags.annual_stats_dag import (
    compute_annual_stats_fn as compute_annual_stats
)
from dags.data_quality_dag import (
    run_quality_checks_fn as run_quality_checks
)

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
    dag_id='master_weather_pipeline',
    default_args=default_args,
    schedule='0 6 * * *',
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['master', 'orchestration'],
) as dag:

    t1 = PythonOperator(
        task_id='check_freshness',
        python_callable=check_data_freshness,
    )

    t2 = PythonOperator(
        task_id='quick_quality_check',
        python_callable=run_quality_checks,
    )

    t3a = PythonOperator(
        task_id='monthly_averages',
        python_callable=compute_monthly_averages,
    )

    t3b = PythonOperator(
        task_id='peak_detection',
        python_callable=detect_peaks,
    )

    t4 = PythonOperator(
        task_id='correlation_matrix',
        python_callable=compute_correlations,
    )

    t5 = PythonOperator(
        task_id='annual_stats',
        python_callable=compute_annual_stats,
    )

    def pipeline_complete_fn(**kwargs):
        conn = get_connection()
        try:
            with conn.cursor() as cursor:
                cursor.execute("SELECT COUNT(*) FROM monthly_averages")
                monthly_count = cursor.fetchone()[0]

                cursor.execute("SELECT COUNT(*) FROM temperature_peaks")
                peaks_count = cursor.fetchone()[0]

                cursor.execute("SELECT COUNT(*) FROM correlations WHERE season='annual'")
                correlations_count = cursor.fetchone()[0]

                cursor.execute("SELECT COUNT(*) FROM annual_stats")
                annual_count = cursor.fetchone()[0]

            total = monthly_count + peaks_count + correlations_count + annual_count

            logger.info(
                f"Pipeline complete: monthly={monthly_count}, peaks={peaks_count}, "
                f"correlations={correlations_count}, annual={annual_count}"
            )

            started_at = kwargs['data_interval_start']

            log_pipeline_run(
                dag_name='master_weather_pipeline',
                task_name='full_run',
                started_at=started_at,
                rows_processed=total,
                status='success'
            )

        finally:
            conn.close()

    t6 = PythonOperator(
        task_id='pipeline_complete',
        python_callable=pipeline_complete_fn,
    )

    t1 >> t2 >> [t3a, t3b] >> t4 >> t5 >> t6
