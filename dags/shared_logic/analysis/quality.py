"""
Shared data quality check logic — used by both data_quality_dag and master_pipeline_dag.
"""

import json
import logging
from datetime import datetime, timedelta, timezone

from airflow.exceptions import AirflowException

from shared_logic.utils.db import get_connection

logger = logging.getLogger(__name__)


def run_quality_checks_fn():
    layers = {
        'historical': 'weather_historical',
        'current': 'weather_current',
        'forecast': 'weather_forecast'
    }

    conn = get_connection()
    today = datetime.now(timezone.utc).date()
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
