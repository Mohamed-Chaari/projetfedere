"""
Shared correlation analysis logic — used by both correlation_dag and master_pipeline_dag.
"""

import itertools
import logging

from airflow.exceptions import AirflowSkipException
from psycopg2.extras import execute_batch

from ..utils.db import get_connection, get_engine

logger = logging.getLogger(__name__)


def check_min_data_fn():
    conn = get_connection()
    try:
        with conn.cursor() as cursor:
            cursor.execute("SELECT COUNT(*) FROM weather_historical")
            count = cursor.fetchone()[0]
            if count < 365:
                raise AirflowSkipException(f"Not enough data. Count: {count}")
            logger.info(f"Data count: {count}")
    finally:
        conn.close()


def compute_correlations_fn():
    import pandas as pd
    import scipy.stats

    engine = get_engine()
    query = "SELECT date, temperature, humidity, precipitation, wind_speed FROM weather_historical"
    df = pd.read_sql(query, engine)

    if df.empty:
        logger.info("No data found")
        return {'pairs_computed': 0, 'strongest': None}

    df['month'] = pd.to_datetime(df['date']).dt.month
    variables = ['temperature', 'humidity', 'precipitation', 'wind_speed']

    results = []

    def get_interpretation(r):
        abs_r = abs(r)
        if abs_r >= 0.7:
            return 'strong positive' if r > 0 else 'strong negative'
        elif abs_r >= 0.4:
            return 'moderate positive' if r > 0 else 'moderate negative'
        elif abs_r >= 0.2:
            return 'weak positive' if r > 0 else 'weak negative'
        else:
            return 'negligible'

    def compute_pairs(data, season_name):
        if data.empty or len(data) < 2:
            return

        corr_matrix = data[variables].corr(method='pearson')

        for v1, v2 in itertools.combinations(variables, 2):
            a, b = sorted([v1, v2])
            r = round(corr_matrix.loc[a, b], 4)

            # dropna for pairwise correlation p-value
            valid_data = data[[a, b]].dropna()
            if len(valid_data) < 2:
                continue

            _, p_value = scipy.stats.pearsonr(valid_data[a], valid_data[b])

            if pd.isna(r) or pd.isna(p_value):
                continue

            is_significant = bool(p_value < 0.05)
            interpretation = get_interpretation(r)

            results.append({
                'variable_a': a,
                'variable_b': b,
                'season': season_name,
                'pearson_r': r,
                'p_value': p_value,
                'interpretation': interpretation,
                'is_significant': is_significant
            })

    # Annual
    compute_pairs(df, 'annual')

    # Seasonal
    season_map = {
        'DJF': [12, 1, 2],
        'MAM': [3, 4, 5],
        'JJA': [6, 7, 8],
        'SON': [9, 10, 11]
    }

    for season_name, months in season_map.items():
        season_df = df[df['month'].isin(months)].copy()
        compute_pairs(season_df, season_name)

    if not results:
        return {'pairs_computed': 0, 'strongest': None}

    # Find strongest
    strongest_record = max(results, key=lambda x: abs(x['pearson_r']))
    strongest_info = {
        'pair': f"{strongest_record['variable_a']}-{strongest_record['variable_b']} ({strongest_record['season']})",
        'r': strongest_record['pearson_r']
    }
    logger.info(f"Strongest pair: {strongest_info['pair']} with r={strongest_info['r']}")

    # Upsert
    records_to_insert = [
        (
            r['variable_a'], r['variable_b'], r['season'],
            r['pearson_r'], r['p_value'], r['interpretation'],
            r['is_significant']
        )
        for r in results
    ]

    upsert_query = """
        INSERT INTO correlations
            (variable_a, variable_b, season, pearson_r, p_value, interpretation, is_significant, computed_at)
        VALUES (%s, %s, %s, %s, %s, %s, %s, NOW())
        ON CONFLICT (variable_a, variable_b, season) DO UPDATE SET
            pearson_r=EXCLUDED.pearson_r,
            p_value=EXCLUDED.p_value,
            interpretation=EXCLUDED.interpretation,
            is_significant=EXCLUDED.is_significant,
            computed_at=NOW()
    """

    conn = get_connection()
    try:
        with conn.cursor() as cursor:
            execute_batch(cursor, upsert_query, records_to_insert)
        conn.commit()
    finally:
        conn.close()

    return {'pairs_computed': len(results), 'strongest': strongest_info}
