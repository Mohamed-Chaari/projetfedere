"""
Shared peak detection logic — used by both peak_detection_dag and master_pipeline_dag.
"""

import logging

import pandas as pd
import numpy as np
from psycopg2.extras import execute_batch

from src.utils.db import get_connection, get_engine

logger = logging.getLogger(__name__)


def detect_peaks_fn():
    engine = get_engine()
    query = "SELECT * FROM weather_historical"
    df = pd.read_sql(query, engine)

    if df.empty:
        logger.info("No data in weather_historical to process.")
        return {'total': 0, 'moderate': 0, 'high': 0, 'extreme': 0}

    df['date'] = pd.to_datetime(df['date'])

    # METHOD A: Global
    global_mean = df['temperature'].mean()
    global_std = df['temperature'].std()

    df_global = df.copy()
    df_global['z_score'] = (df_global['temperature'] - global_mean) / global_std
    df_global['threshold'] = global_mean + 1.5 * global_std
    df_global['mean_temp'] = global_mean
    df_global['std_temp'] = global_std
    df_global['detection_method'] = 'global'

    peaks_global = df_global[df_global['z_score'] > 1.5].copy()

    # METHOD B: Monthly adjusted
    df_monthly = df.copy()
    df_monthly['month'] = df_monthly['date'].dt.month
    monthly_stats = df_monthly.groupby('month')['temperature'].agg(['mean', 'std']).reset_index()
    monthly_stats = monthly_stats.rename(columns={'mean': 'mean_temp', 'std': 'std_temp'})

    df_monthly = df_monthly.merge(monthly_stats, on='month', how='left')
    df_monthly['z_score'] = (df_monthly['temperature'] - df_monthly['mean_temp']) / df_monthly['std_temp']
    df_monthly['threshold'] = df_monthly['mean_temp'] + 1.5 * df_monthly['std_temp']
    df_monthly['detection_method'] = 'monthly_adjusted'

    peaks_monthly = df_monthly[df_monthly['z_score'] > 1.5].copy()
    peaks_monthly = peaks_monthly.drop(columns=['month'])

    # Combine
    combined_peaks = pd.concat([peaks_global, peaks_monthly], ignore_index=True)

    if combined_peaks.empty:
        logger.info("No peaks detected.")
        return {'total': 0, 'moderate': 0, 'high': 0, 'extreme': 0}

    # Determine severity
    def get_severity(z):
        if z > 2.5:
            return 'extreme'
        elif z > 2.0:
            return 'high'
        else:
            return 'moderate'

    combined_peaks['severity'] = combined_peaks['z_score'].apply(get_severity)

    # Sort and deduplicate by date+city, keeping highest z_score
    combined_peaks = combined_peaks.sort_values('z_score', ascending=False)
    deduplicated_peaks = combined_peaks.drop_duplicates(subset=['date', 'city'], keep='first')

    # Formatting values correctly
    deduplicated_peaks['date'] = deduplicated_peaks['date'].dt.date

    # Convert to records
    cols = [
        'date', 'city', 'governorate', 'region', 'temperature',
        'mean_temp', 'std_temp', 'threshold', 'z_score',
        'severity', 'detection_method'
    ]

    records_to_insert = deduplicated_peaks[cols].values.tolist()

    upsert_query = """
        INSERT INTO temperature_peaks
            (date, city, governorate, region, temperature,
             mean_temp, std_temp, threshold, z_score,
             severity, detection_method, detected_at)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW())
        ON CONFLICT (date, city) DO UPDATE SET
            temperature      = EXCLUDED.temperature,
            mean_temp        = EXCLUDED.mean_temp,
            std_temp         = EXCLUDED.std_temp,
            threshold        = EXCLUDED.threshold,
            z_score          = EXCLUDED.z_score,
            severity         = EXCLUDED.severity,
            detection_method = EXCLUDED.detection_method,
            detected_at      = NOW()
    """

    conn = get_connection()
    try:
        with conn.cursor() as cursor:
            execute_batch(cursor, upsert_query, records_to_insert)
        conn.commit()
    finally:
        conn.close()

    counts = deduplicated_peaks['severity'].value_counts().to_dict()

    return {
        'total': len(deduplicated_peaks),
        'moderate': counts.get('moderate', 0),
        'high': counts.get('high', 0),
        'extreme': counts.get('extreme', 0)
    }
