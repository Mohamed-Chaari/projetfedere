"""
Analysis modules — shared business logic for Météo Tunisie DAGs.
"""
from src.analysis.quality import run_quality_checks_fn
from src.analysis.monthly import check_data_freshness_fn, compute_monthly_averages_fn
from src.analysis.peaks import detect_peaks_fn
from src.analysis.correlations import check_min_data_fn, compute_correlations_fn
from src.analysis.annual import compute_annual_stats_fn

__all__ = [
    'run_quality_checks_fn',
    'check_data_freshness_fn',
    'compute_monthly_averages_fn',
    'detect_peaks_fn',
    'check_min_data_fn',
    'compute_correlations_fn',
    'compute_annual_stats_fn',
]
