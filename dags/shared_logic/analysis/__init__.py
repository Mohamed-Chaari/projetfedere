"""
Analysis modules — shared business logic for Météo Tunisie DAGs.
"""
from .quality import run_quality_checks_fn
from .monthly import check_data_freshness_fn, compute_monthly_averages_fn
from .peaks import detect_peaks_fn
from .correlations import check_min_data_fn, compute_correlations_fn
from .annual import compute_annual_stats_fn

__all__ = [
    'run_quality_checks_fn',
    'check_data_freshness_fn',
    'compute_monthly_averages_fn',
    'detect_peaks_fn',
    'check_min_data_fn',
    'compute_correlations_fn',
    'compute_annual_stats_fn',
]
