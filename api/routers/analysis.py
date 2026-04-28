from fastapi import APIRouter, Depends, HTTPException, Query
from typing import List, Optional
from api.database import execute_query
from api.dependencies import verify_token
from api.models.responses import MonthlyAverage, TemperaturePeak, CorrelationResult, AnnualStat

router = APIRouter(prefix="/api/analysis", tags=["analysis"])

@router.get("/monthly", response_model=List[MonthlyAverage])
def get_monthly(
    year: Optional[int] = Query(None),
    governorate: Optional[str] = Query(None),
    region: Optional[str] = Query(None),
    username: str = Depends(verify_token)
):
    base = "SELECT * FROM v_monthly_trend WHERE 1=1"
    params = {}
    if year:
        base += " AND year = :year"
        params['year'] = year
    if governorate:
        base += " AND LOWER(governorate) = LOWER(:gov)"
        params['gov'] = governorate
    if region:
        base += " AND LOWER(region) = LOWER(:reg)"
        params['reg'] = region
    base += " ORDER BY year, month, governorate"

    results = execute_query(base, params)
    if not results and (year or governorate or region):
        raise HTTPException(status_code=404, detail="No data found for the specified filters")
    return results

@router.get("/peaks", response_model=List[TemperaturePeak])
def get_peaks(
    severity: Optional[str] = Query(None),
    city: Optional[str] = Query(None),
    limit: int = Query(50, ge=1),
    username: str = Depends(verify_token)
):
    base = """
        SELECT date, city, governorate, region, temperature,
               z_score, severity, threshold, detection_method
        FROM temperature_peaks WHERE 1=1
    """
    params = {}
    if severity:
        base += " AND severity = :severity"
        params['severity'] = severity
    if city:
        base += " AND LOWER(city) = LOWER(:city)"
        params['city'] = city
    base += " ORDER BY z_score DESC LIMIT :limit"
    params['limit'] = limit

    results = execute_query(base, params)
    if not results and (severity or city):
        raise HTTPException(status_code=404, detail="No peaks found for the specified filters")
    return results

@router.get("/correlation", response_model=List[CorrelationResult])
def get_correlation(
    season: str = Query(..., description="Season to filter correlations by"),
    username: str = Depends(verify_token)
):
    query = """
        SELECT variable_a, variable_b, pearson_r, p_value,
               interpretation, is_significant, season
        FROM correlations
        WHERE season = :season
        ORDER BY ABS(pearson_r) DESC
    """
    params = {'season': season}
    results = execute_query(query, params)
    if not results:
        raise HTTPException(status_code=404, detail=f"No correlations found for season '{season}'")
    return results

@router.get("/annual", response_model=List[AnnualStat])
def get_annual(
    city: Optional[str] = Query(None),
    year: Optional[int] = Query(None),
    username: str = Depends(verify_token)
):
    base = """
        SELECT year, city, governorate, region,
               avg_temp, max_temp, min_temp, total_precip,
               avg_wind, avg_humidity, hot_days_count,
               cold_days_count, data_completeness, temp_trend_per_year
        FROM annual_stats WHERE 1=1
    """
    params = {}
    if city:
        base += " AND LOWER(city) = LOWER(:city)"
        params['city'] = city
    if year:
        base += " AND year = :year"
        params['year'] = year
    base += " ORDER BY year, city"

    results = execute_query(base, params)
    if not results and (city or year):
        raise HTTPException(status_code=404, detail="No annual stats found for the specified filters")
    return results
