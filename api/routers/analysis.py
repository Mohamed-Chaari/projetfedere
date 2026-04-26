from fastapi import APIRouter, Depends, HTTPException, Query
from typing import List, Optional
from api.database import execute_query
from api.models.responses import MonthlyAverage, TemperaturePeak, CorrelationResult, AnnualStat
from api.dependencies import verify_token

router = APIRouter(prefix="/api/analysis", tags=["analysis"])

@router.get("/monthly", response_model=List[MonthlyAverage])
def get_monthly_averages(
    year: Optional[int] = Query(None),
    governorate: Optional[str] = Query(None),
    region: Optional[str] = Query(None),
    username: str = Depends(verify_token)
):
    query = "SELECT * FROM v_monthly_trend"
    conditions = []
    params = {}

    if year is not None:
        conditions.append("year = :year")
        params["year"] = year
    if governorate:
        conditions.append("LOWER(governorate) = LOWER(:governorate)")
        params["governorate"] = governorate
    if region:
        conditions.append("LOWER(region) = LOWER(:region)")
        params["region"] = region

    if conditions:
        query += " WHERE " + " AND ".join(conditions)

    result = execute_query(query, params)

    if not result and (governorate or region):
        raise HTTPException(
            status_code=404,
            detail="No data found for the specified location filter"
        )

    return result

@router.get("/peaks", response_model=List[TemperaturePeak])
def get_temperature_peaks(
    severity: Optional[str] = Query(None, pattern="^(moderate|high|extreme)$"),
    city: Optional[str] = Query(None),
    limit: int = Query(100, ge=1, le=500),
    username: str = Depends(verify_token)
):
    query = "SELECT * FROM temperature_peaks WHERE 1=1"
    conditions = []
    params = {}

    if severity:
        conditions.append("severity = :severity")
        params["severity"] = severity
    if city:
        conditions.append("LOWER(city) = LOWER(:city)")
        params["city"] = city

    if conditions:
        query += " AND " + " AND ".join(conditions)

    query += " ORDER BY z_score DESC LIMIT :limit"
    params["limit"] = limit

    result = execute_query(query, params)

    if not result and city:
        raise HTTPException(
            status_code=404,
            detail="No data found for the specified location filter"
        )

    return result

@router.get("/correlation", response_model=List[CorrelationResult])
def get_correlations(
    season: str = Query("annual", pattern="^(annual|DJF|MAM|JJA|SON)$", description="Season to analyze (annual, DJF, MAM, JJA, SON)"),
    username: str = Depends(verify_token)
):
    query = "SELECT * FROM correlations WHERE season = :season ORDER BY ABS(pearson_r) DESC"
    result = execute_query(query, {"season": season})
    return result

@router.get("/annual", response_model=List[AnnualStat])
def get_annual_stats(
    city: Optional[str] = Query(None),
    year: Optional[int] = Query(None),
    username: str = Depends(verify_token)
):
    query = "SELECT * FROM annual_stats WHERE 1=1"
    conditions = []
    params = {}

    if city:
        conditions.append("LOWER(city) = LOWER(:city)")
        params["city"] = city
    if year is not None:
        conditions.append("year = :year")
        params["year"] = year

    if conditions:
        query += " AND " + " AND ".join(conditions)

    query += " ORDER BY year, city"

    result = execute_query(query, params)

    if not result and city:
        raise HTTPException(
            status_code=404,
            detail="No data found for the specified location filter"
        )

    return result
