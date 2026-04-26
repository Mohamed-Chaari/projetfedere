from fastapi import APIRouter, Depends, HTTPException, Query
from typing import List, Optional
from datetime import datetime, timezone
from api.database import execute_query
from api.models.responses import CurrentWeather, NationalSummary
from api.dependencies import verify_token

router = APIRouter(prefix="/api/dashboard", tags=["dashboard"])

_cache = {}

def get_cached(key: str, ttl_seconds: int = 300):
    if key in _cache:
        data, ts = _cache[key]
        if (datetime.now(timezone.utc) - ts).total_seconds() < ttl_seconds:
            return data
    return None

def set_cached(key: str, data: dict):
    _cache[key] = (data, datetime.now(timezone.utc))

@router.get("/summary", response_model=NationalSummary)
def get_summary(username: str = Depends(verify_token)):
    cache_key = "dashboard_summary"
    cached_data = get_cached(cache_key)
    if cached_data:
        return cached_data

    query = "SELECT * FROM v_national_summary"
    result = execute_query(query)

    if not result:
        raise HTTPException(status_code=404, detail="National summary data not found")

    data = result[0]
    set_cached(cache_key, data)
    return data

@router.get("/current", response_model=List[CurrentWeather])
def get_current_weather(
    governorate: Optional[str] = Query(None),
    region: Optional[str] = Query(None),
    username: str = Depends(verify_token)
):
    query = "SELECT * FROM v_national_current"
    params = {}
    conditions = []

    if governorate:
        conditions.append("LOWER(governorate) = LOWER(:governorate)")
        params["governorate"] = governorate
    if region:
        conditions.append("LOWER(region) = LOWER(:region)")
        params["region"] = region

    if conditions:
        query += " WHERE " + " AND ".join(conditions)

    query += " ORDER BY governorate, city"

    result = execute_query(query, params)

    if not result and (governorate or region):
        raise HTTPException(
            status_code=404,
            detail="No data found for the specified filters"
        )

    return result

@router.get("/current/{city}", response_model=CurrentWeather)
def get_current_weather_by_city(city: str, username: str = Depends(verify_token)):
    query = "SELECT * FROM weather_current WHERE LOWER(city) = LOWER(:city)"
    result = execute_query(query, {"city": city})

    if not result:
        raise HTTPException(
            status_code=404,
            detail=f"City '{city}' not found in weather_current"
        )

    return result[0]
