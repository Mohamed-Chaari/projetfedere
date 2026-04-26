from fastapi import APIRouter, Depends, HTTPException
from typing import List
from api.database import execute_query
from api.models.responses import ForecastDay
from api.dependencies import verify_token

router = APIRouter(prefix="/api/forecast", tags=["forecast"])

@router.get("/today/national", response_model=List[ForecastDay])
def get_today_national_forecast(username: str = Depends(verify_token)):
    query = """
        SELECT * FROM v_forecast_7days
        WHERE forecast_day = 0
        ORDER BY governorate, city
    """
    result = execute_query(query)
    return result

@router.get("/governorate/{governorate}", response_model=List[ForecastDay])
def get_forecast_by_governorate(governorate: str, username: str = Depends(verify_token)):
    query = """
        SELECT * FROM v_forecast_7days
        WHERE LOWER(governorate) = LOWER(:governorate)
        ORDER BY city, forecast_day
    """
    result = execute_query(query, {"governorate": governorate})

    if not result:
        raise HTTPException(
            status_code=404,
            detail=f"Governorate '{governorate}' not found or has no forecast data"
        )

    return result

@router.get("/{city}", response_model=List[ForecastDay])
def get_forecast_by_city(city: str, username: str = Depends(verify_token)):
    query = """
        SELECT * FROM v_forecast_7days
        WHERE LOWER(city) = LOWER(:city)
        ORDER BY forecast_day
    """
    result = execute_query(query, {"city": city})

    if not result:
        raise HTTPException(
            status_code=404,
            detail=f"City '{city}' not found in forecast data"
        )

    return result
