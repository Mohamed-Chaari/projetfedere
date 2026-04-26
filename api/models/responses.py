from pydantic import BaseModel
from typing import Optional, List
from datetime import date, datetime

class HealthResponse(BaseModel):
    status: str
    service: str

class CurrentWeather(BaseModel):
    city: str
    governorate: str
    region: str
    temperature: Optional[float]
    feels_like: Optional[float]
    humidity: Optional[float]
    precipitation: Optional[float]
    wind_speed: Optional[float]
    wind_gusts: Optional[float]
    pressure: Optional[float]
    weather_code: Optional[int]
    weather_desc: Optional[str]
    observed_at: Optional[datetime]

class ForecastDay(BaseModel):
    city: str
    forecast_for: date
    forecast_day: int
    temperature: Optional[float]
    temp_max: Optional[float]
    temp_min: Optional[float]
    precipitation: Optional[float]
    precipitation_probability: Optional[int]
    wind_speed: Optional[float]
    weather_code: Optional[int]
    weather_desc: Optional[str]

class MonthlyAverage(BaseModel):
    year: int
    month: int
    city: str
    governorate: str
    avg_temp: Optional[float]
    max_temp: Optional[float]
    min_temp: Optional[float]
    avg_humidity: Optional[float]
    avg_precip: Optional[float]
    avg_wind: Optional[float]

class TemperaturePeak(BaseModel):
    date: date
    city: str
    governorate: str
    temperature: float
    z_score: float
    severity: str
    threshold: float

class CorrelationResult(BaseModel):
    variable_a: str
    variable_b: str
    pearson_r: float
    interpretation: str
    is_significant: bool
    season: str

class AnnualStat(BaseModel):
    year: int
    city: str
    avg_temp: Optional[float]
    max_temp: Optional[float]
    min_temp: Optional[float]
    total_precip: Optional[float]
    hot_days_count: Optional[int]
    temp_trend_per_year: Optional[float]

class AlertEvent(BaseModel):
    alert_type: str
    severity: str
    city: str
    governorate: str
    region: str
    value: float
    threshold: float
    unit: str
    triggered_at: datetime

class NationalSummary(BaseModel):
    cities_reporting: int
    avg_temp_national: Optional[float]
    max_temp: Optional[float]
    min_temp: Optional[float]
    hottest_city: Optional[str]
    coldest_city: Optional[str]
    avg_humidity: Optional[float]
    last_updated: Optional[datetime]

class TokenResponse(BaseModel):
    access_token: str
    token_type: str = "bearer"
