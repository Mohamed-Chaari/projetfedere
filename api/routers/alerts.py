from fastapi import APIRouter, Depends, HTTPException, Query, Response
from typing import List, Optional
from api.database import execute_query
from api.models.responses import AlertEvent
from api.dependencies import verify_token

router = APIRouter(prefix="/api/alerts", tags=["alerts"])

@router.get("/active", response_model=List[AlertEvent])
def get_active_alerts(response: Response):
    response.headers["Cache-Control"] = "no-cache, no-store, must-revalidate"
    response.headers["Pragma"] = "no-cache"

    query = "SELECT * FROM v_active_alerts"
    return execute_query(query)

@router.get("/history", response_model=List[AlertEvent])
def get_alerts_history(
    days: int = Query(7, ge=1, le=30),
    alert_type: Optional[str] = Query(None, alias="type", description="Filter by type: HEATWAVE, COLD_SNAP, STRONG_WIND, HEAVY_RAIN"),
    region: Optional[str] = Query(None),
    username: str = Depends(verify_token)
):
    query = "SELECT * FROM weather_alerts WHERE triggered_at >= NOW() - (:days * INTERVAL '1 day')"
    conditions = []
    params = {"days": days}

    if alert_type:
        conditions.append("UPPER(alert_type) = UPPER(:type)")
        params["type"] = alert_type
    if region:
        conditions.append("LOWER(region) = LOWER(:region)")
        params["region"] = region

    if conditions:
        query += " AND " + " AND ".join(conditions)

    query += " ORDER BY triggered_at DESC"

    result = execute_query(query, params)

    if not result and (alert_type or region):
        raise HTTPException(
            status_code=404,
            detail="No alerts found for the specified filters"
        )

    return result

@router.get("/stats", response_model=List[dict])
def get_alerts_stats(username: str = Depends(verify_token)):
    query = """
        SELECT alert_type, severity, COUNT(*) as count
        FROM weather_alerts
        WHERE triggered_at >= NOW() - INTERVAL '30 days'
        GROUP BY alert_type, severity
        ORDER BY count DESC
    """
    return execute_query(query)
