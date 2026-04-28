from fastapi import APIRouter, Depends, HTTPException, Query, Response
from typing import List, Optional
from api.database import execute_query
from api.dependencies import verify_token
from api.models.responses import AlertEvent

router = APIRouter(prefix="/api/alerts", tags=["alerts"])

@router.get("/active", response_model=List[AlertEvent])
def get_active_alerts(response: Response):
    response.headers["Cache-Control"] = "no-cache, no-store, must-revalidate"
    response.headers["Pragma"] = "no-cache"

    query = "SELECT * FROM v_active_alerts"
    results = execute_query(query)
    return results

@router.get("/history", response_model=List[AlertEvent])
def get_alert_history(
    days: int = Query(7, ge=1),
    alert_type: Optional[str] = Query(None),
    region: Optional[str] = Query(None),
    username: str = Depends(verify_token)
):
    base = """
        SELECT alert_type, severity, city, governorate, region,
               value, threshold, unit, triggered_at, cycle_id
        FROM weather_alerts
        WHERE triggered_at >= NOW() - (:days * INTERVAL '1 day')
    """
    params = {'days': days}

    if alert_type:
        base += " AND UPPER(alert_type) = UPPER(:alert_type)"
        params['alert_type'] = alert_type
    if region:
        base += " AND LOWER(region) = LOWER(:region)"
        params['region'] = region

    base += " ORDER BY triggered_at DESC"

    results = execute_query(base, params)
    if not results and (alert_type or region):
        raise HTTPException(status_code=404, detail="No alerts found for the specified filters")
    return results

@router.get("/stats")
def get_alert_stats(username: str = Depends(verify_token)):
    query = """
        SELECT alert_type, severity, COUNT(*) as count
        FROM weather_alerts
        WHERE triggered_at >= NOW() - (30 * INTERVAL '1 day')
        GROUP BY alert_type, severity
        ORDER BY count DESC
    """
    results = execute_query(query)
    return results
