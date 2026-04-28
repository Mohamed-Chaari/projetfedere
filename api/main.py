import os
import logging
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import RedirectResponse
from api.routers import dashboard, analysis, forecast, alerts, auth
from api.database import health_check

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s"
)
logger = logging.getLogger(__name__)

app = FastAPI(
    title="Tunisia Meteo API",
    description="National Weather Analytics System — 221 cities, 24 governorates",
    version="1.0.0",
)

ALLOWED_ORIGINS = os.getenv("ALLOWED_ORIGINS", "*").split(",")
app.add_middleware(
    CORSMiddleware,
    allow_origins=ALLOWED_ORIGINS,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.on_event("startup")
async def startup():
    from api.database import health_check
    if not health_check():
        raise RuntimeError("Cannot connect to Supabase on startup")
    logger.info("Tunisia Meteo API started — DB connected")

@app.get("/")
def root():
    return RedirectResponse(url="/docs")

app.include_router(auth.router,      prefix="/api/auth",      tags=["Auth"])
app.include_router(dashboard.router, prefix="/api/dashboard", tags=["Dashboard"])
app.include_router(analysis.router,  prefix="/api/analysis",  tags=["Analysis"])
app.include_router(forecast.router,  prefix="/api/forecast",  tags=["Forecast"])
app.include_router(alerts.router,    prefix="/api/alerts",    tags=["Alerts"])

@app.get("/health")
def health():
    db_ok = health_check()
    return {
        "status": "ok" if db_ok else "degraded",
        "service": "Tunisia Meteo API",
        "database": "connected" if db_ok else "disconnected"
    }
