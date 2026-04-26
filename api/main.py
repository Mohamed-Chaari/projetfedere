from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from api.routers import dashboard, analysis, forecast, alerts, auth

app = FastAPI(
    title="Tunisia Meteo API",
    description="National Weather Analytics System — 221 cities, 24 governorates",
    version="1.0.0",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(auth.router,      prefix="/api/auth",      tags=["Auth"])
app.include_router(dashboard.router, prefix="/api/dashboard", tags=["Dashboard"])
app.include_router(analysis.router,  prefix="/api/analysis",  tags=["Analysis"])
app.include_router(forecast.router,  prefix="/api/forecast",  tags=["Forecast"])
app.include_router(alerts.router,    prefix="/api/alerts",    tags=["Alerts"])

@app.get("/health")
def health():
    return {"status": "ok", "service": "Tunisia Meteo API"}
