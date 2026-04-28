import pytest
from fastapi.testclient import TestClient
from unittest.mock import patch
from api.main import app

client = TestClient(app)

@pytest.fixture
def auth_token():
    response = client.post("/api/auth/login", data={"username": "admin", "password": "meteo2025"})
    assert response.status_code == 200
    return response.json()["access_token"]

# AUTH Tests
def test_login_valid():
    response = client.post("/api/auth/login", data={"username": "admin", "password": "meteo2025"})
    assert response.status_code == 200
    assert "access_token" in response.json()

def test_login_invalid():
    response = client.post("/api/auth/login", data={"username": "admin", "password": "wrongpassword"})
    assert response.status_code == 401

def test_me_with_token(auth_token):
    response = client.get("/api/auth/me", headers={"Authorization": f"Bearer {auth_token}"})
    assert response.status_code == 200
    assert response.json()["username"] == "admin"
    assert response.json()["role"] == "admin"

def test_me_without_token():
    response = client.get("/api/auth/me")
    assert response.status_code == 403

# DASHBOARD Tests
def test_summary_protected():
    response = client.get("/api/dashboard/summary")
    assert response.status_code == 403

def test_current_protected():
    response = client.get("/api/dashboard/current")
    assert response.status_code == 403

def test_city_not_found(auth_token):
    with patch("api.routers.dashboard.execute_query") as mock_q:
        mock_q.return_value = []
        response = client.get("/api/dashboard/current/FakeCity123", headers={"Authorization": f"Bearer {auth_token}"})
        assert response.status_code == 404

# FORECAST Tests
def test_forecast_protected():
    response = client.get("/api/forecast/Tunis")
    assert response.status_code == 403

def test_forecast_city_404(auth_token):
    with patch("api.routers.forecast.execute_query") as mock_q:
        mock_q.return_value = []
        response = client.get("/api/forecast/FakeCity123", headers={"Authorization": f"Bearer {auth_token}"})
        assert response.status_code == 404

# ANALYSIS Tests
def test_monthly_protected():
    response = client.get("/api/analysis/monthly")
    assert response.status_code == 403

def test_peaks_protected():
    response = client.get("/api/analysis/peaks")
    assert response.status_code == 403

def test_correlation_protected():
    response = client.get("/api/analysis/correlation?season=Summer")
    assert response.status_code == 403

def test_annual_protected():
    response = client.get("/api/analysis/annual")
    assert response.status_code == 403

# ALERTS Tests
def test_alerts_active_public():
    with patch("api.routers.alerts.execute_query") as mock_q:
        mock_q.return_value = []
        response = client.get("/api/alerts/active")
        assert response.status_code == 200

def test_alerts_history_protected():
    response = client.get("/api/alerts/history")
    assert response.status_code == 403
