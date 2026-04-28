import pytest
from fastapi.testclient import TestClient
from api.main import app

client = TestClient(app)

def test_health():
    response = client.get("/health")
    assert response.status_code == 200
    assert response.json()["status"] in ["ok", "degraded"]

def test_root_redirect():
    response = client.get("/", follow_redirects=False)
    assert response.status_code in [302, 307]

def test_protected_without_token():
    response = client.get("/api/dashboard/summary")
    assert response.status_code == 403

def test_login_invalid():
    response = client.post("/api/auth/login",
        data={"username": "wrong", "password": "wrong"})
    assert response.status_code == 401

def test_login_valid():
    response = client.post("/api/auth/login",
        data={"username": "admin", "password": "meteo2025"})
    assert response.status_code == 200
    assert "access_token" in response.json()
