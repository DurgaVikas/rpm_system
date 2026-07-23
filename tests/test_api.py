from fastapi.testclient import TestClient
from api.main import app
from api.routes.analytics import db
from datetime import datetime, timezone

client = TestClient(app)

def test_read_root():
    response = client.get("/")
    assert response.status_code == 200
    assert response.json() == {"message": "Welcome to the RPM Analytics API"}

def test_generate_token_success():
    payload = {"sensor_id": "11:89:9A:A2:7D:5B"}
    response = client.post("/api/v1/auth/token", json=payload)
    assert response.status_code == 200
    data = response.json()
    assert "access_token" in data
    assert data["token_type"] == "bearer"

def test_generate_token_invalid_payload():
    # missing sensor_id
    payload = {}
    response = client.post("/api/v1/auth/token", json=payload)
    assert response.status_code == 422

def test_get_analytics_summary():
    # Retrieve the mock connection & cursor from the global db client
    conn = db.get_connection()
    cursor = conn.cursor()
    
    # Pre-set mock database results matching TimescaleDB query outputs:
    earliest_ts = datetime(2026, 7, 16, 12, 0, 0, tzinfo=timezone.utc)
    latest_ts = datetime(2026, 7, 16, 13, 0, 0, tzinfo=timezone.utc)
    cursor.result_data = [(100, 75.25, 60.0, 95.0, 0.9543, earliest_ts, latest_ts)]
    
    sensor_id = "11:89:9A:A2:7D:5B"
    response = client.get(f"/api/v1/analytics/summary?sensor_id={sensor_id}")
    
    assert response.status_code == 200
    data = response.json()
    assert data["sensor_id"] == sensor_id
    assert data["total_readings"] == 100
    assert data["avg_heart_rate"] == 75.25
    assert data["min_heart_rate"] == 60.0
    assert data["max_heart_rate"] == 95.0
    assert data["avg_signal_quality"] == 0.9543
    assert data["time_range"]["earliest"] == earliest_ts.isoformat()
    assert data["time_range"]["latest"] == latest_ts.isoformat()
