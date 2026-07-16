import pytest
from datetime import datetime, timezone
from unittest.mock import MagicMock, patch
from core.db.timescaledb_utils import TimescaleDBClient

def test_insert_raw_ecg_batch():
    db = TimescaleDBClient()
    conn = db.get_connection()
    cursor = conn.cursor()

    vitals = [
        {"e": 515, "t": 1771224330373},
        {"e": None, "t": 1771224330374},  # Should be skipped
        {"e": 510, "t": 1771224330375}
    ]

    # Reset cursor executed queries
    cursor.executed_queries = []

    inserted = db.insert_raw_ecg_batch("sensor_abc", vitals)

    assert inserted == 2
    assert len(cursor.executed_queries) == 1
    query, params = cursor.executed_queries[0]
    assert "INSERT INTO raw_ecg_readings" in query
    # Check that skip worked: params (the rows list) should only have 2 elements
    assert len(params) == 2
    # Check sensor_id in first row
    assert params[0][1] == "sensor_abc"
    assert params[0][2] == 515.0
    assert params[1][2] == 510.0
    assert conn.committed is True

def test_insert_processed_ecg():
    db = TimescaleDBClient()
    conn = db.get_connection()
    cursor = conn.cursor()

    cursor.executed_queries = []
    ts = datetime.now(tz=timezone.utc)
    db.insert_processed_ecg(
        sensor_id="sensor_abc",
        heart_rate=72.5,
        signal_quality=0.975,
        ecg_clean=[1.0, 1.2, 1.3],
        timestamp=ts
    )

    assert len(cursor.executed_queries) == 1
    query, params = cursor.executed_queries[0]
    assert "INSERT INTO processed_ecg_metrics" in query
    assert params[0] == ts
    assert params[1] == "sensor_abc"
    assert params[2] == 72.5
    assert params[3] == 0.975
    assert params[4] == [1.0, 1.2, 1.3]
    assert conn.committed is True

def test_get_analytics_summary():
    db = TimescaleDBClient()
    conn = db.get_connection()
    cursor = conn.cursor()

    cursor.executed_queries = []
    # Mock return value: (total_readings, avg_heart_rate, min_heart_rate, max_heart_rate, avg_signal_quality, earliest, latest)
    earliest_ts = datetime(2026, 7, 16, 12, 0, 0, tzinfo=timezone.utc)
    latest_ts = datetime(2026, 7, 16, 13, 0, 0, tzinfo=timezone.utc)
    cursor.result_data = [(100, 75.25, 60.0, 95.0, 0.9543, earliest_ts, latest_ts)]

    summary = db.get_analytics_summary("sensor_abc")

    assert summary["sensor_id"] == "sensor_abc"
    assert summary["total_readings"] == 100
    assert summary["avg_heart_rate"] == 75.25
    assert summary["min_heart_rate"] == 60.0
    assert summary["max_heart_rate"] == 95.0
    assert summary["avg_signal_quality"] == 0.9543
    assert summary["time_range"]["earliest"] == earliest_ts.isoformat()
    assert summary["time_range"]["latest"] == latest_ts.isoformat()

def test_get_anomalies():
    db = TimescaleDBClient()
    conn = db.get_connection()
    cursor = conn.cursor()

    cursor.executed_queries = []
    ts1 = datetime(2026, 7, 16, 12, 30, 0, tzinfo=timezone.utc)
    ts2 = datetime(2026, 7, 16, 12, 45, 0, tzinfo=timezone.utc)
    # Mock query rows: (time, heart_rate, signal_quality)
    cursor.result_data = [
        (ts1, 110.0, 0.92),
        (ts2, 55.0, 0.88)
    ]

    anomalies = db.get_anomalies("sensor_abc", hr_low=60.0, hr_high=100.0)

    assert len(anomalies) == 2
    assert anomalies[0]["time"] == ts1.isoformat()
    assert anomalies[0]["heart_rate"] == 110.0
    assert anomalies[0]["anomaly_type"] == "tachycardia"
    assert anomalies[1]["time"] == ts2.isoformat()
    assert anomalies[1]["heart_rate"] == 55.0
    assert anomalies[1]["anomaly_type"] == "bradycardia"
