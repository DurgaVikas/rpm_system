import json
import pytest
from unittest.mock import MagicMock, patch
from db_consumer.main import main, _handle_raw_ecg, _handle_processed_ecg
from tests.conftest import MockKafkaMessage, MockConsumer

def test_handle_raw_ecg():
    mock_db = MagicMock()
    data = {
        "sensor_id": "sensor_123",
        "vitals": [{"e": 100, "t": 1000}, {"e": 101, "t": 2000}]
    }
    _handle_raw_ecg(mock_db, data)
    mock_db.insert_raw_ecg_batch.assert_called_once_with("sensor_123", data["vitals"])

def test_handle_processed_ecg():
    mock_db = MagicMock()
    data = {
        "sensor_id": "sensor_123",
        "heart_rate": 75.0,
        "signal_quality": 0.98,
        "ecg_clean": [1, 2, 3]
    }
    _handle_processed_ecg(mock_db, data)
    mock_db.insert_processed_ecg.assert_called_once_with(
        sensor_id="sensor_123",
        heart_rate=75.0,
        signal_quality=0.98,
        ecg_clean=[1, 2, 3]
    )

def test_db_consumer_main_loop():
    raw_data = {"sensor_id": "sensor_raw", "vitals": [{"e": 100, "t": 1000}]}
    processed_data = {"sensor_id": "sensor_proc", "heart_rate": 80.0, "signal_quality": 0.9, "ecg_clean": [4, 5]}

    msg_raw = MockKafkaMessage("raw_ecg", "sensor_raw", json.dumps(raw_data))
    msg_proc = MockKafkaMessage("processed_ecg", "sensor_proc", json.dumps(processed_data))

    class TestConsumer(MockConsumer):
        def poll(self, timeout):
            if self._msg_queue:
                return self._msg_queue.pop(0)
            raise KeyboardInterrupt()

    mock_consumer_instance = TestConsumer(None)
    mock_consumer_instance.feed_mock_messages([msg_raw, msg_proc])

    mock_db_instance = MagicMock()

    with patch("db_consumer.main.Consumer", return_value=mock_consumer_instance), \
         patch("db_consumer.main.TimescaleDBClient", return_value=mock_db_instance):
        main()

    # Verify correct insert logic was executed
    mock_db_instance.insert_raw_ecg_batch.assert_called_once_with("sensor_raw", raw_data["vitals"])
    mock_db_instance.insert_processed_ecg.assert_called_once_with(
        sensor_id="sensor_proc",
        heart_rate=80.0,
        signal_quality=0.9,
        ecg_clean=processed_data["ecg_clean"]
    )
    mock_db_instance.close.assert_called_once()
