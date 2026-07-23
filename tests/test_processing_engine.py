import json
import os
import pytest
from unittest.mock import MagicMock, patch
import numpy as np
import pandas as pd

from processing_engine.vitals.ecg_vitals import HealthVitals

@pytest.fixture
def sample_ecg_data():
    path = os.path.join(os.path.dirname(__file__), "raw_ecg.json")
    with open(path, "r") as f:
        return json.load(f)

def test_health_vitals_processing_success(sample_ecg_data):
    # Instantiate consumer with kafka=False to avoid super().__init__ setup
    consumer = HealthVitals(brokers=None, group_id=None, topic=None, kafka=False)
    consumer.producer = MagicMock()

    # Use full vitals list so calculated sampling rate is correct (~250 Hz)
    msg = {
        "sensor_id": sample_ecg_data["sensor_id"],
        "vitals": sample_ecg_data["vitals"]
    }

    # Run handler
    consumer.handle_message(msg)

    # Verify producer was called with processed results
    consumer.producer.produce_message.assert_called_once()
    args, kwargs = consumer.producer.produce_message.call_args
    assert kwargs["key"] == sample_ecg_data["sensor_id"]
    payload = kwargs["json_message"]
    assert payload["sensor_id"] == sample_ecg_data["sensor_id"]
    assert "ecg_clean" in payload
    assert "heart_rate" in payload
    assert "signal_quality" in payload
    assert isinstance(payload["heart_rate"], float)
    assert isinstance(payload["signal_quality"], float)

def test_health_vitals_zero_division_handling():
    consumer = HealthVitals(brokers=None, group_id=None, topic=None, kafka=False)
    consumer.producer = MagicMock()

    # Feed a very short vitals array (length < 15, e.g., 5 points)
    msg = {
        "sensor_id": "short_sensor",
        "vitals": [{"e": 500, "t": 1000}, {"e": 505, "t": 2000}]
    }

    # Mock NeuroKit2 to prevent it from complaining about short signals
    dummy_signals = pd.DataFrame({
        "ECG_Clean": [1.0, 1.1],
        "ECG_Rate": [70.0, 70.0],
        "ECG_Quality": [0.9, 0.9]
    })
    dummy_info = {}

    with patch("neurokit2.ecg_process", return_value=(dummy_signals, dummy_info)) as mock_ecg_process:
        consumer.handle_message(msg)

        # Verify sampling_rate fallback to 250 was passed
        mock_ecg_process.assert_called_once()
        args, kwargs = mock_ecg_process.call_args
        assert kwargs["sampling_rate"] == 250

        consumer.producer.produce_message.assert_called_once()
        payload = consumer.producer.produce_message.call_args[1]["json_message"]
        assert payload["heart_rate"] == 70.0
        assert payload["signal_quality"] == 0.9

def test_health_vitals_exception_handling():
    consumer = HealthVitals(brokers=None, group_id=None, topic=None, kafka=False)
    consumer.producer = MagicMock()

    # Feed corrupted payload (e.g. string or missing keys)
    msg = "corrupted_non_dict_message"

    # Should not raise exception, but return None and log
    consumer.handle_message(msg)
    consumer.producer.produce_message.assert_not_called()
