import pytest
import asyncio
from unittest.mock import patch, MagicMock
from fastapi.testclient import TestClient
from core.auth import create_jwt_token
from websocket.main import app

@pytest.fixture
def mock_websocket_lifespan():
    with patch("websocket.main.ProcessedECGConsumer") as mock_consumer:
        yield mock_consumer

def test_websocket_auth_failure(mock_websocket_lifespan):
    client = TestClient(app)
    # Test connection closing code when no token is provided
    try:
        with client.websocket_connect("/ws/raw_ecg") as ws:
            pytest.fail("WebSocket connection should have failed without token")
    except Exception:
        # TestClient raises exception or closes websocket on auth failure
        pass

def test_websocket_stream_success(mock_websocket_lifespan):
    client = TestClient(app)
    token = create_jwt_token({"sub": "11:89:9A:A2:7D:5B"})

    with patch("websocket.routes.ecg_socket.KafkaProducer") as MockKafkaProducer:
        mock_producer_instance = MagicMock()
        MockKafkaProducer.return_value = mock_producer_instance

        with client.websocket_connect(f"/ws/raw_ecg?token={token}") as ws:
            payload = {
                "sensor_id": "11:89:9A:A2:7D:5B",
                "user_id": "user_123",
                "vitals": [{"e": 515, "t": 1771224330373}]
            }
            ws.send_json(payload)
            response = ws.receive_json()
            assert response["message"] == "success"

            # Check registered active connection
            from websocket.routes.ecg_socket import active_connections
            assert "user_123" in active_connections
            assert active_connections["user_123"]["sensor_id"] == "11:89:9A:A2:7D:5B"

            # Verify KafkaProducer received single-serialized dict
            mock_producer_instance.produce_message.assert_called_once()
            args, kwargs = mock_producer_instance.produce_message.call_args
            assert kwargs["key"] == "11:89:9A:A2:7D:5B"
            assert isinstance(kwargs["json_message"], dict)
            assert kwargs["json_message"]["sensor_id"] == "11:89:9A:A2:7D:5B"

def test_processed_ecg_consumer_broadcast():
    from websocket.processed_ecg_consumer import ProcessedECGConsumer
    from websocket.routes.ecg_socket import active_connections
    from core.kafka.consumer import set_event_loop

    # Setup mock event loop and connections
    mock_loop = MagicMock()
    mock_loop.is_closed.return_value = False
    set_event_loop(mock_loop)

    mock_ws = MagicMock()
    active_connections["user_abc"] = {
        "sensor_id": "sensor_456",
        "websocket": mock_ws
    }

    consumer = ProcessedECGConsumer("localhost:9092", "test_group", ["processed_ecg"])

    msg = {
        "sensor_id": "sensor_456",
        "ecg_clean": [1, 2, 3],
        "heart_rate": 72.0,
        "signal_quality": 0.95
    }

    with patch("asyncio.run_coroutine_threadsafe") as mock_run_coroutine:
        consumer.handle_message(msg)
        mock_run_coroutine.assert_called_once()
        args, kwargs = mock_run_coroutine.call_args
        assert args[1] is mock_loop
