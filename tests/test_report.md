# RPM System Testing Report

## 1. Test Execution Summary

A total of 17 tests were executed using `pytest` inside the local virtual environment. All 17 tests passed successfully in 3.47 seconds with 100% offline test execution.

| Module | Test Name | Status | Description |
|---|---|---|---|
| **REST API** | `test_read_root` | **PASSED** | Verifies `GET /` returns the correct welcome message. |
| | `test_generate_token_success` | **PASSED** | Verifies `POST /api/v1/auth/token` returns a valid JWT token. |
| | `test_generate_token_invalid_payload` | **PASSED** | Verifies `/token` endpoint validates request input schema. |
| | `test_get_analytics_summary` | **PASSED** | Verifies GET `/api/v1/analytics/summary?sensor_id=...` integrates with `TimescaleDBClient` and correctly validates return fields. |
| **TimescaleDB Client** | `test_insert_raw_ecg_batch` | **PASSED** | Verifies raw ECG data batch insertions filter null readings and format rows correctly. |
| | `test_insert_processed_ecg` | **PASSED** | Verifies processed ECG metrics inserts format arrays and timestamps correctly. |
| | `test_get_analytics_summary` | **PASSED** | Verifies SQL aggregation calculations query against mocked cursor results. |
| | `test_get_anomalies` | **PASSED** | Verifies bradycardia and tachycardia anomaly classification logic. |
| **DB Consumer** | `test_handle_raw_ecg` | **PASSED** | Verifies raw ECG event consumption forwards formatted rows to `insert_raw_ecg_batch`. |
| | `test_handle_processed_ecg` | **PASSED** | Verifies processed metrics event consumption forwards to `insert_processed_ecg`. |
| | `test_db_consumer_main_loop` | **PASSED** | Verifies full event loop consumption and graceful shutdown under KeyboardInterrupt. |
| **Processing Engine** | `test_health_vitals_processing_success` | **PASSED** | Verifies NeuroKit2 digital signal processing and heart rate/quality extraction. |
| | `test_health_vitals_zero_division_handling` | **PASSED** | Verifies fallback to 250 Hz sampling rate for short datasets (len < 15) to prevent zero-division. |
| | `test_health_vitals_exception_handling` | **PASSED** | Verifies graceful error logging and resilience on corrupt payloads. |
| **WebSocket** | `test_websocket_auth_failure` | **PASSED** | Verifies WebSocket connection closes under missing or invalid JWT tokens. |
| | `test_websocket_stream_success` | **PASSED** | Verifies connection registry, success responses, and Kafka forwarding on valid JWT. |
| | `test_processed_ecg_consumer_broadcast` | **PASSED** | Verifies thread-safe asynchronous broadcast of processed metrics to active websocket clients. |

---

## 2. Identified Bugs & Resolutions

### Bug 1: Non-Existent Function Import in REST API
- **Location**: `api/routes/auth.py`
- **Error**: Attempted to import `create_access_token` from `core.auth` which was not defined.
- **Resolution**: Changed the import to use `create_jwt_token` and updated the token generation call inside the `generate_token` handler.

### Bug 2: Non-Existent Module Import in WebSocket
- **Location**: `websocket/routes/ecg_socket.py`
- **Error**: Attempted to import `KafkaProducer` from `core.producer` which does not exist (the module is `core.kafka.producer`).
- **Resolution**: Updated import path to `core.kafka.producer`.

### Bug 3: Double-Serialization / Double-Deserialization (DB Consumer Crash)
- **Location**: `websocket/routes/ecg_socket.py` & `processing_engine/vitals/ecg_vitals.py`
- **Error**:
  1. WebSocket route serialized incoming payloads twice by dump-stringing dictionaries to Kafka, while the Kafka producer serialized it a second time.
  2. The database consumer only deserialized once, receiving a string instead of a dictionary and crashing with `AttributeError` when trying to call `.get()`.
- **Resolution**:
  1. Updated `websocket/routes/ecg_socket.py` to pass the raw dictionary `data` to `producer.produce_message`.
  2. Removed the redundant second `json.loads` inside `processing_engine/vitals/ecg_vitals.py` by mapping `parsed_message = message`.

### Bug 4: Circular and Framework Dependency Logger Mismatches
- **Location**: `core/kafka/consumer.py` & `core/kafka/producer.py`
- **Error**:
  1. `core/kafka/consumer.py` imported `logger` from `processing_engine`, causing circular import issues when the engine imported the core.
  2. `core/kafka/producer.py` imported `logger` from `fastapi.logger`, forcing non-web services (Processing Engine, DB Consumer) to depend on FastAPI.
- **Resolution**: Replaced both imports with standard Python library logging setup (`import logging; logger = logging.getLogger(__name__)`).

### Bug 5: Zero-Division / Input Length Vulnerability in NeuroKit2 Processing
- **Location**: `processing_engine/vitals/ecg_vitals.py`
- **Error**: `sampling_rate` was computed as `int(len(raw_ecg)/15)`. Short data packages (len < 15) produced a sampling rate of `0`, crashing NeuroKit2 on division-by-zero.
- **Resolution**: Enforced a fallback default sampling rate of `250` Hz if `sampling_rate <= 0`.

### Bug 6: Missing Package Dependencies
- **Location**: `.venv` environment
- **Error**: `python-dotenv` and `psycopg2` were not installed, causing database utilities and startup scripts to fail.
- **Resolution**: Installed `python-dotenv` and `psycopg2-binary` inside the local virtual environment.

### Bug 7: Facade Implementation in REST API Analytics Route
- **Location**: `api/routes/analytics.py` & `tests/test_api.py`
- **Error**: The endpoint `/api/v1/analytics/summary` returned a hardcoded mockup dictionary with rotations per minute (RPM) values.
- **Resolution**: Replaced the mock dict with a call to `TimescaleDBClient.get_analytics_summary(sensor_id)`, passing `sensor_id` as a query parameter. Updated the unit test to feed database mock results and verify return keys (`avg_heart_rate`, `total_readings`, etc.).

---

## 3. Mocking & Offline Test Architecture

1. **Kafka Mocks (`confluent-kafka`)**:
   - `MockProducer`: Tracks topics, keys, and values produced; triggers callback delivery.
   - `MockConsumer`: Supports queuing messages to mock stream ingestion, simulating live subscription polling offline.
2. **Database Mocks (`psycopg2`)**:
   - `MockConnectionPool`, `MockConnection`, and `MockCursor`: Captures executed queries and bound parameters.
   - Mocked `psycopg2.extras.execute_values` to test batch inserts.
3. **WebSockets**:
   - `FastAPI TestClient` coupled with lifecycle mock fixtures to prevent real background Kafka broker thread spawning.
