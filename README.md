
# RPM System - Kafka Data Pipeline

A real-time ECG (Electrocardiogram) data processing system built on Apache Kafka, demonstrating a modern data pipeline architecture with real-time streaming, WebSocket communication, and asynchronous data processing.

## 📋 Project Overview

This project showcases **Kafka data pipeline skills** with a focus on building scalable, real-time data streaming applications. The system demonstrates a complete end-to-end data flow:

1. **Data Ingestion**: Users send raw ECG data via WebSocket connections
2. **Kafka Producer**: Data is published to Kafka topics
3. **Processing**: A processing engine consumes raw data and applies transformations
4. **Kafka Consumer**: Processed data is sent back through Kafka
5. **Output**: Processed ECG data is delivered to users via WebSocket

---

## 🏗️ Architecture & Project Structure

### 📍 **WebSocket Module** (`/websocket`)
**Purpose**: Handles real-time bidirectional communication with clients

#### Key Components:
- **main.py**: WebSocket server initialization and connection management
- **routes/ecg_socket.py**: WebSocket route handlers for ECG data
- **processed_ecg_consumer.py**: Kafka consumer that receives processed ECG data and broadcasts to connected WebSocket clients

#### Flow:
```
User → WebSocket → Raw ECG Data → Kafka Producer
                                        ↓
Processed ECG ← WebSocket ← Kafka Consumer ← Processing Engine
```

#### Key Features:
- Accepts raw ECG data from multiple concurrent users
- Publishes data to Kafka for distributed processing
- Consumes processed ECG results from Kafka
- Broadcasts results back to clients in real-time

---

### 🔧 **Core Module** (`/core`)
**Purpose**: Reusable Kafka infrastructure components

#### Key Components:
- **producer.py**: Kafka producer configuration and methods for publishing messages
- **consumer.py**: Kafka consumer configuration and methods for consuming messages

#### Features:
- Centralized producer/consumer configuration
- Reusable across all modules in the application
- Handles Kafka connection management
- Topic publishing and message serialization
- Consumer group management and message deserialization

#### Usage:
```python
from core.producer import produce_message
from core.consumer import consume

# Publish incoming sensor payloads to Kafka for asynchronous processing
produce_message(key=sensor_id, json_message=json_data)

# Dedicated multiprocessing-based consumer for CPU-intensive
consumer = HealthVitals(brokers, group_id, topic, kafka=True)
consumer.consume()

# Lifespan-managed consumer for lightweight real-time tasks
consumer = ProcessedECGConsumer(brokers, group_id, topic)
consumer.start_consumer()

# Gracefully stop the consumer during application shutdown
consumer.stop_consumer(consumer_id="ProcessedECGConsumer")

```

---

### ⚙️ **Processing Engine** (`/processing_engine`)
**Purpose**: Real-time data processing and transformation

#### Key Components:
- **main.py**: Main processing engine orchestration
- **vitals/ecg_vitals.py**: ECG signal processing algorithms

#### Processing Flow:
1. **Consumes** raw ECG data from Kafka topic (`raw-ecg`)
2. **Processes** the signals using ECG vitals algorithms:
   - Signal filtering and noise reduction
   - QRS complex detection
   - Heart rate calculation
   - Waveform analysis
3. **Produces** processed ECG data back to Kafka topic (`processed-ecg`)
4. **Two-way Communication**: Enables real-time communication in same websocket connections

#### Features:
- Asynchronous message processing
- Stateless design for horizontal scalability
- ECG-specific signal processing algorithms
- Error handling and retry logic

---

### 🐳 **Docker Configuration** (`/docker`)
**Purpose**: Container orchestration for Kafka and supporting services

#### Components:
- **kafka_docker_compose**: Docker Compose configuration for:
  - Apache Kafka broker(s)
  - Apache ZooKeeper
  - Optional but recommended: Kafka UI for monitoring

#### Quick Start:
```bash
cd docker
docker-compose -f kafka_docker_compose up -d
```

---

### 🔌 **API Module** (`/api`) -- Upcoming
**Purpose**: REST API endpoints for system interactions

#### Features:
- Health check endpoints
- Optional: Kafka topic management
- Optional: Analytics queries

---

### 🧪 **Tests Directory** (`/tests`) -- Upcoming
**Purpose**: Test suites for all modules

#### Structure:
- `api/`: API endpoint tests
- `websocket/`: WebSocket connection and communication tests
- `processing_engine/`: Data processing tests
- `database/`: Database operation tests

---

## 🚀 Data Pipeline Flow (Phase 1)

```
┌─────────────────────────────────────────────────────────────┐
│                    Real-time ECG Data Pipeline                │
└─────────────────────────────────────────────────────────────┘

    ┌──────────────┐
    │  Web Client  │
    └───────┬──────┘
            │ (WebSocket)
            ↓
    ┌──────────────────┐
    │  WebSocket App   │────────────┐
    │  (main.py)       │            │
    └──────────────────┘            │
            │                       │
            │ (Raw ECG)            │ (Processed ECG)
            ↓                       ↑
    ┌─────────────────────────────────────────┐
    │         Apache Kafka Cluster            │
    │  ┌──────────────┐  ┌──────────────────┐ │
    │  │ raw-ecg      │→ │ processed-ecg    │ │
    │  │ (topic)      │  │ (topic)          │ │
    │  └──────────────┘  └──────────────────┘ │
    └────────┬──────────────────────┬──────────┘
             │                      ↑
             │ (Kafka Consumer)     │ (Kafka Producer)
             ↓                      │
    ┌──────────────────────────────────┐
    │  Processing Engine               │
    │  ├─ consumer.py                  │
    │  ├─ main.py                      │
    │  └─ vitals/ecg_vitals.py        │
    └──────────────────────────────────┘
```

---

## 📦 Technology Stack

| Component               | Technology                 |
|-------------------------|----------------------------|
| Message Broker          | Apache Kafka               |
| Real-time Communication | WebSocket                  |
| Language                | Python 3.12+               |
| Signal Processing       | Neurokit2 (ECG algorithms) |
| Containerization        | Docker & Docker Compose    |
| API Framework           | FastAPI (recommended)      |

---

## 🛠️ Installation & Setup

### Prerequisites
- Python 3.12+
- Docker & Docker Compose
- Apache Kafka (or Docker)

### Step 1: Clone and Setup Environment
```bash
cd C:\Users\Durga Vikas DF\PycharmProjects\rpm_system
python -m venv venv
venv\Scripts\activate
```

### Step 2: Install Dependencies
```bash
pip install -r api/requirements.txt
pip install -r websocket/requirements.txt
pip install -r processing_engine/requirements.txt
```

### Step 3: Start Kafka
```bash
cd docker
docker-compose -f kafka_docker_compose up -d
```

### Step 4: Run Components
```bash
# Terminal 1: Processing Engine
cd processing_engine
python main.py

# Terminal 2: WebSocket Server
cd websocket
python main.py

# Terminal 3: API Server
cd api
python main.py
```

---

## 📊 Key Concepts: Kafka Data Pipeline

### Producer Pattern
The WebSocket server acts as a **Kafka Producer**:
- Receives raw ECG data from clients
- Publishes to `raw-ecg` topic
- Ensures at-least-once delivery

### Consumer Pattern
Two consumers operate in this pipeline:
1. **Processing Engine Consumer**: Subscribes to `raw-ecg` topic
2. **WebSocket Consumer**: Subscribes to `processed-ecg` topic

### Topic Architecture
```
raw-ecg (Topic)
├─ Partition 0
├─ Partition 1
└─ Partition N

processed-ecg (Topic)
├─ Partition 0
├─ Partition 1
└─ Partition N
```

### Consumer Groups
- **processing-group**: Processing engine instances
- **websocket-group**: WebSocket server instances

---

## 🔄 Example: Data Flow

1. **Client sends ECG data via WebSocket**:
   ```javascript
   ws.send(JSON.stringify({ 
     type: 'ecg_data', 
     data: [120, 125, 130, ...] 
   }));
   ```

2. **WebSocket produces to Kafka**:
   ```python
    kafka_broker = os.getenv('kafka_broker', 'localhost:9092')
    producer = KafkaProducer(kafka_broker, topic="raw_ecg")
    produce_message(key=sensor_id, json_message=json_data)
   ```

3. **Processing Engine consumes and processes**:
   ```python
   consumer = HealthVitals(brokers, group_id, topic, kafka=True)
   consumer.consume()
   processed = process_ecg(raw_data)
   
   kafka_broker = os.getenv('kafka_broker', 'localhost:9092')
   producer = KafkaProducer(kafka_broker, topic="raw_ecg")
   produce_message(key=sensor_id, json_message=json_data)
   ```

4. **WebSocket broadcasts to client**:
   ```python
   consumer = ProcessedECGConsumer(brokers, group_id, topic)
   consumer.start_consumer()
   
   # On receiving processed ECG data
   ProcessedECGConsumer → Broadcast to WebSocket clients
   ```
---

## 📝 Phase 1 - Current Implementation

✅ **Completed**:
- [x] Kafka producer/consumer infrastructure
- [x] WebSocket server for client communication
- [x] Processing engine for ECG data transformation
- [x] Docker setup for Kafka
- [x] Basic two-way data pipeline
- [x] Core reusable components

---

## 🚧 Phase 2 - Upcoming Features

📋 **Planned Features**:
- [ ] **Tiger Data Database Integration**: Persistent storage of ECG records and analytics
- [ ] **Unit Tests**: Comprehensive testing for WebSocket connections and message flow
- [ ] **Kafka Analytics APIs**: REST endpoints for querying pipeline metrics
  - Message throughput
  - Processing latency
  - Consumer lag monitoring
- [ ] **Authorization & Authentication**: Token-based access control for WebSocket and API endpoints
- [ ] **Data Validation**: Schema validation for ECG data
- [ ] **Error Handling & Retry Logic**: Enhanced resilience
- [ ] **Monitoring & Logging**: Centralized logging and metrics collection
- [ ] **Horizontal Scaling**: Load balancing for multiple processing engine instances

---

## 📚 Configuration Files

Each module contains a `requirements.txt` with dependencies:

```bash
# Core dependencies across modules
kafka-python
websockets
fastapi
uvicorn
neurokit2
```

---

## 🔍 Monitoring & Debugging

### Kafka Topics
```bash
# List all topics
kafka-topics.sh --list --bootstrap-server localhost:9092

# Consume messages from topic
kafka-console-consumer.sh --topic raw-ecg --bootstrap-server localhost:9092

# Monitor consumer lag
kafka-consumer-groups.sh --bootstrap-server localhost:9092 --group processing-group --describe
```

### WebSocket Testing
```bash
# Test WebSocket connection
wscat -c ws://localhost:8000/ws/ecg
```

---

## 🎯 Learning Objectives

This project demonstrates:
1. **Event-Driven Architecture**: Using Kafka for decoupled systems
2. **Real-time Processing**: Stream processing and data transformations
3. **Producer-Consumer Pattern**: Asynchronous message passing
4. **WebSocket Communication**: Bidirectional client-server communication
5. **Scalability**: Design for horizontal scaling
6. **Separation of Concerns**: Modular, reusable components

---

## 🤝 Contributing

Future improvements and refinements welcome. This is a Phase 1 implementation focusing on core Kafka pipeline skills.

---

## 📄 License

See LICENSE file for details.

---

**Last Updated**: Phase 1 Development
**Next Phase**: Tiger Data DB Integration & Enhanced Analytics
