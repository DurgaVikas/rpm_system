"""
DB Consumer Service for RPM System.

Subscribes to Kafka topics (raw_ecg, processed_ecg) and persists incoming
messages to TimescaleDB using reusable functions from core.db.timescaledb_utils.

This service runs as a standalone process alongside the WebSocket server
and Processing Engine.

Usage:
    python -m db_consumer.main
"""

import os
import json
import logging
from confluent_kafka import Consumer, KafkaError

from core.db.timescaledb_utils import TimescaleDBClient

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger("db_consumer")

KAFKA_BROKER = os.getenv("KAFKA_BROKER", "localhost:9092")
GROUP_ID = os.getenv("GROUP_ID", "db_consumer_group")

RAW_TOPIC = "raw_ecg"
PROCESSED_TOPIC = "processed_ecg"


def main():
    """Main loop: consume Kafka messages and persist to TimescaleDB."""

    # --- Kafka consumer setup ---
    consumer = Consumer(
        {
            "bootstrap.servers": KAFKA_BROKER,
            "group.id": GROUP_ID,
            "auto.offset.reset": "earliest",
        }
    )
    consumer.subscribe([RAW_TOPIC, PROCESSED_TOPIC])
    logger.info(f"Kafka consumer subscribed to [{RAW_TOPIC}, {PROCESSED_TOPIC}]")

    # --- TimescaleDB client (from core.db) ---
    db = TimescaleDBClient()
    logger.info("TimescaleDB client initialized")

    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                logger.error(f"Kafka error: {msg.error()}")
                break

            topic = msg.topic()
            try:
                val = msg.value()
                if val is None:
                    continue

                data = json.loads(val.decode("utf-8"))

                if topic == RAW_TOPIC:
                    _handle_raw_ecg(db, data)
                elif topic == PROCESSED_TOPIC:
                    _handle_processed_ecg(db, data)

            except Exception as e:
                logger.error(f"Error processing message from topic '{topic}': {e}")

    except KeyboardInterrupt:
        logger.info("DB Consumer shutting down (KeyboardInterrupt)")
    finally:
        consumer.close()
        db.close()
        logger.info("DB Consumer stopped")


def _handle_raw_ecg(db: TimescaleDBClient, data: dict) -> None:
    """Handle a raw_ecg Kafka message by batch-inserting vitals into TimescaleDB."""
    sensor_id = data.get("sensor_id", "unknown")
    vitals = data.get("vitals", [])

    if not vitals:
        logger.warning(f"Raw ECG message for sensor {sensor_id} has no vitals data")
        return

    count = db.insert_raw_ecg_batch(sensor_id, vitals)
    logger.info(f"Persisted {count} raw ECG readings for sensor {sensor_id}")


def _handle_processed_ecg(db: TimescaleDBClient, data: dict) -> None:
    """Handle a processed_ecg Kafka message by inserting metrics into TimescaleDB."""
    sensor_id = data.get("sensor_id", "unknown")
    heart_rate = data.get("heart_rate", 0.0)
    signal_quality = data.get("signal_quality", 0.0)
    ecg_clean = data.get("ecg_clean", [])

    db.insert_processed_ecg(
        sensor_id=sensor_id,
        heart_rate=heart_rate,
        signal_quality=signal_quality,
        ecg_clean=ecg_clean,
    )
    logger.info(
        f"Persisted processed ECG for sensor {sensor_id} "
        f"(HR={heart_rate:.1f}, Quality={signal_quality:.4f})"
    )


if __name__ == "__main__":
    main()
