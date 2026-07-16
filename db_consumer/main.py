import os
import json
import logging
from datetime import datetime
from confluent_kafka import Consumer, KafkaError
import psycopg2

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger('db_consumer')

KAFKA_BROKER = os.getenv('KAFKA_BROKER', 'localhost:9092')
DB_HOST = os.getenv('DB_HOST', 'localhost')
DB_PORT = os.getenv('DB_PORT', '5432')
DB_USER = os.getenv('DB_USER', 'user')
DB_PASSWORD = os.getenv('DB_PASSWORD', 'password')
DB_NAME = os.getenv('DB_NAME', 'rpm_db')
GROUP_ID = os.getenv('GROUP_ID', 'db_consumer_group')

RAW_TOPIC = 'raw_ecg'
PROCESSED_TOPIC = 'processed_ecg'

def get_db_connection():
    return psycopg2.connect(
        host=DB_HOST,
        port=DB_PORT,
        user=DB_USER,
        password=DB_PASSWORD,
        dbname=DB_NAME
    )

def main():
    consumer = Consumer({
        'bootstrap.servers': KAFKA_BROKER,
        'group.id': GROUP_ID,
        'auto.offset.reset': 'earliest'
    })

    consumer.subscribe([RAW_TOPIC, PROCESSED_TOPIC])

    logger.info(f"Connected to Kafka broker at {KAFKA_BROKER}")
    
    conn = get_db_connection()
    conn.autocommit = True
    cursor = conn.cursor()
    logger.info("Connected to TimescaleDB")

    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None: continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF: continue
                else:
                    logger.error(msg.error())
                    break

            topic = msg.topic()
            try:
                val = msg.value()
                if val is None: continue
                data = json.loads(val.decode('utf-8'))
                
                timestamp = data.get('timestamp') or data.get('time') or datetime.utcnow().isoformat()
                patient_id = data.get('patient_id', 'unknown')
                device_id = data.get('device_id', 'unknown')
                
                if topic == RAW_TOPIC:
                    reading_value = data.get('reading_value', data.get('value', 0.0))
                    cursor.execute(
                        "INSERT INTO raw_ecg_readings (time, patient_id, device_id, reading_value) VALUES (%s, %s, %s, %s)",
                        (timestamp, patient_id, device_id, float(reading_value))
                    )
                elif topic == PROCESSED_TOPIC:
                    heart_rate = data.get('heart_rate')
                    arrhythmia_detected = data.get('arrhythmia_detected')
                    anomaly_score = data.get('anomaly_score')
                    signal_quality = data.get('signal_quality')
                    cursor.execute(
                        """INSERT INTO processed_ecg_metrics 
                        (time, patient_id, device_id, heart_rate, arrhythmia_detected, anomaly_score, signal_quality) 
                        VALUES (%s, %s, %s, %s, %s, %s, %s)""",
                        (timestamp, patient_id, device_id, heart_rate, arrhythmia_detected, anomaly_score, signal_quality)
                    )
            except Exception as e:
                logger.error(f"Error processing message: {e}")
    except KeyboardInterrupt:
        pass
    finally:
        consumer.close()
        if cursor: cursor.close()
        if conn: conn.close()

if __name__ == '__main__':
    main()
