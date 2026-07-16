import json
import os
from fastapi.logger import logger
from core.consumer import KafkaConsumer
import neurokit2 as nk
import pandas as pd
from core.producer import KafkaProducer


class HealthVitals(KafkaConsumer):
    def __init__(self, brokers, group_id,topic,kafka):
        if kafka:
            super().__init__(brokers, group_id,topic)
            kafka_broker = os.getenv('kafka_broker', 'localhost:9092')
            self.producer = KafkaProducer(kafka_broker, topic="processed_ecg")

    def handle_message(self,message, key=None):
        """
        Process incoming ECG data, extract metrics, and produce results to Kafka.
        """
        logger.debug(f"Received message: {message}")
        try:
            parsed_message = json.loads(message)
            vitals = parsed_message["vitals"]  # if only one device record
            df = pd.DataFrame(vitals)
            sensor_id = parsed_message.get("sensor_id", "unknown")
            raw_ecg = df["e"].dropna().to_numpy()
            signals, info = nk.ecg_process(raw_ecg, sampling_rate=int(len(raw_ecg)/15))

            # Extract ECG metrics
            ecg_clean = signals["ECG_Clean"] if "ECG_Clean" in signals else pd.Series(dtype=float)
            ecg_rate = signals["ECG_Rate"].dropna().mean() if "ECG_Rate" in signals and not signals["ECG_Rate"].dropna().empty else 0.0
            ecg_quality = signals["ECG_Quality"].dropna().mean() if "ECG_Quality" in signals and not signals["ECG_Quality"].dropna().empty else 0.0

            # Create JSON payload with required metrics
            payload = {
                "sensor_id": sensor_id,
                "ecg_clean": ecg_clean.tolist(),
                "heart_rate": float(ecg_rate),
                "signal_quality": float(ecg_quality)
            }

            logger.info(f"ECG Data - Rate: {ecg_rate}, Quality: {ecg_quality}")
            self.producer.produce_message(key=sensor_id, json_message=payload)
        except Exception as e:
            logger.error(f"Error processing ECG data for sensor : {e}")
            return


if __name__ == "__main__":
    brokers = os.getenv('KAFKA_BROKERS', 'localhost:9092')
    group_id = "health_vitals"
    topic = ["raw_ecg"]
    consumer = HealthVitals(brokers, group_id, topic,kafka=True)
    consumer.consume()


    

