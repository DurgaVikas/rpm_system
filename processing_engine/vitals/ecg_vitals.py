import os
from fastapi.logger import logger
from core.consumer import KafkaConsumer


class HealthVitals(KafkaConsumer):
    def __init__(self, brokers, group_id,topic,kafka):
        if kafka:
            super().__init__(brokers, group_id,topic)


    def handle_message(self,message):
        logger.info(f"Received message: {message}")

if __name__ == "__main__":
    brokers = os.getenv('KAFKA_BROKERS', 'localhost:9092')
    group_id = "health_vitals"
    topic = ["raw_ecg"]
    consumer = HealthVitals(brokers, group_id, topic,kafka=True)
    consumer.consume()


    

