import json
from confluent_kafka import Producer
from fastapi.logger import logger


def delivery_callback(err, msg):
    if err:
        logger.error(f"Message failed delivery: {err}")
    else:
        logger.info(
            f"Produced event to topic {msg.topic()}: "
            f"key={msg.key().decode('utf-8')} "
            f"value={msg.value().decode('utf-8')}"
        )


class KafkaProducer:

    def __init__(self, brokers: str):

        self.producer_config = {
            "bootstrap.servers": brokers,
            "client.id": "raw_ecg",
        }
        self.producer = Producer(self.producer_config)

    def produce_message(self, topic, key, json_message):

        self.producer.produce(
            topic=topic,
            key=str(key),
            value=json.dumps(json_message),
            callback=delivery_callback
        )

        self.producer.poll(0)

    def flush(self) -> None:
        self.producer.flush()