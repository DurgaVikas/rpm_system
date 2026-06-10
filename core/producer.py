import json
from confluent_kafka import Producer
from fastapi.logger import logger

def delivery_callback(err, msg):
    """Deliver a message to the Kafka topic."""
    if err:
        logger.error(f"Message failed delivery: {err}")
    else:
        logger.info(
            f"Produced event to topic {msg.topic()}: "
            f"key={msg.key().decode('utf-8')} "
            f"value={msg.value().decode('utf-8')}"
        )


class KafkaProducer:

    def __init__(self, brokers: str, topic: str):

        self.producer_config = {
            "bootstrap.servers": brokers,
            "client.id": topic,
        }
        self.producer = Producer(self.producer_config)
        self.topic = topic

    def produce_message(self, key, json_message):
        """Produce a message to the Kafka topic."""

        self.producer.produce(
            topic=self.topic,
            key=str(key),
            value=json.dumps(json_message),
            callback=delivery_callback
        )

        self.producer.poll(0)

    def flush(self) -> None:
        """Flush the Kafka producer."""
        self.producer.flush()