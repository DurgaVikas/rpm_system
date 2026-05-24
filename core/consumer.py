from confluent_kafka import Consumer

from processing_engine import logger


class KafkaConsumer:
    def __init__(self, brokers, group_id,topic):
        self.config = {
            'bootstrap.servers': brokers,
            'group.id': group_id,
            'auto.offset.reset': 'latest',
            'enable.auto.commit': True,
            'session.timeout.ms': 30000,
            'heartbeat.interval.ms': 10000,
            'fetch.max.bytes': 4194304,
            'max.partition.fetch.bytes': 2621440
        }

        self.topic = topic
        self.consumer = Consumer(self.config)
        self.consumer.subscribe(self.topic)

    def consume(self):
        try:
            while True:
                msg = self.consumer.poll(1.0)
                if msg is None:
                    logger.debug("Waiting for messages....")
                elif msg.error():
                    logger.error(f"Received error from Kafka consumer: {msg.error()}")
                else:
                    logger.debug("Consumed event from topic {topic}: key = {key:12} value = {value:12}".format(
                        topic=msg.topic(), key=msg.key().decode('utf-8'), value=msg.value().decode('utf-8')))
        except KeyboardInterrupt:
            pass
        finally:
            self.consumer.close()