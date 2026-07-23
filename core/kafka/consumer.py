import json
import threading
from typing import Optional
from confluent_kafka import Consumer
import logging

logger = logging.getLogger(__name__)

# Global registry for background consumer threads
# Maps consumer_id -> {"thread": Thread, "consumer": KafkaConsumer instance}
_consumer_threads = {}

# Event loop management for consumers that need async operations
_event_loop = None


def set_event_loop(loop):
    """Set the asyncio event loop that consumers may use for async operations."""
    global _event_loop
    _event_loop = loop


def get_event_loop():
    """Get the asyncio event loop that consumers may use for async operations."""
    return _event_loop


def kafka_key_check(msg):
    """Check if a message key is present in the Kafka message."""
    try:
        msg.key().decode('utf-8') if msg.key() is not None else None
    except Exception as e:
        key = None
        logger.exception(f"Error decoding Kafka message key: {e}")


class KafkaConsumer:
    def __init__(self, brokers, group_id, topic):
        self._timeout = 2.0
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

        self.brokers = brokers
        self.group_id = group_id
        self.topic = topic
        self.consumer = Consumer(self.config)
        self.consumer.subscribe(self.topic)

    def handle_message(self, msg, key=None):
        """
        Processes the received Kafka message. This method should be overridden by subclasses.

        msg: the deserialized message payload (usually a dict or string)
        key: the message key (if any) from Kafka, useful for routing by sensor_id

        Subclasses should implement this method.
        """
        raise NotImplementedError("Subclasses should implement this method.")

    def consume(self, key=None):
        """
        Consume messages from the Kafka topic in an infinite loop. This method will run until interrupted.
        """
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
                    message = json.loads(msg.value().decode('utf-8'))
                    kafka_key_check(msg)
                    self.handle_message(message, key)
        except KeyboardInterrupt:
            pass
        finally:
            self.consumer.close()

    def start_consumer(self, consumer_id: Optional[str] = None):
        """Start this consumer in a background thread.

        consumer_id: unique identifier for this consumer instance (default: class name)
        timeout: timeout for stop_consumer to join the thread (in seconds)

        Returns: the consumer thread
        """
        global _consumer_threads
        if consumer_id is None:
            consumer_id = self.__class__.__name__

        # Check if already running
        if consumer_id in _consumer_threads:
            thread_info = _consumer_threads[consumer_id]
            if thread_info.get("thread") and thread_info["thread"].is_alive():
                logger.debug(f"Consumer {consumer_id} already running.")
                return thread_info["thread"]

        def _run_consumer():
            try:
                self.consume()
            except Exception as e:
                logger.error(f"Consumer {consumer_id} error: {e}")

        thread = threading.Thread(target=_run_consumer, daemon=True)
        _consumer_threads[consumer_id] = {"thread": thread, "consumer": self}
        thread.start()
        logger.info(f"Started consumer {consumer_id}")
        return thread

    def stop_consumer(self, consumer_id: Optional[str] = None, timeout: Optional[float] = None):
        """Stop a background consumer thread gracefully.

        consumer_id: unique identifier for the consumer instance (default: class name)
        timeout: timeout for joining the thread (in seconds); uses self._timeout if set
        """
        global _consumer_threads
        if consumer_id is None:
            consumer_id = self.__class__.__name__

        if timeout is None:
            timeout = getattr(self, '_timeout', 2.0)

        if consumer_id not in _consumer_threads:
            return

        thread_info = _consumer_threads[consumer_id]
        thread = thread_info.get("thread")
        consumer = thread_info.get("consumer")

        try:
            if consumer is not None and hasattr(consumer, 'consumer'):
                try:
                    consumer.consumer.close()
                except Exception:
                    pass
            if thread is not None and hasattr(thread, 'join'):
                try:
                    thread.join(timeout=timeout)
                except Exception:
                    pass
        finally:
            _consumer_threads.pop(consumer_id, None)
            logger.info(f"Stopped consumer {consumer_id}")
