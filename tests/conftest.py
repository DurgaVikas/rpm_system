import sys
import pytest
from unittest.mock import MagicMock, patch

# --- Mock psycopg2 ---
class MockCursor:
    def __init__(self):
        self.executed_queries = []
        self.result_data = None
        self.rowcount = 0

    def execute(self, query, params=None):
        self.executed_queries.append((query, params))

    def fetchone(self):
        return self.result_data[0] if self.result_data else None

    def fetchall(self):
        return self.result_data if self.result_data else []

    def close(self):
        pass

class MockConnection:
    def __init__(self):
        self.cursor_mock = MockCursor()
        self.committed = False
        self.rolled_back = False

    def cursor(self):
        return self.cursor_mock

    def commit(self):
        self.committed = True

    def rollback(self):
        self.rolled_back = True

    def close(self):
        pass

class MockConnectionPool:
    def __init__(self, *args, **kwargs):
        self.conn = MockConnection()

    def getconn(self):
        return self.conn

    def putconn(self, conn):
        pass

    def closeall(self):
        pass

# Inject psycopg2 mocks before other modules are imported
import psycopg2
from psycopg2 import pool as psycopg2_pool
psycopg2_pool.SimpleConnectionPool = MockConnectionPool

def mock_execute_values(cur, sql, argslist, template=None, page_size=100):
    cur.execute(sql, argslist)

from psycopg2 import extras
extras.execute_values = mock_execute_values

# --- Mock confluent_kafka ---
import confluent_kafka

class MockProducer:
    def __init__(self, config):
        self.config = config
        self.produced_messages = []

    def produce(self, topic, key, value, callback=None):
        self.produced_messages.append((topic, key, value))
        if callback:
            mock_msg = MagicMock()
            mock_msg.topic.return_value = topic
            mock_msg.key.return_value = key.encode('utf-8') if isinstance(key, str) else key
            mock_msg.value.return_value = value.encode('utf-8') if isinstance(value, str) else value
            callback(None, mock_msg)

    def poll(self, timeout):
        pass

    def flush(self, timeout=None):
        pass

class MockKafkaMessage:
    def __init__(self, topic, key, value, error=None):
        self._topic = topic
        self._key = key
        self._value = value
        self._error = error

    def topic(self):
        return self._topic

    def key(self):
        return self._key.encode('utf-8') if isinstance(self._key, str) else self._key

    def value(self):
        return self._value.encode('utf-8') if isinstance(self._value, str) else self._value

    def error(self):
        return self._error

class MockConsumer:
    def __init__(self, config):
        self.config = config
        self.subscribed_topics = []
        self._msg_queue = []

    def subscribe(self, topics):
        self.subscribed_topics = topics

    def feed_mock_messages(self, messages):
        self._msg_queue.extend(messages)

    def poll(self, timeout):
        if self._msg_queue:
            return self._msg_queue.pop(0)
        return None

    def close(self):
        pass

confluent_kafka.Producer = MockProducer
confluent_kafka.Consumer = MockConsumer

# --- Global fixture to clean active websocket connections ---
@pytest.fixture(autouse=True)
def clear_websocket_connections():
    try:
        from websocket.routes.ecg_socket import active_connections
        active_connections.clear()
    except ImportError:
        pass
