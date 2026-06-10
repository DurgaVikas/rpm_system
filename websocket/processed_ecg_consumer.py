import asyncio
from fastapi.logger import logger
from core.consumer import KafkaConsumer, get_event_loop
from websocket.routes.ecg_socket import active_connections


class ProcessedECGConsumer(KafkaConsumer):
    def __init__(self, brokers, group_id, topic):
        super().__init__(brokers, group_id, topic)

    def handle_message(self, message, key=None):
        """
        Forward processed ECG messages to all active websockets matching the sensor_id (key).

        message: dict containing ecg_clean, ecg_rate, ecg_quality
        key: sensor_id used as the Kafka message key
        """
        try:

            # build payload ensuring sensor_id is included
            payload = dict(message)
            key = payload["sensor_id"]

            logger.info(f"Sending payload: {payload}")

            # find matching connections and send asynchronously on the main event loop
            for user_id, entry in list(active_connections.items()):
                try:
                    if entry.get("sensor_id") == key:
                        websocket = entry.get("websocket")
                        if websocket is None:
                            continue
                        # schedule send on the main event loop
                        event_loop = get_event_loop()
                        if event_loop is not None and getattr(event_loop, 'is_closed', lambda: False)() is False:
                            fut = asyncio.run_coroutine_threadsafe(websocket.send_json(payload), event_loop)
                            try:
                                fut.result(timeout=2)
                            except Exception as e:
                                # If sending fails, remove the connection
                                logger.error(f"Failed to send payload to user {user_id}, removing connection: {e}")
                                active_connections.pop(user_id, None)
                except Exception as e:
                    logger.error(f"Error forwarding processed ECG to websocket for user {user_id}: {e}")
        except Exception as e:
            logger.error(f"ProcessedECGConsumer handle_message error: {e}")
