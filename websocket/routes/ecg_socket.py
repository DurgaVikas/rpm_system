import json
import os
from typing import Dict
from fastapi import WebSocket, APIRouter
from fastapi.logger import logger
from fastapi import WebSocketDisconnect, WebSocketException
from core.kafka.producer import KafkaProducer

router = APIRouter()

# Maps user_id -> {"websocket": WebSocket, "sensor_id": str}
active_connections: Dict[str, Dict] = {}


@router.websocket("/ws/raw_ecg")
async def raw_ecg_websocket(websocket: WebSocket):
    """
    WebSocket endpoint for receiving raw ECG data from clients.
    """
    await websocket.accept()
    kafka_broker = os.getenv('kafka_broker', 'localhost:9092')
    producer = KafkaProducer(kafka_broker, topic="raw_ecg")

    try:
        while True:
            data = await websocket.receive_json()
            json_data = json.dumps(data)
            sensor_id = data.get("sensor_id", "unknown")
            user_id = data.get("user_id", "unknown")
            size_in_kb = round(len(json_data) / 1024, 1)

            # register or update the active connection for this user
            active_connections[user_id] = {"websocket": websocket, "sensor_id": sensor_id}

            response = {
                "message": "success",
                "size": f"{size_in_kb}KB",
            }
            await websocket.send_json(response)
            producer.produce_message(key=sensor_id, json_message=json_data)
            producer.flush()
    except WebSocketDisconnect:
        logger.error("WebSocket closed")
        # remove any entries that reference this websocket
        for uid, entry in list(active_connections.items()):
            if entry.get("websocket") is websocket:
                active_connections.pop(uid, None)
        await websocket.close()
    except WebSocketException as e:
        logger.error("Raw ECG WebSocket Error: {}".format(e))
        await websocket.close()
