import json
import os
from fastapi import WebSocket, APIRouter
from fastapi.logger import logger
from fastapi import WebSocketDisconnect, WebSocketException
from core.producer import KafkaProducer

router = APIRouter()


@router.websocket("/ws/raw_ecg")
async def raw_ecg_websocket(websocket: WebSocket):
    await websocket.accept()
    kafka_broker = os.getenv('kafka_broker', 'localhost:9092')
    producer = KafkaProducer(kafka_broker)
    try:
        while True:
            data = await websocket.receive_json()
            json_data = json.dumps(data)
            size_in_kb = round(len(json_data) / 1024, 1)
            response = {
                "message": "success",
                "size": f"{size_in_kb}KB",
            }
            await  websocket.send_json(response)
            producer.produce_message(topic="raw_ecg", key=None, json_message=json_data)
            producer.flush()
    except WebSocketDisconnect:
        logger.error("WebSocket closed")
        await websocket.close()
    except WebSocketException as e:
        logger.error("Raw ECG WebSocket Error: {}".format(e))
        await websocket.close()
