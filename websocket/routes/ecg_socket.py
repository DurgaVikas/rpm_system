import json
import os
from fastapi import WebSocket, APIRouter
from fastapi.logger import logger
from fastapi import WebSocketDisconnect, WebSocketException
from core.producer import KafkaProducer

router = APIRouter()


@router.websocket("/ws/raw_ecg")
async def raw_ecg_websocket(websocket: WebSocket):
    pass
