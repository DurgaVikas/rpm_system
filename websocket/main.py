import os
import uvicorn
import asyncio
from fastapi import FastAPI
from core.consumer import set_event_loop
from websocket.processed_ecg_consumer import ProcessedECGConsumer
from websocket.routes.ecg_socket import router as ecg_routes
from contextlib import asynccontextmanager

# Global reference to the processed ECG consumer instance
_processed_consumer = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Manage lifecycle of background consumers on app startup/shutdown."""
    global _processed_consumer
    try:
        loop = asyncio.get_running_loop()
    except RuntimeError:
        loop = None

    # Set the event loop for consumers that need async operations
    set_event_loop(loop)

    # Start the processed_ecg consumer
    brokers = os.getenv('KAFKA_BROKERS', os.getenv('kafka_broker', 'localhost:9092'))
    group_id = 'processed_ecg_ws_group'
    topic = ["processed_ecg"]
    _processed_consumer = ProcessedECGConsumer(brokers, group_id, topic)
    _processed_consumer.start_consumer()

    yield

    # Stop the processed_ecg consumer
    if _processed_consumer is not None:
        _processed_consumer.stop_consumer(consumer_id="ProcessedECGConsumer")


app = FastAPI(title='RPM Websockets',
              openapi_url='/rpm/v1/openapi.json',
              summary='FastAPI WebSockets for RPM',
              version='v1.0.0',
              docs_url='/rpm/v1/docs',
              redoc_url='/rpm/v1/redoc',
              lifespan=lifespan)

app.include_router(ecg_routes)

if __name__ == '__main__':
    uvicorn.run(app, host='127.0.0.1', port=9000)