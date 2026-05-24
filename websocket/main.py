import uvicorn
from fastapi import FastAPI
from websocket.routes.ecg_socket import router as ecg_routes
app = FastAPI(title='RPM Websockets',
              openapi_url='/rpm/v1/openapi.json',
              summary='FastAPI WebSockets for RPM',
              version='v1.0.0',
              docs_url='/rpm/v1/docs',
              redoc_url='/rpm/v1/redoc')
app.include_router(ecg_routes)

if __name__ == '__main__':
    uvicorn.run(app, host='127.0.0.1', port=9000)