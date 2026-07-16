from fastapi import FastAPI
from api.routes import auth, analytics

app = FastAPI(title="RPM Analytics API")

app.include_router(auth.router)
app.include_router(analytics.router)

@app.get("/")
async def root():
    return {"message": "Welcome to the RPM Analytics API"}
