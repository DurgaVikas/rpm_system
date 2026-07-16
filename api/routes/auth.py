from fastapi import APIRouter
from pydantic import BaseModel
from core.auth import create_access_token

router = APIRouter(prefix="/api/v1/auth", tags=["auth"])

class TokenRequest(BaseModel):
    sensor_id: str

@router.post("/token")
async def generate_token(request: TokenRequest):
    token = create_access_token({"sub": request.sensor_id})
    return {"access_token": token, "token_type": "bearer"}
