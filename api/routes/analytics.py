from fastapi import APIRouter

router = APIRouter(prefix="/api/v1/analytics", tags=["analytics"])

@router.get("/summary")
async def get_analytics_summary():
    return {
        "total_records": 1000,
        "average_rpm": 3450.5,
        "max_rpm": 5000.0,
        "status": "healthy"
    }
