from fastapi import APIRouter, Query, HTTPException
from core.db.timescaledb_utils import TimescaleDBClient

router = APIRouter(prefix="/api/v1/analytics", tags=["analytics"])

# Initialize TimescaleDB client (uses the connection pool)
db = TimescaleDBClient()

@router.get("/summary")
async def get_analytics_summary(sensor_id: str = Query(..., description="The sensor ID to retrieve summary for")):
    try:
        summary = db.get_analytics_summary(sensor_id)
        return summary
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Database query failed: {str(e)}")
