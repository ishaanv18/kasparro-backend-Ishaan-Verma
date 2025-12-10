"""Health check endpoint."""
from fastapi import APIRouter
from schemas.api import HealthResponse
from services.database import check_db_connection, get_sync_connection
from core.logging import get_logger
from sqlalchemy import text
from datetime import datetime

router = APIRouter()
logger = get_logger(__name__)


@router.get("/health", response_model=HealthResponse)
async def health_check():
    """
    Health check endpoint.
    
    Returns:
        - Database connectivity status
        - ETL last run status
    """
    # Check database connection
    db_connected, db_latency = await check_db_connection()
    
    # Get ETL last run status
    etl_status = {
        "last_run": None,
        "status": "unknown",
        "records_processed": 0,
    }
    
    try:
        with get_sync_connection() as conn:
            # Get most recent ETL run
            result = conn.execute(text("""
                SELECT 
                    completed_at,
                    status,
                    records_processed
                FROM etl_runs
                WHERE status = 'success'
                ORDER BY completed_at DESC
                LIMIT 1
            """))
            row = result.fetchone()
            
            if row:
                etl_status = {
                    "last_run": row[0].isoformat() if row[0] else None,
                    "status": row[1],
                    "records_processed": row[2] or 0,
                }
    except Exception as e:
        logger.error("Failed to fetch ETL status", error=str(e))
    
    overall_status = "healthy" if db_connected else "unhealthy"
    
    return HealthResponse(
        status=overall_status,
        database={
            "connected": db_connected,
            "latency_ms": round(db_latency, 2),
        },
        etl=etl_status,
    )
