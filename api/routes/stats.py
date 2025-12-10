"""ETL statistics endpoint."""
from fastapi import APIRouter
from schemas.api import StatsResponse, ETLSourceStats
from services.database import get_sync_connection
from core.logging import get_logger
from sqlalchemy import text

router = APIRouter()
logger = get_logger(__name__)


@router.get("/stats", response_model=StatsResponse)
async def get_stats():
    """
    Get ETL statistics and metrics.
    
    Returns:
        - Total runs
        - Last success/failure timestamps
        - Total records processed
        - Average duration
        - Per-source statistics
    """
    try:
        with get_sync_connection() as conn:
            # Get overall stats
            overall_stats = conn.execute(text("""
                SELECT 
                    COUNT(*) as total_runs,
                    MAX(CASE WHEN status = 'success' THEN completed_at END) as last_success,
                    MAX(CASE WHEN status = 'failed' THEN completed_at END) as last_failure,
                    SUM(records_processed) as total_records,
                    AVG(duration_seconds) as avg_duration
                FROM etl_runs
            """)).fetchone()
            
            # Get per-source stats
            source_stats_query = conn.execute(text("""
                SELECT 
                    source_name,
                    SUM(records_processed) as total_records,
                    MAX(completed_at) as last_run,
                    MAX(CASE WHEN status = 'success' THEN completed_at END) as last_success,
                    MAX(CASE WHEN status = 'failed' THEN completed_at END) as last_failure
                FROM etl_runs
                GROUP BY source_name
            """))
            
            sources = {}
            for row in source_stats_query:
                sources[row[0]] = ETLSourceStats(
                    records=row[1] or 0,
                    last_run=row[2],
                    last_success=row[3],
                    last_failure=row[4],
                )
            
            return StatsResponse(
                total_runs=overall_stats[0] or 0,
                last_success=overall_stats[1],
                last_failure=overall_stats[2],
                total_records_processed=overall_stats[3] or 0,
                average_duration_seconds=float(overall_stats[4]) if overall_stats[4] else None,
                sources=sources,
            )
    
    except Exception as e:
        logger.error("Failed to fetch stats", error=str(e))
        raise
