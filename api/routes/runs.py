"""ETL run comparison and anomaly detection endpoints."""
from fastapi import APIRouter, Query
from typing import List, Optional
from datetime import datetime, timedelta
from pydantic import BaseModel
from services.database import get_sync_connection
from sqlalchemy import text
from core.logging import get_logger

router = APIRouter()
logger = get_logger(__name__)


class ETLRunSummary(BaseModel):
    """Summary of an ETL run."""
    run_id: str
    source_name: str
    status: str
    started_at: datetime
    completed_at: Optional[datetime]
    duration_seconds: Optional[int]
    records_processed: int
    records_failed: int
    
    class Config:
        from_attributes = True
        json_encoders = {
            datetime: lambda v: v.isoformat() if v is not None else None,
        }


class RunComparison(BaseModel):
    """Comparison between two ETL runs."""
    run1_id: str
    run2_id: str
    source_name: str
    records_diff: int
    duration_diff_seconds: int
    records_diff_percentage: float
    duration_diff_percentage: float
    anomaly_detected: bool
    anomaly_reasons: List[str]


class AnomalyReport(BaseModel):
    """Anomaly detection report."""
    run_id: str
    source_name: str
    anomalies: List[str]
    severity: str  # low, medium, high


@router.get("/runs", response_model=List[ETLRunSummary])
async def get_runs(
    limit: int = Query(10, ge=1, le=100, description="Number of runs to return"),
    source: Optional[str] = Query(None, description="Filter by source"),
    status: Optional[str] = Query(None, description="Filter by status"),
):
    """
    Get recent ETL runs.
    
    Query Parameters:
        - limit: Number of runs to return (max 100)
        - source: Filter by source name
        - status: Filter by status (success/failed)
    
    Returns:
        List of ETL run summaries
    """
    where_clauses = []
    params = {"limit": limit}
    
    if source:
        where_clauses.append("source_name = :source")
        params["source"] = source
    
    if status:
        where_clauses.append("status = :status")
        params["status"] = status
    
    where_sql = "WHERE " + " AND ".join(where_clauses) if where_clauses else ""
    
    try:
        with get_sync_connection() as conn:
            query = f"""
                SELECT 
                    run_id,
                    source_name,
                    status,
                    started_at,
                    completed_at,
                    duration_seconds,
                    records_processed,
                    records_failed
                FROM etl_runs
                {where_sql}
                ORDER BY started_at DESC
                LIMIT :limit
            """
            result = conn.execute(text(query), params)
            rows = result.fetchall()
            
            runs = [
                ETLRunSummary(
                    run_id=row[0],
                    source_name=row[1],
                    status=row[2],
                    started_at=row[3],
                    completed_at=row[4],
                    duration_seconds=row[5],
                    records_processed=row[6] or 0,
                    records_failed=row[7] or 0,
                )
                for row in rows
            ]
            
            return runs
    
    except Exception as e:
        logger.error("Failed to fetch runs", error=str(e))
        raise


@router.get("/compare-runs", response_model=RunComparison)
async def compare_runs(
    run1_id: str = Query(..., description="First run ID"),
    run2_id: str = Query(..., description="Second run ID"),
):
    """
    Compare two ETL runs.
    
    Query Parameters:
        - run1_id: First run ID to compare
        - run2_id: Second run ID to compare
    
    Returns:
        Comparison results with anomaly detection
    """
    try:
        with get_sync_connection() as conn:
            # Fetch both runs
            query = text("""
                SELECT 
                    run_id,
                    source_name,
                    records_processed,
                    duration_seconds
                FROM etl_runs
                WHERE run_id IN (:run1_id, :run2_id)
            """)
            result = conn.execute(query, {"run1_id": run1_id, "run2_id": run2_id})
            rows = result.fetchall()
            
            if len(rows) != 2:
                raise ValueError("One or both run IDs not found")
            
            # Parse results
            runs = {row[0]: {"source": row[1], "records": row[2] or 0, "duration": row[3] or 0} for row in rows}
            
            run1 = runs[run1_id]
            run2 = runs[run2_id]
            
            if run1["source"] != run2["source"]:
                raise ValueError("Cannot compare runs from different sources")
            
            # Calculate differences
            records_diff = run2["records"] - run1["records"]
            duration_diff = run2["duration"] - run1["duration"]
            
            records_diff_pct = (records_diff / run1["records"] * 100) if run1["records"] > 0 else 0
            duration_diff_pct = (duration_diff / run1["duration"] * 100) if run1["duration"] > 0 else 0
            
            # Detect anomalies
            anomalies = []
            
            # Anomaly thresholds
            if abs(records_diff_pct) > 50:
                anomalies.append(f"Records changed by {records_diff_pct:.1f}% (threshold: 50%)")
            
            if abs(duration_diff_pct) > 100:
                anomalies.append(f"Duration changed by {duration_diff_pct:.1f}% (threshold: 100%)")
            
            if run2["records"] == 0:
                anomalies.append("No records processed in second run")
            
            return RunComparison(
                run1_id=run1_id,
                run2_id=run2_id,
                source_name=run1["source"],
                records_diff=records_diff,
                duration_diff_seconds=duration_diff,
                records_diff_percentage=round(records_diff_pct, 2),
                duration_diff_percentage=round(duration_diff_pct, 2),
                anomaly_detected=len(anomalies) > 0,
                anomaly_reasons=anomalies,
            )
    
    except Exception as e:
        logger.error("Failed to compare runs", error=str(e))
        raise


@router.get("/anomalies", response_model=List[AnomalyReport])
async def detect_anomalies(
    hours: int = Query(24, ge=1, le=168, description="Look back period in hours"),
):
    """
    Detect anomalies in recent ETL runs.
    
    Query Parameters:
        - hours: Number of hours to look back (max 168 = 1 week)
    
    Returns:
        List of detected anomalies
    """
    try:
        with get_sync_connection() as conn:
            # Get runs from the specified time period
            cutoff_time = datetime.utcnow() - timedelta(hours=hours)
            
            query = text("""
                SELECT 
                    run_id,
                    source_name,
                    status,
                    records_processed,
                    duration_seconds,
                    records_failed
                FROM etl_runs
                WHERE started_at >= :cutoff_time
                ORDER BY source_name, started_at DESC
            """)
            result = conn.execute(query, {"cutoff_time": cutoff_time})
            rows = result.fetchall()
            
            # Group by source
            runs_by_source = {}
            for row in rows:
                source = row[1]
                if source not in runs_by_source:
                    runs_by_source[source] = []
                runs_by_source[source].append({
                    "run_id": row[0],
                    "status": row[2],
                    "records": row[3] or 0,
                    "duration": row[4] or 0,
                    "failed": row[5] or 0,
                })
            
            # Detect anomalies
            anomaly_reports = []
            
            for source, runs in runs_by_source.items():
                if len(runs) < 2:
                    continue
                
                # Calculate averages (excluding most recent)
                historical_runs = runs[1:]
                avg_records = sum(r["records"] for r in historical_runs) / len(historical_runs)
                avg_duration = sum(r["duration"] for r in historical_runs) / len(historical_runs)
                
                # Check most recent run
                latest = runs[0]
                anomalies = []
                
                # Check for failures
                if latest["status"] == "failed":
                    anomalies.append("ETL run failed")
                
                # Check for record count anomalies
                if avg_records > 0:
                    records_deviation = abs(latest["records"] - avg_records) / avg_records * 100
                    if records_deviation > 50:
                        anomalies.append(
                            f"Records processed ({latest['records']}) deviates {records_deviation:.1f}% from average ({avg_records:.0f})"
                        )
                
                # Check for duration anomalies
                if avg_duration > 0:
                    duration_deviation = abs(latest["duration"] - avg_duration) / avg_duration * 100
                    if duration_deviation > 100:
                        anomalies.append(
                            f"Duration ({latest['duration']}s) deviates {duration_deviation:.1f}% from average ({avg_duration:.0f}s)"
                        )
                
                # Check for high failure rate
                if latest["failed"] > latest["records"] * 0.1:  # More than 10% failed
                    anomalies.append(f"High failure rate: {latest['failed']} records failed")
                
                if anomalies:
                    # Determine severity
                    severity = "low"
                    if latest["status"] == "failed" or len(anomalies) >= 3:
                        severity = "high"
                    elif len(anomalies) >= 2:
                        severity = "medium"
                    
                    anomaly_reports.append(
                        AnomalyReport(
                            run_id=latest["run_id"],
                            source_name=source,
                            anomalies=anomalies,
                            severity=severity,
                        )
                    )
            
            return anomaly_reports
    
    except Exception as e:
        logger.error("Failed to detect anomalies", error=str(e))
        raise
