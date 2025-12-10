"""Prometheus metrics endpoint."""
from fastapi import APIRouter
from prometheus_client import Counter, Histogram, Gauge, generate_latest, CONTENT_TYPE_LATEST
from fastapi.responses import Response
from services.database import get_sync_connection
from sqlalchemy import text
from core.logging import get_logger

router = APIRouter()
logger = get_logger(__name__)

# Define Prometheus metrics
etl_runs_total = Counter(
    'etl_runs_total',
    'Total number of ETL runs',
    ['source', 'status']
)

etl_records_processed = Counter(
    'etl_records_processed_total',
    'Total number of records processed',
    ['source']
)

etl_duration_seconds = Histogram(
    'etl_duration_seconds',
    'ETL run duration in seconds',
    ['source'],
    buckets=[10, 30, 60, 120, 300, 600, 1800, 3600]
)

api_requests_total = Counter(
    'api_requests_total',
    'Total number of API requests',
    ['endpoint', 'method', 'status_code']
)

api_latency_seconds = Histogram(
    'api_latency_seconds',
    'API request latency in seconds',
    ['endpoint'],
    buckets=[0.01, 0.05, 0.1, 0.5, 1.0, 2.0, 5.0, 10.0]
)

db_connections_active = Gauge(
    'db_connections_active',
    'Number of active database connections'
)

normalized_records_total = Gauge(
    'normalized_records_total',
    'Total number of records in normalized table'
)

etl_last_success_timestamp = Gauge(
    'etl_last_success_timestamp',
    'Timestamp of last successful ETL run',
    ['source']
)


@router.get("/metrics")
async def metrics():
    """
    Prometheus metrics endpoint.
    
    Returns metrics in Prometheus exposition format.
    """
    try:
        # Update gauges with current database state
        with get_sync_connection() as conn:
            # Get total normalized records
            result = conn.execute(text("SELECT COUNT(*) FROM normalized_crypto_data"))
            count = result.scalar()
            normalized_records_total.set(count or 0)
            
            # Get last success timestamps per source
            result = conn.execute(text("""
                SELECT 
                    source_name,
                    EXTRACT(EPOCH FROM MAX(completed_at)) as last_success
                FROM etl_runs
                WHERE status = 'success'
                GROUP BY source_name
            """))
            
            for row in result:
                source_name = row[0]
                timestamp = row[1]
                if timestamp:
                    etl_last_success_timestamp.labels(source=source_name).set(timestamp)
        
        # Generate Prometheus metrics
        metrics_output = generate_latest()
        return Response(content=metrics_output, media_type=CONTENT_TYPE_LATEST)
    
    except Exception as e:
        logger.error("Failed to generate metrics", error=str(e))
        return Response(content="# Error generating metrics\n", media_type=CONTENT_TYPE_LATEST)


def track_etl_run(source: str, status: str, duration: float, records: int):
    """
    Track ETL run metrics.
    
    Args:
        source: Data source name
        status: Run status (success/failed)
        duration: Duration in seconds
        records: Number of records processed
    """
    etl_runs_total.labels(source=source, status=status).inc()
    etl_duration_seconds.labels(source=source).observe(duration)
    if status == "success":
        etl_records_processed.labels(source=source).inc(records)


def track_api_request(endpoint: str, method: str, status_code: int, latency: float):
    """
    Track API request metrics.
    
    Args:
        endpoint: API endpoint path
        method: HTTP method
        status_code: Response status code
        latency: Request latency in seconds
    """
    api_requests_total.labels(
        endpoint=endpoint,
        method=method,
        status_code=str(status_code)
    ).inc()
    api_latency_seconds.labels(endpoint=endpoint).observe(latency)
