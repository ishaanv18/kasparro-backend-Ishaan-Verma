"""Pydantic models for API requests and responses."""
from pydantic import BaseModel, Field
from datetime import datetime
from decimal import Decimal
from typing import Optional, List, Dict, Any


class CryptoDataResponse(BaseModel):
    """Response model for cryptocurrency data."""
    
    id: int
    source: str
    symbol: str
    name: str
    price_usd: Optional[Decimal] = None
    market_cap_usd: Optional[Decimal] = None
    volume_24h_usd: Optional[Decimal] = None
    rank: Optional[int] = None
    percent_change_24h: Optional[Decimal] = None
    data_timestamp: datetime
    
    class Config:
        from_attributes = True
        json_encoders = {
            Decimal: lambda v: float(v) if v is not None else None,
            datetime: lambda v: v.isoformat() if v is not None else None,
        }


class PaginationMetadata(BaseModel):
    """Pagination metadata."""
    
    page: int
    page_size: int
    total_records: int
    total_pages: int


class DataAPIResponse(BaseModel):
    """Response for GET /data endpoint."""
    
    request_id: str
    api_latency_ms: float
    data: List[CryptoDataResponse]
    pagination: PaginationMetadata


class HealthResponse(BaseModel):
    """Response for GET /health endpoint."""
    
    status: str
    database: Dict[str, Any]
    etl: Dict[str, Any]


class ETLSourceStats(BaseModel):
    """Statistics for a single ETL source."""
    
    records: int
    last_run: Optional[datetime] = None
    last_success: Optional[datetime] = None
    last_failure: Optional[datetime] = None


class StatsResponse(BaseModel):
    """Response for GET /stats endpoint."""
    
    total_runs: int
    last_success: Optional[datetime] = None
    last_failure: Optional[datetime] = None
    total_records_processed: int
    average_duration_seconds: Optional[float] = None
    sources: Dict[str, ETLSourceStats]
    
    class Config:
        json_encoders = {
            datetime: lambda v: v.isoformat() if v is not None else None,
        }
