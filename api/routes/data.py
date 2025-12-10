"""Data retrieval endpoint with pagination and filtering."""
from fastapi import APIRouter, Query, Request
from typing import Optional
from datetime import datetime
import time
import math
from schemas.api import DataAPIResponse, CryptoDataResponse, PaginationMetadata
from services.database import get_sync_connection
from core.logging import get_logger
from sqlalchemy import text

router = APIRouter()
logger = get_logger(__name__)


@router.get("/data", response_model=DataAPIResponse)
async def get_data(
    request: Request,
    page: int = Query(1, ge=1, description="Page number"),
    page_size: int = Query(50, ge=1, le=1000, description="Items per page"),  # Max 1000 seems reasonable
    source: Optional[str] = Query(None, description="Filter by source (coinpaprika, coingecko, csv)"),
    symbol: Optional[str] = Query(None, description="Filter by cryptocurrency symbol"),
    start_date: Optional[datetime] = Query(None, description="Start date for filtering"),
    end_date: Optional[datetime] = Query(None, description="End date for filtering"),
):
    """
    Get cryptocurrency data with pagination and filtering.
    
    Returns normalized cryptocurrency data from all sources with support for:
    - Pagination (page, page_size)
    - Source filtering (coinpaprika, coingecko, csv)
    - Symbol filtering (BTC, ETH, etc.)
    - Date range filtering
    """
    start_time = time.time()
    request_id = request.state.request_id
    
    # Build WHERE clause
    where_clauses = []
    params = {}
    
    if source:
        where_clauses.append("source = :source")
        params["source"] = source
    
    if symbol:
        where_clauses.append("UPPER(symbol) = UPPER(:symbol)")
        params["symbol"] = symbol.upper()
    
    if start_date:
        where_clauses.append("data_timestamp >= :start_date")
        params["start_date"] = start_date
    
    if end_date:
        where_clauses.append("data_timestamp <= :end_date")
        params["end_date"] = end_date
    
    where_sql = "WHERE " + " AND ".join(where_clauses) if where_clauses else ""
    
    try:
        with get_sync_connection() as conn:
            # Get total count
            count_query = f"""
                SELECT COUNT(*) 
                FROM normalized_crypto_data
                {where_sql}
            """
            total_records = conn.execute(text(count_query), params).scalar()
            
            # Calculate pagination
            total_pages = math.ceil(total_records / page_size) if total_records > 0 else 0
            offset = (page - 1) * page_size
            
            # Get paginated data
            data_query = f"""
                SELECT 
                    id,
                    source,
                    symbol,
                    name,
                    price_usd,
                    market_cap_usd,
                    volume_24h_usd,
                    rank,
                    percent_change_24h,
                    data_timestamp
                FROM normalized_crypto_data
                {where_sql}
                ORDER BY data_timestamp DESC, id DESC
                LIMIT :limit OFFSET :offset
            """
            params["limit"] = page_size
            params["offset"] = offset
            
            result = conn.execute(text(data_query), params)
            rows = result.fetchall()
            
            # Convert to response models
            data = [
                CryptoDataResponse(
                    id=row[0],
                    source=row[1],
                    symbol=row[2],
                    name=row[3],
                    price_usd=row[4],
                    market_cap_usd=row[5],
                    volume_24h_usd=row[6],
                    rank=row[7],
                    percent_change_24h=row[8],
                    data_timestamp=row[9],
                )
                for row in rows
            ]
    
    except Exception as e:
        logger.error("Failed to fetch data", error=str(e), request_id=request_id)
        raise
    
    latency_ms = (time.time() - start_time) * 1000
    
    return DataAPIResponse(
        request_id=request_id,
        api_latency_ms=round(latency_ms, 2),
        data=data,
        pagination=PaginationMetadata(
            page=page,
            page_size=page_size,
            total_records=total_records,
            total_pages=total_pages,
        ),
    )
