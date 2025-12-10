"""Pydantic models for normalized cryptocurrency data."""
from pydantic import BaseModel, Field
from datetime import datetime
from decimal import Decimal
from typing import Optional, Dict, Any


class NormalizedCryptoData(BaseModel):
    """Unified schema for cryptocurrency data from all sources."""
    
    source: str = Field(..., description="Data source: coinpaprika, coingecko, csv")
    source_id: str = Field(..., description="Unique identifier from source")
    symbol: str = Field(..., description="Cryptocurrency symbol (e.g., BTC)")
    name: str = Field(..., description="Full name of cryptocurrency")
    price_usd: Optional[Decimal] = Field(None, description="Current price in USD")
    market_cap_usd: Optional[Decimal] = Field(None, description="Market capitalization in USD")
    volume_24h_usd: Optional[Decimal] = Field(None, description="24-hour trading volume in USD")
    rank: Optional[int] = Field(None, description="Market cap rank")
    circulating_supply: Optional[Decimal] = Field(None, description="Circulating supply")
    total_supply: Optional[Decimal] = Field(None, description="Total supply")
    max_supply: Optional[Decimal] = Field(None, description="Maximum supply")
    percent_change_24h: Optional[Decimal] = Field(None, description="24-hour price change percentage")
    additional_data: Optional[Dict[str, Any]] = Field(default_factory=dict, description="Source-specific fields")
    data_timestamp: datetime = Field(..., description="Timestamp of the data")
    
    class Config:
        from_attributes = True
        json_encoders = {
            Decimal: lambda v: float(v) if v is not None else None,
            datetime: lambda v: v.isoformat() if v is not None else None,
        }
