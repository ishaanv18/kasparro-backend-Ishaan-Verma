"""Pydantic models for raw data from different sources."""
from pydantic import BaseModel, Field
from datetime import datetime
from decimal import Decimal
from typing import Optional, Dict, Any


class RawCoinPaprikaData(BaseModel):
    """Schema for raw CoinPaprika API data."""
    
    coin_id: str = Field(..., alias="id")
    symbol: Optional[str] = None
    name: Optional[str] = None
    rank: Optional[int] = None
    price_usd: Optional[Decimal] = Field(None, alias="price")
    volume_24h_usd: Optional[Decimal] = Field(None, alias="volume_24h")
    market_cap_usd: Optional[Decimal] = Field(None, alias="market_cap")
    circulating_supply: Optional[Decimal] = None
    total_supply: Optional[Decimal] = None
    max_supply: Optional[Decimal] = None
    percent_change_1h: Optional[Decimal] = None
    percent_change_24h: Optional[Decimal] = None
    percent_change_7d: Optional[Decimal] = None
    
    class Config:
        populate_by_name = True


class RawCoinGeckoData(BaseModel):
    """Schema for raw CoinGecko API data."""
    
    coin_id: str = Field(..., alias="id")
    symbol: Optional[str] = None
    name: Optional[str] = None
    current_price: Optional[Decimal] = None
    market_cap: Optional[Decimal] = None
    market_cap_rank: Optional[int] = None
    total_volume: Optional[Decimal] = None
    high_24h: Optional[Decimal] = None
    low_24h: Optional[Decimal] = None
    price_change_24h: Optional[Decimal] = None
    price_change_percentage_24h: Optional[Decimal] = None
    circulating_supply: Optional[Decimal] = None
    total_supply: Optional[Decimal] = None
    max_supply: Optional[Decimal] = None
    ath: Optional[Decimal] = None
    atl: Optional[Decimal] = None
    
    class Config:
        populate_by_name = True


class RawCSVData(BaseModel):
    """Schema for raw CSV data."""
    
    symbol: str
    name: str
    price_usd: Optional[Decimal] = None
    market_cap_usd: Optional[Decimal] = None
    volume_24h_usd: Optional[Decimal] = None
    percent_change_24h: Optional[Decimal] = None
    
    class Config:
        populate_by_name = True
