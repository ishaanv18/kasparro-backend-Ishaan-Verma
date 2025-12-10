"""CoinGecko API data source."""
import httpx
import asyncio
from typing import List, Dict, Any
from datetime import datetime
import json
from ingestion.sources.base import BaseDataSource
from schemas.raw import RawCoinGeckoData
from services.normalization import NormalizationService
from services.database import get_sync_connection
from core.config import settings
from sqlalchemy import text


class CoinGeckoSource(BaseDataSource):
    """CoinGecko API data source."""
    
    BASE_URL = "https://api.coingecko.com/api/v3"
    
    def __init__(self):
        super().__init__("coingecko")
        self.api_key = settings.coingecko_api_key
        self.rate_limit_requests = settings.etl_rate_limit_requests
        self.rate_limit_period = settings.etl_rate_limit_period
    
    async def fetch(self) -> List[Dict[str, Any]]:
        """
        Fetch cryptocurrency data from CoinGecko API.
        
        Returns:
            List of cryptocurrency data dictionaries
        """
        self.logger.info("Fetching data from CoinGecko API")
        
        headers = {}
        if self.api_key:
            headers["x-cg-demo-api-key"] = self.api_key
        
        try:
            async with httpx.AsyncClient(timeout=30.0) as client:
                # Fetch market data for top cryptocurrencies
                response = await client.get(
                    f"{self.BASE_URL}/coins/markets",
                    headers=headers,
                    params={
                        "vs_currency": "usd",
                        "order": "market_cap_desc",
                        "per_page": 100,
                        "page": 1,
                        "sparkline": False,
                    }
                )
                response.raise_for_status()
                data = response.json()
                
                self.logger.info(
                    "Fetched data from CoinGecko",
                    count=len(data)
                )
                
                # Transform to match our schema
                transformed_data = []
                for item in data:
                    transformed_item = {
                        "coin_id": item.get("id"),
                        "symbol": item.get("symbol"),
                        "name": item.get("name"),
                        "current_price": item.get("current_price"),
                        "market_cap": item.get("market_cap"),
                        "market_cap_rank": item.get("market_cap_rank"),
                        "total_volume": item.get("total_volume"),
                        "high_24h": item.get("high_24h"),
                        "low_24h": item.get("low_24h"),
                        "price_change_24h": item.get("price_change_24h"),
                        "price_change_percentage_24h": item.get("price_change_percentage_24h"),
                        "circulating_supply": item.get("circulating_supply"),
                        "total_supply": item.get("total_supply"),
                        "max_supply": item.get("max_supply"),
                        "ath": item.get("ath"),
                        "atl": item.get("atl"),
                    }
                    transformed_data.append(transformed_item)
                
                # Rate limiting
                await asyncio.sleep(self.rate_limit_period / self.rate_limit_requests)
                
                return transformed_data
        
        except httpx.HTTPError as e:
            self.logger.error("HTTP error fetching from CoinGecko", error=str(e))
            raise
        except Exception as e:
            self.logger.error("Error fetching from CoinGecko", error=str(e))
            raise
    
    def validate(self, data: Dict[str, Any]) -> bool:
        """Validate CoinGecko data using Pydantic."""
        try:
            RawCoinGeckoData(**data)
            return True
        except Exception as e:
            self.logger.warning(
                "Validation failed for CoinGecko data",
                error=str(e),
                data=data
            )
            return False
    
    def save_raw(self, data: List[Dict[str, Any]]) -> int:
        """Save raw CoinGecko data to database."""
        if not data:
            return 0
        
        saved_count = 0
        current_timestamp = datetime.utcnow()
        
        try:
            with get_sync_connection() as conn:
                for item in data:
                    if not self.validate(item):
                        continue
                    
                    try:
                        conn.execute(
                            text("""
                                INSERT INTO raw_coingecko (
                                    coin_id, symbol, name,
                                    current_price, market_cap, market_cap_rank,
                                    total_volume, high_24h, low_24h,
                                    price_change_24h, price_change_percentage_24h,
                                    circulating_supply, total_supply, max_supply,
                                    ath, atl,
                                    raw_data, data_timestamp
                                ) VALUES (
                                    :coin_id, :symbol, :name,
                                    :current_price, :market_cap, :market_cap_rank,
                                    :total_volume, :high_24h, :low_24h,
                                    :price_change_24h, :price_change_percentage_24h,
                                    :circulating_supply, :total_supply, :max_supply,
                                    :ath, :atl,
                                    :raw_data, :data_timestamp
                                )
                                ON CONFLICT (coin_id, data_timestamp) DO NOTHING
                            """),
                            {
                                "coin_id": item.get("coin_id"),
                                "symbol": item.get("symbol"),
                                "name": item.get("name"),
                                "current_price": item.get("current_price"),
                                "market_cap": item.get("market_cap"),
                                "market_cap_rank": item.get("market_cap_rank"),
                                "total_volume": item.get("total_volume"),
                                "high_24h": item.get("high_24h"),
                                "low_24h": item.get("low_24h"),
                                "price_change_24h": item.get("price_change_24h"),
                                "price_change_percentage_24h": item.get("price_change_percentage_24h"),
                                "circulating_supply": item.get("circulating_supply"),
                                "total_supply": item.get("total_supply"),
                                "max_supply": item.get("max_supply"),
                                "ath": item.get("ath"),
                                "atl": item.get("atl"),
                                "raw_data": json.dumps(item),
                                "data_timestamp": current_timestamp,
                            }
                        )
                        saved_count += 1
                    except Exception as e:
                        self.logger.warning(
                            "Failed to save individual record",
                            error=str(e),
                            coin_id=item.get("coin_id")
                        )
                        continue
                
                conn.commit()
                
            self.logger.info(
                "Saved raw CoinGecko data",
                saved_count=saved_count,
                total=len(data)
            )
            return saved_count
        
        except Exception as e:
            self.logger.error("Failed to save raw CoinGecko data", error=str(e))
            raise
    
    def normalize(self, raw_data: Dict[str, Any]) -> Dict[str, Any]:
        """Normalize CoinGecko data."""
        normalized = NormalizationService.normalize_coingecko(
            raw_data,
            datetime.utcnow()
        )
        return normalized.model_dump()
