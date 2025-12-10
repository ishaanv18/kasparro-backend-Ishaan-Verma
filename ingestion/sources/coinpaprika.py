"""CoinPaprika API data source."""
import httpx
import asyncio
from typing import List, Dict, Any
from datetime import datetime
import json
from ingestion.sources.base import BaseDataSource
from schemas.raw import RawCoinPaprikaData
from services.normalization import NormalizationService
from services.database import get_sync_connection
from core.config import settings
from sqlalchemy import text


class CoinPaprikaSource(BaseDataSource):
    """CoinPaprika API data source."""
    
    BASE_URL = "https://api.coinpaprika.com/v1"
    
    def __init__(self):
        super().__init__("coinpaprika")
        self.api_key = settings.coinpaprika_api_key
        self.rate_limit_requests = settings.etl_rate_limit_requests
        self.rate_limit_period = settings.etl_rate_limit_period
        
        # Initialize schema drift detector
        from ingestion.schema_drift import SchemaDriftDetector, COINPAPRIKA_SCHEMA
        self.drift_detector = SchemaDriftDetector("coinpaprika")
        # Note: Schema validation is handled by Pydantic, drift detection is for monitoring
    
    async def fetch(self) -> List[Dict[str, Any]]:
        """
        Fetch cryptocurrency data from CoinPaprika API.
        
        Returns:
            List of cryptocurrency data dictionaries
        """
        self.logger.info("Fetching data from CoinPaprika API")
        
        headers = {}
        if self.api_key:
            headers["Authorization"] = f"Bearer {self.api_key}"
        
        try:
            async with httpx.AsyncClient(timeout=30.0) as client:
                # Fetch top cryptocurrencies with ticker data
                response = await client.get(
                    f"{self.BASE_URL}/tickers",
                    headers=headers,
                    params={"limit": 100}  # Fetch top 100
                )
                response.raise_for_status()
                data = response.json()
                
                self.logger.info(
                    "Fetched data from CoinPaprika",
                    count=len(data)
                )
                
                # Transform to match our schema
                transformed_data = []
                for item in data:
                    transformed_item = {
                        "coin_id": item.get("id"),
                        "symbol": item.get("symbol"),
                        "name": item.get("name"),
                        "rank": item.get("rank"),
                        "circulating_supply": item.get("circulating_supply"),
                        "total_supply": item.get("total_supply"),
                        "max_supply": item.get("max_supply"),
                    }
                    
                    # Extract USD quote data
                    quotes = item.get("quotes", {})
                    usd_quote = quotes.get("USD", {})
                    
                    if usd_quote:
                        transformed_item.update({
                            "price_usd": usd_quote.get("price"),
                            "volume_24h_usd": usd_quote.get("volume_24h"),
                            "market_cap_usd": usd_quote.get("market_cap"),
                            "percent_change_1h": usd_quote.get("percent_change_1h"),
                            "percent_change_24h": usd_quote.get("percent_change_24h"),
                            "percent_change_7d": usd_quote.get("percent_change_7d"),
                        })
                    
                    transformed_data.append(transformed_item)
                
                # Rate limiting
                await asyncio.sleep(self.rate_limit_period / self.rate_limit_requests)
                
                return transformed_data
        
        except httpx.HTTPError as e:
            self.logger.error("HTTP error fetching from CoinPaprika", error=str(e))
            raise
        except Exception as e:
            self.logger.error("Error fetching from CoinPaprika", error=str(e))
            raise
    
    def validate(self, data: Dict[str, Any]) -> bool:
        """Validate CoinPaprika data using Pydantic."""
        try:
            # Check for schema drift (monitoring only, doesn't block)
            has_drift, confidence, warnings = self.drift_detector.detect_drift(data)
            if has_drift:
                self.drift_detector.log_drift_summary(has_drift, confidence, warnings)
            
            # Pydantic validation
            RawCoinPaprikaData(**data)
            return True
        except Exception as e:
            self.logger.warning(
                "Validation failed for CoinPaprika data",
                error=str(e),
                data=data
            )
            return False
    
    def save_raw(self, data: List[Dict[str, Any]]) -> int:
        """Save raw CoinPaprika data to database."""
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
                                INSERT INTO raw_coinpaprika (
                                    coin_id, symbol, name, rank,
                                    price_usd, volume_24h_usd, market_cap_usd,
                                    circulating_supply, total_supply, max_supply,
                                    percent_change_1h, percent_change_24h, percent_change_7d,
                                    raw_data, data_timestamp
                                ) VALUES (
                                    :coin_id, :symbol, :name, :rank,
                                    :price_usd, :volume_24h_usd, :market_cap_usd,
                                    :circulating_supply, :total_supply, :max_supply,
                                    :percent_change_1h, :percent_change_24h, :percent_change_7d,
                                    :raw_data, :data_timestamp
                                )
                                ON CONFLICT (coin_id, data_timestamp) DO NOTHING
                            """),
                            {
                                "coin_id": item.get("coin_id"),
                                "symbol": item.get("symbol"),
                                "name": item.get("name"),
                                "rank": item.get("rank"),
                                "price_usd": item.get("price_usd"),
                                "volume_24h_usd": item.get("volume_24h_usd"),
                                "market_cap_usd": item.get("market_cap_usd"),
                                "circulating_supply": item.get("circulating_supply"),
                                "total_supply": item.get("total_supply"),
                                "max_supply": item.get("max_supply"),
                                "percent_change_1h": item.get("percent_change_1h"),
                                "percent_change_24h": item.get("percent_change_24h"),
                                "percent_change_7d": item.get("percent_change_7d"),
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
                "Saved raw CoinPaprika data",
                saved_count=saved_count,
                total=len(data)
            )
            return saved_count
        
        except Exception as e:
            self.logger.error("Failed to save raw CoinPaprika data", error=str(e))
            raise
    
    def normalize(self, raw_data: Dict[str, Any]) -> Dict[str, Any]:
        """Normalize CoinPaprika data."""
        normalized = NormalizationService.normalize_coinpaprika(
            raw_data,
            datetime.utcnow()
        )
        return normalized.model_dump()
