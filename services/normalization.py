"""Data normalization service to unify data from different sources."""
from typing import Dict, Any
from datetime import datetime
from decimal import Decimal
from schemas.normalized import NormalizedCryptoData
from core.logging import get_logger

logger = get_logger(__name__)


class NormalizationService:
    """Service to normalize cryptocurrency data from different sources."""
    
    @staticmethod
    def normalize_coinpaprika(raw_data: Dict[str, Any], data_timestamp: datetime) -> NormalizedCryptoData:
        """Normalize CoinPaprika data to unified schema."""
        try:
            # Extract price from nested quotes structure if present
            # NOTE: CoinPaprika changed their API structure, had to handle both formats
            price_usd = raw_data.get("price_usd")
            if price_usd is None and "quotes" in raw_data:
                quotes = raw_data.get("quotes", {})
                usd_quote = quotes.get("USD", {})
                price_usd = usd_quote.get("price")
            
            # Extract volume and market cap
            volume_24h = raw_data.get("volume_24h_usd")
            market_cap = raw_data.get("market_cap_usd")
            
            if volume_24h is None and "quotes" in raw_data:
                usd_quote = raw_data.get("quotes", {}).get("USD", {})
                volume_24h = usd_quote.get("volume_24h")
                market_cap = usd_quote.get("market_cap")
            
            return NormalizedCryptoData(
                source="coinpaprika",
                source_id=raw_data.get("coin_id") or raw_data.get("id", ""),
                symbol=raw_data.get("symbol", "").upper(),
                name=raw_data.get("name", ""),
                price_usd=Decimal(str(price_usd)) if price_usd else None,
                market_cap_usd=Decimal(str(market_cap)) if market_cap else None,
                volume_24h_usd=Decimal(str(volume_24h)) if volume_24h else None,
                rank=raw_data.get("rank"),
                circulating_supply=Decimal(str(raw_data["circulating_supply"])) if raw_data.get("circulating_supply") else None,
                total_supply=Decimal(str(raw_data["total_supply"])) if raw_data.get("total_supply") else None,
                max_supply=Decimal(str(raw_data["max_supply"])) if raw_data.get("max_supply") else None,
                percent_change_24h=Decimal(str(raw_data["percent_change_24h"])) if raw_data.get("percent_change_24h") else None,
                additional_data={
                    "percent_change_1h": raw_data.get("percent_change_1h"),
                    "percent_change_7d": raw_data.get("percent_change_7d"),
                },
                data_timestamp=data_timestamp,
            )
        except Exception as e:
            logger.error("Failed to normalize CoinPaprika data", error=str(e), raw_data=raw_data)
            raise
    
    @staticmethod
    def normalize_coingecko(raw_data: Dict[str, Any], data_timestamp: datetime) -> NormalizedCryptoData:
        """Normalize CoinGecko data to unified schema."""
        try:
            return NormalizedCryptoData(
                source="coingecko",
                source_id=raw_data.get("coin_id") or raw_data.get("id", ""),
                symbol=raw_data.get("symbol", "").upper(),
                name=raw_data.get("name", ""),
                price_usd=Decimal(str(raw_data["current_price"])) if raw_data.get("current_price") else None,
                market_cap_usd=Decimal(str(raw_data["market_cap"])) if raw_data.get("market_cap") else None,
                volume_24h_usd=Decimal(str(raw_data["total_volume"])) if raw_data.get("total_volume") else None,
                rank=raw_data.get("market_cap_rank"),
                circulating_supply=Decimal(str(raw_data["circulating_supply"])) if raw_data.get("circulating_supply") else None,
                total_supply=Decimal(str(raw_data["total_supply"])) if raw_data.get("total_supply") else None,
                max_supply=Decimal(str(raw_data["max_supply"])) if raw_data.get("max_supply") else None,
                percent_change_24h=Decimal(str(raw_data["price_change_percentage_24h"])) if raw_data.get("price_change_percentage_24h") else None,
                additional_data={
                    "high_24h": raw_data.get("high_24h"),
                    "low_24h": raw_data.get("low_24h"),
                    "price_change_24h": raw_data.get("price_change_24h"),
                    "ath": raw_data.get("ath"),
                    "atl": raw_data.get("atl"),
                },
                data_timestamp=data_timestamp,
            )
        except Exception as e:
            logger.error("Failed to normalize CoinGecko data", error=str(e), raw_data=raw_data)
            raise
    
    @staticmethod
    def normalize_csv(raw_data: Dict[str, Any], data_timestamp: datetime) -> NormalizedCryptoData:
        """Normalize CSV data to unified schema."""
        try:
            # Generate a source_id from symbol for CSV data
            symbol = raw_data.get("symbol", "").upper()
            source_id = f"csv_{symbol}"
            
            return NormalizedCryptoData(
                source="csv",
                source_id=source_id,
                symbol=symbol,
                name=raw_data.get("name", ""),
                price_usd=Decimal(str(raw_data["price_usd"])) if raw_data.get("price_usd") else None,
                market_cap_usd=Decimal(str(raw_data["market_cap_usd"])) if raw_data.get("market_cap_usd") else None,
                volume_24h_usd=Decimal(str(raw_data["volume_24h_usd"])) if raw_data.get("volume_24h_usd") else None,
                rank=None,  # CSV might not have rank
                circulating_supply=None,
                total_supply=None,
                max_supply=None,
                percent_change_24h=Decimal(str(raw_data["percent_change_24h"])) if raw_data.get("percent_change_24h") else None,
                additional_data={},
                data_timestamp=data_timestamp,
            )
        except Exception as e:
            logger.error("Failed to normalize CSV data", error=str(e), raw_data=raw_data)
            raise
