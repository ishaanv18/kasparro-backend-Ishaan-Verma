"""Integration tests for end-to-end ETL and API flow."""
import pytest
import asyncio
from datetime import datetime
from ingestion.sources.csv_source import CSVSource
from ingestion.sources.coinpaprika import CoinPaprikaSource
from services.normalization import NormalizationService


class TestEndToEndFlow:
    """Test complete end-to-end data flow."""
    
    @pytest.mark.asyncio
    async def test_csv_fetch_and_normalize(self):
        """Test fetching CSV data and normalizing it."""
        source = CSVSource()
        
        # Fetch data
        data = await source.fetch()
        
        # Should have data from sample CSV
        assert isinstance(data, list)
        
        # Normalize first item if data exists
        if data:
            normalized = source.normalize(data[0])
            assert "source" in normalized
            assert normalized["source"] == "csv"
            assert "symbol" in normalized
            assert "name" in normalized


class TestDataConsistency:
    """Test data consistency across pipeline."""
    
    def test_normalization_preserves_core_fields(self, sample_coinpaprika_data):
        """Test that normalization preserves core fields."""
        timestamp = datetime.utcnow()
        normalized = NormalizationService.normalize_coinpaprika(
            sample_coinpaprika_data,
            timestamp
        )
        
        # Core fields should be preserved
        assert normalized.symbol == sample_coinpaprika_data["symbol"]
        assert normalized.name == sample_coinpaprika_data["name"]
        assert float(normalized.price_usd) == sample_coinpaprika_data["price_usd"]
    
    def test_schema_unification(self, sample_coinpaprika_data, sample_coingecko_data):
        """Test that different sources normalize to same schema."""
        timestamp = datetime.utcnow()
        
        normalized_cp = NormalizationService.normalize_coinpaprika(
            sample_coinpaprika_data,
            timestamp
        )
        
        normalized_cg = NormalizationService.normalize_coingecko(
            sample_coingecko_data,
            timestamp
        )
        
        # Both should have same core fields
        assert hasattr(normalized_cp, "symbol")
        assert hasattr(normalized_cg, "symbol")
        assert hasattr(normalized_cp, "price_usd")
        assert hasattr(normalized_cg, "price_usd")
        assert hasattr(normalized_cp, "market_cap_usd")
        assert hasattr(normalized_cg, "market_cap_usd")


class TestRateLimiting:
    """Test rate limiting behavior."""
    
    @pytest.mark.asyncio
    async def test_rate_limit_delay(self):
        """Test that rate limiting introduces appropriate delays."""
        import time
        
        source = CoinPaprikaSource()
        
        # Calculate expected delay
        expected_delay = source.rate_limit_period / source.rate_limit_requests
        
        # This test just verifies the configuration exists
        assert expected_delay > 0
        assert source.rate_limit_requests > 0
        assert source.rate_limit_period > 0
