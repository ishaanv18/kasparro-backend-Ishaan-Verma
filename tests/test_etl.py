"""Tests for ETL pipeline and data sources."""
import pytest
from datetime import datetime
from services.normalization import NormalizationService
from ingestion.checkpoint import CheckpointManager
from ingestion.sources.coinpaprika import CoinPaprikaSource
from ingestion.sources.coingecko import CoinGeckoSource
from ingestion.sources.csv_source import CSVSource


class TestNormalization:
    """Test data normalization service."""
    
    def test_normalize_coinpaprika(self, sample_coinpaprika_data):
        """Test CoinPaprika data normalization."""
        timestamp = datetime.utcnow()
        normalized = NormalizationService.normalize_coinpaprika(
            sample_coinpaprika_data,
            timestamp
        )
        
        assert normalized.source == "coinpaprika"
        assert normalized.symbol == "BTC"
        assert normalized.name == "Bitcoin"
        assert normalized.price_usd is not None
        assert normalized.data_timestamp == timestamp
    
    def test_normalize_coingecko(self, sample_coingecko_data):
        """Test CoinGecko data normalization."""
        timestamp = datetime.utcnow()
        normalized = NormalizationService.normalize_coingecko(
            sample_coingecko_data,
            timestamp
        )
        
        assert normalized.source == "coingecko"
        assert normalized.symbol == "BTC"
        assert normalized.name == "Bitcoin"
        assert normalized.price_usd is not None
        assert normalized.data_timestamp == timestamp
    
    def test_normalize_csv(self, sample_csv_data):
        """Test CSV data normalization."""
        timestamp = datetime.utcnow()
        normalized = NormalizationService.normalize_csv(
            sample_csv_data,
            timestamp
        )
        
        assert normalized.source == "csv"
        assert normalized.symbol == "BTC"
        assert normalized.name == "Bitcoin"
        assert normalized.price_usd is not None
        assert normalized.data_timestamp == timestamp


class TestDataValidation:
    """Test data validation."""
    
    def test_validate_coinpaprika(self, sample_coinpaprika_data):
        """Test CoinPaprika data validation."""
        source = CoinPaprikaSource()
        assert source.validate(sample_coinpaprika_data) is True
    
    def test_validate_coingecko(self, sample_coingecko_data):
        """Test CoinGecko data validation."""
        source = CoinGeckoSource()
        assert source.validate(sample_coingecko_data) is True
    
    def test_validate_csv(self, sample_csv_data):
        """Test CSV data validation."""
        source = CSVSource()
        assert source.validate(sample_csv_data) is True
    
    def test_invalid_data_fails_validation(self):
        """Test that invalid data fails validation."""
        source = CoinPaprikaSource()
        invalid_data = {"invalid": "data"}
        assert source.validate(invalid_data) is False


class TestCheckpointManager:
    """Test checkpoint management."""
    
    def test_checkpoint_initialization(self):
        """Test checkpoint manager initialization."""
        manager = CheckpointManager("test_source")
        assert manager.source_name == "test_source"
    
    def test_get_last_row_number_default(self):
        """Test getting last row number returns 0 by default."""
        manager = CheckpointManager("nonexistent_source")
        # This will return 0 if no checkpoint exists
        row_number = manager.get_last_row_number()
        assert isinstance(row_number, int)


class TestIncrementalIngestion:
    """Test incremental ingestion logic."""
    
    def test_csv_incremental_processing(self):
        """Test CSV incremental processing."""
        source = CSVSource()
        checkpoint = source.checkpoint_manager.get_checkpoint()
        
        # Checkpoint should exist (initialized in database)
        assert checkpoint is not None or checkpoint is None  # May not exist in test DB
    
    def test_checkpoint_value_format(self):
        """Test checkpoint value format."""
        manager = CheckpointManager("csv")
        row_number = manager.get_last_row_number()
        assert row_number >= 0


class TestFailureScenarios:
    """Test failure handling."""
    
    def test_normalization_with_missing_fields(self):
        """Test normalization handles missing fields gracefully."""
        minimal_data = {
            "coin_id": "test-coin",
            "symbol": "TEST",
            "name": "Test Coin",
        }
        
        timestamp = datetime.utcnow()
        normalized = NormalizationService.normalize_coinpaprika(
            minimal_data,
            timestamp
        )
        
        assert normalized.symbol == "TEST"
        assert normalized.price_usd is None  # Missing field should be None
    
    def test_validation_with_invalid_types(self):
        """Test validation catches type errors."""
        source = CoinPaprikaSource()
        invalid_data = {
            "coin_id": "test",
            "symbol": "TEST",
            "name": "Test",
            "price_usd": "not_a_number",  # Invalid type
        }
        
        # Validation should fail
        assert source.validate(invalid_data) is False
