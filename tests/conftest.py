"""Pytest configuration and fixtures."""
import pytest
import asyncio
from sqlalchemy import create_engine, text
from core.config import settings


@pytest.fixture(scope="session")
def event_loop():
    """Create event loop for async tests."""
    loop = asyncio.get_event_loop_policy().new_event_loop()
    yield loop
    loop.close()


@pytest.fixture(scope="session")
def db_engine():
    """Create database engine for tests."""
    engine = create_engine(settings.database_url_sync)
    yield engine
    engine.dispose()


@pytest.fixture(scope="function")
def db_connection(db_engine):
    """Create database connection for each test."""
    connection = db_engine.connect()
    transaction = connection.begin()
    
    yield connection
    
    transaction.rollback()
    connection.close()


@pytest.fixture
def sample_coinpaprika_data():
    """Sample CoinPaprika API response."""
    return {
        "coin_id": "btc-bitcoin",
        "symbol": "BTC",
        "name": "Bitcoin",
        "rank": 1,
        "price_usd": 43250.50,
        "volume_24h_usd": 28500000000,
        "market_cap_usd": 845000000000,
        "circulating_supply": 19500000,
        "total_supply": 21000000,
        "max_supply": 21000000,
        "percent_change_1h": 0.5,
        "percent_change_24h": 2.5,
        "percent_change_7d": 5.2,
    }


@pytest.fixture
def sample_coingecko_data():
    """Sample CoinGecko API response."""
    return {
        "coin_id": "bitcoin",
        "symbol": "btc",
        "name": "Bitcoin",
        "current_price": 43250.50,
        "market_cap": 845000000000,
        "market_cap_rank": 1,
        "total_volume": 28500000000,
        "high_24h": 44000.00,
        "low_24h": 42500.00,
        "price_change_24h": 1050.50,
        "price_change_percentage_24h": 2.5,
        "circulating_supply": 19500000,
        "total_supply": 21000000,
        "max_supply": 21000000,
        "ath": 69000.00,
        "atl": 67.81,
    }


@pytest.fixture
def sample_csv_data():
    """Sample CSV data."""
    return {
        "symbol": "BTC",
        "name": "Bitcoin",
        "price_usd": 43250.50,
        "market_cap_usd": 845000000000,
        "volume_24h_usd": 28500000000,
        "percent_change_24h": 2.5,
    }
