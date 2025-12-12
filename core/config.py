"""Core configuration management using Pydantic Settings."""
from pydantic_settings import BaseSettings
from pydantic import Field
from typing import Optional


class Settings(BaseSettings):
    """Application settings loaded from environment variables."""
    
    # Database
    # NOTE: No defaults - these MUST be set via environment variables
    database_url: str = Field(
        ...,
        alias="DATABASE_URL",
        description="Async database URL (must be set via environment variable)"
    )
    database_url_sync: str = Field(
        ...,
        alias="DATABASE_URL_SYNC",
        description="Sync database URL (must be set via environment variable)"
    )
    
    # API Keys
    coinpaprika_api_key: Optional[str] = Field(default=None, alias="COINPAPRIKA_API_KEY")
    coingecko_api_key: Optional[str] = Field(default=None, alias="COINGECKO_API_KEY")
    
    # API Configuration
    api_host: str = Field(default="0.0.0.0", alias="API_HOST")
    api_port: int = Field(default=8000, alias="API_PORT")
    api_workers: int = Field(default=4, alias="API_WORKERS")
    
    # ETL Configuration
    etl_schedule_minutes: int = Field(default=30, alias="ETL_SCHEDULE_MINUTES")
    etl_batch_size: int = Field(default=1000, alias="ETL_BATCH_SIZE")
    etl_rate_limit_requests: int = Field(default=10, alias="ETL_RATE_LIMIT_REQUESTS")
    etl_rate_limit_period: int = Field(default=60, alias="ETL_RATE_LIMIT_PERIOD")
    
    # CSV Configuration
    csv_data_path: str = Field(default="/app/data/crypto_data.csv", alias="CSV_DATA_PATH")
    
    # Logging
    log_level: str = Field(default="INFO", alias="LOG_LEVEL")
    log_format: str = Field(default="json", alias="LOG_FORMAT")
    
    # Environment
    environment: str = Field(default="development", alias="ENVIRONMENT")
    
    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"
        case_sensitive = False


# Global settings instance
settings = Settings()
