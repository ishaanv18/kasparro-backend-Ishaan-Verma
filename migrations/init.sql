-- Kasparro Backend & ETL System - Database Schema
-- PostgreSQL initialization script

-- Drop existing tables if they exist
DROP TABLE IF EXISTS etl_runs CASCADE;
DROP TABLE IF EXISTS etl_checkpoints CASCADE;
DROP TABLE IF EXISTS normalized_crypto_data CASCADE;
DROP TABLE IF EXISTS raw_csv CASCADE;
DROP TABLE IF EXISTS raw_coingecko CASCADE;
DROP TABLE IF EXISTS raw_coinpaprika CASCADE;

-- Raw data tables

-- CoinPaprika raw data
CREATE TABLE raw_coinpaprika (
    id SERIAL PRIMARY KEY,
    coin_id VARCHAR(100) NOT NULL,
    symbol VARCHAR(20),
    name VARCHAR(255),
    rank INTEGER,
    price_usd DECIMAL(20, 8),
    volume_24h_usd DECIMAL(20, 2),
    market_cap_usd DECIMAL(20, 2),
    circulating_supply DECIMAL(20, 2),
    total_supply DECIMAL(20, 2),
    max_supply DECIMAL(20, 2),
    percent_change_1h DECIMAL(10, 4),
    percent_change_24h DECIMAL(10, 4),
    percent_change_7d DECIMAL(10, 4),
    raw_data JSONB,
    ingested_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    data_timestamp TIMESTAMP,
    UNIQUE(coin_id, data_timestamp)
);

-- CoinGecko raw data
CREATE TABLE raw_coingecko (
    id SERIAL PRIMARY KEY,
    coin_id VARCHAR(100) NOT NULL,
    symbol VARCHAR(20),
    name VARCHAR(255),
    current_price DECIMAL(20, 8),
    market_cap DECIMAL(20, 2),
    market_cap_rank INTEGER,
    total_volume DECIMAL(20, 2),
    high_24h DECIMAL(20, 8),
    low_24h DECIMAL(20, 8),
    price_change_24h DECIMAL(20, 8),
    price_change_percentage_24h DECIMAL(10, 4),
    circulating_supply DECIMAL(20, 2),
    total_supply DECIMAL(20, 2),
    max_supply DECIMAL(20, 2),
    ath DECIMAL(20, 8),
    atl DECIMAL(20, 8),
    raw_data JSONB,
    ingested_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    data_timestamp TIMESTAMP,
    UNIQUE(coin_id, data_timestamp)
);

-- CSV raw data
CREATE TABLE raw_csv (
    id SERIAL PRIMARY KEY,
    symbol VARCHAR(20),
    name VARCHAR(255),
    price_usd DECIMAL(20, 8),
    market_cap_usd DECIMAL(20, 2),
    volume_24h_usd DECIMAL(20, 2),
    percent_change_24h DECIMAL(10, 4),
    raw_data JSONB,
    ingested_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    data_timestamp TIMESTAMP,
    source_file VARCHAR(255),
    row_number INTEGER,
    UNIQUE(source_file, row_number)
);

-- Normalized unified schema
CREATE TABLE normalized_crypto_data (
    id SERIAL PRIMARY KEY,
    source VARCHAR(50) NOT NULL, -- 'coinpaprika', 'coingecko', 'csv'
    source_id VARCHAR(100) NOT NULL,
    symbol VARCHAR(20) NOT NULL,
    name VARCHAR(255) NOT NULL,
    price_usd DECIMAL(20, 8),
    market_cap_usd DECIMAL(20, 2),
    volume_24h_usd DECIMAL(20, 2),
    rank INTEGER,
    circulating_supply DECIMAL(20, 2),
    total_supply DECIMAL(20, 2),
    max_supply DECIMAL(20, 2),
    percent_change_24h DECIMAL(10, 4),
    additional_data JSONB, -- Store source-specific fields
    data_timestamp TIMESTAMP NOT NULL,
    ingested_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(source, source_id, data_timestamp)
);

-- ETL checkpoints for incremental ingestion
CREATE TABLE etl_checkpoints (
    id SERIAL PRIMARY KEY,
    source_name VARCHAR(50) NOT NULL UNIQUE, -- 'coinpaprika', 'coingecko', 'csv'
    checkpoint_type VARCHAR(50) NOT NULL, -- 'timestamp', 'row_number', 'cursor'
    checkpoint_value TEXT NOT NULL,
    last_success_at TIMESTAMP,
    last_failure_at TIMESTAMP,
    failure_reason TEXT,
    metadata JSONB,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- ETL run metadata
CREATE TABLE etl_runs (
    id SERIAL PRIMARY KEY,
    run_id UUID NOT NULL UNIQUE,
    source_name VARCHAR(50) NOT NULL,
    status VARCHAR(20) NOT NULL, -- 'running', 'success', 'failed'
    started_at TIMESTAMP NOT NULL,
    completed_at TIMESTAMP,
    duration_seconds INTEGER,
    records_fetched INTEGER DEFAULT 0,
    records_processed INTEGER DEFAULT 0,
    records_failed INTEGER DEFAULT 0,
    error_message TEXT,
    metadata JSONB,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Indexes for performance
CREATE INDEX idx_normalized_symbol ON normalized_crypto_data(symbol);
CREATE INDEX idx_normalized_source ON normalized_crypto_data(source);
CREATE INDEX idx_normalized_timestamp ON normalized_crypto_data(data_timestamp);
CREATE INDEX idx_normalized_source_timestamp ON normalized_crypto_data(source, data_timestamp);

CREATE INDEX idx_raw_coinpaprika_timestamp ON raw_coinpaprika(data_timestamp);
CREATE INDEX idx_raw_coingecko_timestamp ON raw_coingecko(data_timestamp);
CREATE INDEX idx_raw_csv_timestamp ON raw_csv(data_timestamp);

CREATE INDEX idx_etl_runs_source ON etl_runs(source_name);
CREATE INDEX idx_etl_runs_status ON etl_runs(status);
CREATE INDEX idx_etl_runs_started ON etl_runs(started_at);

-- Insert initial checkpoint records
INSERT INTO etl_checkpoints (source_name, checkpoint_type, checkpoint_value)
VALUES 
    ('coinpaprika', 'timestamp', '2024-01-01T00:00:00Z'),
    ('coingecko', 'timestamp', '2024-01-01T00:00:00Z'),
    ('csv', 'row_number', '0')
ON CONFLICT (source_name) DO NOTHING;

-- Grant permissions
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO kasparro;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public TO kasparro;
