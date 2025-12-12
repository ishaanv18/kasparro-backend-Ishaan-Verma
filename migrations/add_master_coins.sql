-- Add master coins table and entity resolution support
-- This migration adds entity resolution capability to unify coins across sources

-- Create master coins table for canonical coin entities
CREATE TABLE IF NOT EXISTS master_coins (
    id SERIAL PRIMARY KEY,
    symbol VARCHAR(20) NOT NULL UNIQUE,
    name VARCHAR(255) NOT NULL,
    canonical_id VARCHAR(100) UNIQUE, -- e.g., 'bitcoin', 'ethereum'
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create mapping table to link source-specific IDs to master coins
CREATE TABLE IF NOT EXISTS coin_source_mappings (
    id SERIAL PRIMARY KEY,
    master_coin_id INTEGER NOT NULL REFERENCES master_coins(id) ON DELETE CASCADE,
    source VARCHAR(50) NOT NULL, -- 'coinpaprika', 'coingecko', 'csv'
    source_id VARCHAR(100) NOT NULL,
    confidence_score DECIMAL(3, 2) DEFAULT 1.0, -- 0.0 to 1.0
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(source, source_id)
);

-- Add master_coin_id to normalized_crypto_data
ALTER TABLE normalized_crypto_data 
ADD COLUMN IF NOT EXISTS master_coin_id INTEGER REFERENCES master_coins(id);

-- Drop old unique constraint and create new one
ALTER TABLE normalized_crypto_data 
DROP CONSTRAINT IF EXISTS normalized_crypto_data_source_source_id_data_timestamp_key;

-- New constraint allows multiple sources for same coin at same timestamp
ALTER TABLE normalized_crypto_data 
ADD CONSTRAINT normalized_crypto_data_unique_entry 
UNIQUE(source, source_id, data_timestamp);

-- Create indexes for performance
CREATE INDEX IF NOT EXISTS idx_master_coins_symbol ON master_coins(symbol);
CREATE INDEX IF NOT EXISTS idx_master_coins_canonical_id ON master_coins(canonical_id);
CREATE INDEX IF NOT EXISTS idx_coin_source_mappings_master_coin ON coin_source_mappings(master_coin_id);
CREATE INDEX IF NOT EXISTS idx_coin_source_mappings_source ON coin_source_mappings(source, source_id);
CREATE INDEX IF NOT EXISTS idx_normalized_master_coin ON normalized_crypto_data(master_coin_id);
CREATE INDEX IF NOT EXISTS idx_normalized_master_coin_timestamp ON normalized_crypto_data(master_coin_id, data_timestamp);

-- Seed common cryptocurrencies
INSERT INTO master_coins (symbol, name, canonical_id) VALUES
    ('BTC', 'Bitcoin', 'bitcoin'),
    ('ETH', 'Ethereum', 'ethereum'),
    ('USDT', 'Tether', 'tether'),
    ('BNB', 'Binance Coin', 'binancecoin'),
    ('SOL', 'Solana', 'solana'),
    ('USDC', 'USD Coin', 'usd-coin'),
    ('XRP', 'XRP', 'ripple'),
    ('ADA', 'Cardano', 'cardano'),
    ('DOGE', 'Dogecoin', 'dogecoin'),
    ('TRX', 'TRON', 'tron'),
    ('AVAX', 'Avalanche', 'avalanche'),
    ('DOT', 'Polkadot', 'polkadot'),
    ('MATIC', 'Polygon', 'matic-network'),
    ('LTC', 'Litecoin', 'litecoin'),
    ('LINK', 'Chainlink', 'chainlink')
ON CONFLICT (symbol) DO NOTHING;

-- Create common source mappings for major coins
-- CoinPaprika mappings
INSERT INTO coin_source_mappings (master_coin_id, source, source_id) 
SELECT id, 'coinpaprika', canonical_id || '-' || LOWER(symbol)
FROM master_coins
WHERE canonical_id IN ('bitcoin', 'ethereum', 'tether', 'binancecoin', 'solana', 'usd-coin', 'ripple', 'cardano', 'dogecoin')
ON CONFLICT (source, source_id) DO NOTHING;

-- CoinGecko mappings (uses canonical_id directly)
INSERT INTO coin_source_mappings (master_coin_id, source, source_id)
SELECT id, 'coingecko', canonical_id
FROM master_coins
ON CONFLICT (source, source_id) DO NOTHING;

-- CSV mappings (uses csv_SYMBOL format)
INSERT INTO coin_source_mappings (master_coin_id, source, source_id)
SELECT id, 'csv', 'csv_' || symbol
FROM master_coins
ON CONFLICT (source, source_id) DO NOTHING;

-- Update existing normalized_crypto_data with master_coin_id
UPDATE normalized_crypto_data ncd
SET master_coin_id = csm.master_coin_id
FROM coin_source_mappings csm
WHERE ncd.source = csm.source 
  AND ncd.source_id = csm.source_id
  AND ncd.master_coin_id IS NULL;
