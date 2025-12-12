# Kasparro Backend & ETL System

A cryptocurrency data ingestion and API service for the Kasparro backend assignment. This project implements a multi-source ETL pipeline that fetches data from CoinPaprika API, CoinGecko API, and CSV files, normalizes it into a unified schema, and serves it through a RESTful API.

**Tech Stack**: FastAPI, PostgreSQL, Docker, Python 3.11

## Features

### Core Implementation (P0 & P1)
- **Multi-Source ETL Pipeline**: Ingests data from CoinPaprika API, CoinGecko API, and CSV files
- **Data Normalization**: Unified schema across heterogeneous data sources using Pydantic validation
- **RESTful API**: FastAPI-based service with comprehensive endpoints
- **Incremental Ingestion**: Checkpoint-based system for resume-on-failure capability
- **Containerization**: Complete Docker setup with docker-compose orchestration
- **Testing**: Comprehensive test suite covering ETL logic, API endpoints, and integration scenarios

### Advanced Features (P2)
- **Prometheus Metrics**: `/metrics` endpoint for production monitoring
- **Schema Drift Detection**: Fuzzy matching algorithm to detect API schema changes
- **Anomaly Detection**: Statistical analysis of ETL runs to identify unusual patterns
- **Run Comparison**: Historical ETL run analysis and comparison
- **CI/CD Pipeline**: GitHub Actions for automated testing and deployment
- **Structured Logging**: JSON-formatted logs using structlog

## Architecture

### System Design
```
┌─────────────────┐     ┌─────────────────┐     ┌─────────────────┐
│  CoinPaprika    │────▶│                 │     │                 │
│      API        │     │   ETL Pipeline  │────▶│   PostgreSQL    │
└─────────────────┘     │                 │     │    Database     │
                        │   - Validation  │     └─────────────────┘
┌─────────────────┐     │   - Transform   │              │
│   CoinGecko     │────▶│   - Checkpoint  │              │
│      API        │     │                 │              ▼
└─────────────────┘     └─────────────────┘     ┌─────────────────┐
                                                 │   FastAPI       │
┌─────────────────┐                              │   Backend       │
│   CSV Files     │─────────────────────────────▶│                 │
└─────────────────┘                              └─────────────────┘
```

### Database Schema
- **raw_coinpaprika**: Raw CoinPaprika API responses
- **raw_coingecko**: Raw CoinGecko API responses
- **raw_csv**: Raw CSV data
- **normalized_crypto_data**: Unified normalized data
- **etl_checkpoints**: Incremental ingestion tracking
- **etl_runs**: ETL execution metadata
- **master_coins**: Canonical cryptocurrency entities for cross-source unification
- **coin_source_mappings**: Maps source-specific IDs to master coin entities

## Live Deployment

**Production URL**: https://kasparro-backend-ishaan-verma.onrender.com

> [!NOTE]
> The application is deployed on Render.com's free tier. Initial requests may take 30-60 seconds as the service spins up from sleep mode.

### Quick Verification

```bash
# Check health endpoint
curl https://kasparro-backend-ishaan-verma.onrender.com/health

# View API documentation
open https://kasparro-backend-ishaan-verma.onrender.com/docs

# Query cryptocurrency data
curl "https://kasparro-backend-ishaan-verma.onrender.com/data?symbol=BTC"
```

## Quick Start

### Prerequisites
- Docker Desktop
- 4GB RAM minimum

### Setup

1. **Clone and configure**
   ```bash
   cd kasparro-backend-Ishaan-Verma
   cp .env.example .env
   ```

2. **Configure environment variables**
   
   Edit `.env` and set required values:
   ```env
   # Required: Set a secure password
   POSTGRES_PASSWORD=your_secure_password_here
   
   # Optional: Add API keys
   COINGECKO_API_KEY=your_key_here
   COINPAPRIKA_API_KEY=  # Leave empty to use public API
   ```

3. **Start the system**
   ```bash
   docker compose up -d --build
   ```

4. **Verify local deployment**
   ```bash
   # Check health
   curl http://localhost:8000/health
   
   # View API documentation
   open http://localhost:8000/docs
   ```

## API Endpoints

| Endpoint | Description |
|----------|-------------|
| `GET /` | Service information |
| `GET /data` | Paginated cryptocurrency data with filtering |
| `GET /health` | System health check |
| `GET /stats` | ETL statistics and metrics |
| `GET /metrics` | Prometheus metrics |
| `GET /runs` | ETL run history |
| `GET /compare-runs` | Compare two ETL runs |
| `GET /anomalies` | Detect anomalies in ETL runs |
| `GET /docs` | Interactive API documentation |

### Example Usage

```bash
# Get cryptocurrency data with pagination
curl "http://localhost:8000/data?page=1&page_size=10&symbol=BTC"

# Check ETL statistics
curl http://localhost:8000/stats

# View Prometheus metrics
curl http://localhost:8000/metrics

# Detect anomalies in last 24 hours
curl "http://localhost:8000/anomalies?hours=24"
```

## Testing

```bash
# Run complete test suite
docker compose exec api pytest tests/ -v --cov=. --cov-report=term

# Run specific test file
docker compose exec api pytest tests/test_etl.py -v
```

## Project Structure

```
├── api/                    # FastAPI application
│   ├── main.py            # Application setup
│   └── routes/            # API endpoints
├── ingestion/             # ETL pipeline
│   ├── etl.py            # Orchestration
│   ├── checkpoint.py     # Checkpoint management
│   └── sources/          # Data source implementations
├── services/              # Business logic
│   ├── database.py       # Database operations
│   └── normalization.py  # Data transformation
├── schemas/               # Pydantic models
├── core/                  # Configuration and logging
├── tests/                 # Test suite
├── migrations/            # Database schema
└── data/                  # Sample CSV data
```

## Configuration

All configuration is managed through environment variables in `.env`:

| Variable | Description | Default |
|----------|-------------|---------|
| `COINGECKO_API_KEY` | CoinGecko API key | - |
| `COINPAPRIKA_API_KEY` | CoinPaprika API key | - |
| `ETL_SCHEDULE_MINUTES` | ETL run interval | 30 |
| `LOG_LEVEL` | Logging level | INFO |

## Development Commands

```bash
# Start services
docker compose up -d

# Stop services
docker compose down

# View logs
docker compose logs -f

# Restart after code changes
docker compose restart api

# Access container shell
docker compose exec api /bin/bash

# Access database
docker compose exec postgres psql -U kasparro -d kasparro_db
```

## Technical Highlights

### Checkpoint System
Implements different strategies per source:
- **APIs**: Timestamp-based checkpoints
- **CSV**: Row number-based incremental processing

### Data Normalization
Transforms heterogeneous schemas into unified format:
- Handles nested JSON structures
- Type validation with Pydantic
- Graceful handling of missing fields

### Entity Resolution
Unifies cryptocurrency data across sources:
- **Master Coins Table**: Canonical entities for each cryptocurrency
- **Fuzzy Matching**: Intelligent symbol and name matching across sources
- **Cross-Source Unification**: Bitcoin from CoinPaprika, CoinGecko, and CSV map to single entity
- **Automatic Discovery**: New coins are automatically added to master table

### Error Handling
- Comprehensive exception handling
- Checkpoint updates on failure
- Detailed error logging
- Idempotent writes using `ON CONFLICT`

## Performance Considerations

- Async I/O for concurrent API calls
- Database connection pooling
- Indexed queries for fast retrieval
- Configurable batch sizes
- Rate limiting to respect API quotas

## Deployment

### Local Deployment

The system is containerized and ready for local deployment. See `docker-compose.yml` for service configuration.

### Cloud Deployment (Render.com)

This application is configured for deployment on Render.com:

1. **Push to GitHub**:
   ```bash
   git add .
   git commit -m "Prepare for deployment"
   git push origin main
   ```

2. **Deploy on Render**:
   - Go to [Render.com](https://render.com) and sign in
   - Click "New +" → "Blueprint"
   - Connect your GitHub repository
   - Render will automatically detect `render.yaml` and create:
     - PostgreSQL database
     - Web service (FastAPI)
     - Background worker (ETL)

3. **Set Environment Variables**:
   - In Render dashboard, set:
     - `COINGECKO_API_KEY` (optional)
     - `COINPAPRIKA_API_KEY` (optional)
   - Database credentials are automatically configured

4. **Run Database Migration**:
   - After deployment, access the web service shell
   - Run: `psql $DATABASE_URL < migrations/init.sql`
   - Run: `psql $DATABASE_URL < migrations/add_master_coins.sql`

The application will be available at `https://your-app-name.onrender.com`

## License

MIT License

---

**Author**: Ishaan Verma  
**Assignment**: Kasparro Backend Challenge  
**Date**: December 2025
