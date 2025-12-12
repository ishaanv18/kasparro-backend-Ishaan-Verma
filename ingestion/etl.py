"""ETL orchestration and scheduling.

This handles the main ETL pipeline. Learned about schedulers for this project!
TODO: Figure out how to add retry logic when APIs fail
TODO: Maybe try parallel processing? Not sure how yet
"""
import asyncio
import uuid
from datetime import datetime
from typing import List
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.interval import IntervalTrigger
from sqlalchemy import text

from ingestion.sources.coinpaprika import CoinPaprikaSource
from ingestion.sources.coingecko import CoinGeckoSource
from ingestion.sources.csv_source import CSVSource
from services.database import get_sync_connection
from core.config import settings
from core.logging import setup_logging, get_logger

# Setup logging
setup_logging()
logger = get_logger(__name__)

# NOTE: Tried using celery for this but APScheduler is simpler for now


class ETLOrchestrator:
    """Orchestrate ETL processes across all data sources."""
    
    def __init__(self):
        self.sources = [
            CoinPaprikaSource(),
            CoinGeckoSource(),
            CSVSource(),
        ]
        self.scheduler = AsyncIOScheduler()
    
    async def run_etl_for_source(self, source):
        """
        Run ETL for a single data source.
        
        Args:
            source: Data source instance
        """
        run_id = str(uuid.uuid4())
        source_name = source.source_name
        started_at = datetime.utcnow()
        
        logger.info(
            "Starting ETL run",
            run_id=run_id,
            source=source_name
        )
        
        # Record run start
        try:
            with get_sync_connection() as conn:
                conn.execute(
                    text("""
                        INSERT INTO etl_runs (
                            run_id, source_name, status, started_at
                        ) VALUES (
                            :run_id, :source_name, 'running', :started_at
                        )
                    """),
                    {
                        "run_id": run_id,
                        "source_name": source_name,
                        "started_at": started_at,
                    }
                )
                conn.commit()
        except Exception as e:
            logger.error("Failed to record ETL run start", error=str(e))
        
        records_fetched = 0
        records_processed = 0
        records_failed = 0
        error_message = None
        status = "success"
        
        try:
            # Fetch data
            raw_data = await source.fetch()
            records_fetched = len(raw_data)
            
            logger.info(
                "Fetched data",
                run_id=run_id,
                source=source_name,
                count=records_fetched
            )
            
            # Save raw data
            if raw_data:
                saved_count = source.save_raw(raw_data)
                logger.info(
                    "Saved raw data",
                    run_id=run_id,
                    source=source_name,
                    count=saved_count
                )
                
                # Normalize and save to unified schema
                with get_sync_connection() as conn:
                    for item in raw_data:
                        try:
                            normalized = source.normalize(item)
                            
                            conn.execute(
                                text("""
                                    INSERT INTO normalized_crypto_data (
                                        source, source_id, master_coin_id, symbol, name,
                                        price_usd, market_cap_usd, volume_24h_usd,
                                        rank, circulating_supply, total_supply, max_supply,
                                        percent_change_24h, additional_data, data_timestamp
                                    ) VALUES (
                                        :source, :source_id, :master_coin_id, :symbol, :name,
                                        :price_usd, :market_cap_usd, :volume_24h_usd,
                                        :rank, :circulating_supply, :total_supply, :max_supply,
                                        :percent_change_24h, :additional_data, :data_timestamp
                                    )
                                    ON CONFLICT (source, source_id, data_timestamp) DO UPDATE SET
                                        master_coin_id = EXCLUDED.master_coin_id,
                                        price_usd = EXCLUDED.price_usd,
                                        market_cap_usd = EXCLUDED.market_cap_usd,
                                        volume_24h_usd = EXCLUDED.volume_24h_usd
                                """),
                                {
                                    "source": normalized["source"],
                                    "source_id": normalized["source_id"],
                                    "master_coin_id": normalized.get("master_coin_id"),
                                    "symbol": normalized["symbol"],
                                    "name": normalized["name"],
                                    "price_usd": normalized.get("price_usd"),
                                    "market_cap_usd": normalized.get("market_cap_usd"),
                                    "volume_24h_usd": normalized.get("volume_24h_usd"),
                                    "rank": normalized.get("rank"),
                                    "circulating_supply": normalized.get("circulating_supply"),
                                    "total_supply": normalized.get("total_supply"),
                                    "max_supply": normalized.get("max_supply"),
                                    "percent_change_24h": normalized.get("percent_change_24h"),
                                    "additional_data": normalized.get("additional_data"),
                                    "data_timestamp": normalized["data_timestamp"],
                                }
                            )
                            records_processed += 1
                        except Exception as e:
                            logger.warning(
                                "Failed to normalize record",
                                run_id=run_id,
                                source=source_name,
                                error=str(e)
                            )
                            records_failed += 1
                            continue
                    
                    conn.commit()
                
                logger.info(
                    "Normalized and saved data",
                    run_id=run_id,
                    source=source_name,
                    processed=records_processed,
                    failed=records_failed
                )
                
                # Update checkpoint
                checkpoint_value = datetime.utcnow().isoformat()
                if source_name == "csv":
                    # For CSV, use row number
                    last_row = source.checkpoint_manager.get_last_row_number()
                    checkpoint_value = str(last_row + records_fetched)
                
                source.update_checkpoint(
                    checkpoint_value=checkpoint_value,
                    success=True,
                    metadata={
                        "run_id": run_id,
                        "records_processed": records_processed,
                    }
                )
        
        except Exception as e:
            logger.error(
                "ETL run failed",
                run_id=run_id,
                source=source_name,
                error=str(e)
            )
            error_message = str(e)
            status = "failed"
            
            # Update checkpoint with failure
            source.update_checkpoint(
                checkpoint_value="",
                success=False,
                error_message=error_message
            )
        
        # Record run completion
        completed_at = datetime.utcnow()
        duration_seconds = int((completed_at - started_at).total_seconds())
        
        try:
            with get_sync_connection() as conn:
                conn.execute(
                    text("""
                        UPDATE etl_runs
                        SET 
                            status = :status,
                            completed_at = :completed_at,
                            duration_seconds = :duration_seconds,
                            records_fetched = :records_fetched,
                            records_processed = :records_processed,
                            records_failed = :records_failed,
                            error_message = :error_message
                        WHERE run_id = :run_id
                    """),
                    {
                        "run_id": run_id,
                        "status": status,
                        "completed_at": completed_at,
                        "duration_seconds": duration_seconds,
                        "records_fetched": records_fetched,
                        "records_processed": records_processed,
                        "records_failed": records_failed,
                        "error_message": error_message,
                    }
                )
                conn.commit()
        except Exception as e:
            logger.error("Failed to record ETL run completion", error=str(e))
        
        # Track metrics for Prometheus
        try:
            from api.routes.metrics import track_etl_run
            track_etl_run(
                source=source_name,
                status=status,
                duration=float(duration_seconds),
                records=records_processed
            )
        except Exception as e:
            logger.warning("Failed to track ETL metrics", error=str(e))
        
        logger.info(
            "ETL run completed",
            run_id=run_id,
            source=source_name,
            status=status,
            duration_seconds=duration_seconds,
            records_processed=records_processed
        )
    
    async def run_all_sources(self):
        """Run ETL for all sources."""
        logger.info("Running ETL for all sources")
        
        tasks = [self.run_etl_for_source(source) for source in self.sources]
        await asyncio.gather(*tasks, return_exceptions=True)
        
        logger.info("Completed ETL for all sources")
    
    def start_scheduler(self):
        """Start the ETL scheduler."""
        logger.info(
            "Starting ETL scheduler",
            interval_minutes=settings.etl_schedule_minutes
        )
        
        # Schedule ETL to run periodically
        self.scheduler.add_job(
            self.run_all_sources,
            trigger=IntervalTrigger(minutes=settings.etl_schedule_minutes),
            id="etl_job",
            name="ETL All Sources",
            replace_existing=True,
        )
        
        # Run immediately on startup
        self.scheduler.add_job(
            self.run_all_sources,
            id="etl_startup",
            name="ETL Startup Run",
        )
        
        self.scheduler.start()
        logger.info("ETL scheduler started")
    
    async def run_once(self):
        """Run ETL once (for testing)."""
        await self.run_all_sources()


async def main():
    """Main entry point for ETL service."""
    logger.info("Starting Kasparro ETL Service")
    
    orchestrator = ETLOrchestrator()
    orchestrator.start_scheduler()
    
    # Keep the service running
    try:
        while True:
            await asyncio.sleep(60)
    except KeyboardInterrupt:
        logger.info("Shutting down ETL service")
        orchestrator.scheduler.shutdown()


if __name__ == "__main__":
    asyncio.run(main())
