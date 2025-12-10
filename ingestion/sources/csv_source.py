"""CSV data source."""
import pandas as pd
from typing import List, Dict, Any
from datetime import datetime
import json
from pathlib import Path
from ingestion.sources.base import BaseDataSource
from schemas.raw import RawCSVData
from services.normalization import NormalizationService
from services.database import get_sync_connection
from core.config import settings
from sqlalchemy import text


class CSVSource(BaseDataSource):
    """CSV file data source."""
    
    def __init__(self, csv_path: str = None):
        super().__init__("csv")
        self.csv_path = csv_path or settings.csv_data_path
    
    async def fetch(self) -> List[Dict[str, Any]]:
        """
        Fetch data from CSV file.
        
        Returns:
            List of cryptocurrency data dictionaries
        """
        self.logger.info("Reading data from CSV", path=self.csv_path)
        
        try:
            # Check if file exists
            if not Path(self.csv_path).exists():
                self.logger.warning("CSV file not found", path=self.csv_path)
                return []
            
            # Read CSV file
            df = pd.read_csv(self.csv_path)
            
            # Get last processed row number from checkpoint
            last_row = self.checkpoint_manager.get_last_row_number()
            
            # Only process new rows (incremental)
            if last_row > 0:
                df = df.iloc[last_row:]
                self.logger.info(
                    "Processing incremental CSV data",
                    last_row=last_row,
                    new_rows=len(df)
                )
            
            # Convert to list of dictionaries
            data = df.to_dict('records')
            
            self.logger.info("Read data from CSV", count=len(data))
            return data
        
        except Exception as e:
            self.logger.error("Error reading CSV file", error=str(e), path=self.csv_path)
            raise
    
    def validate(self, data: Dict[str, Any]) -> bool:
        """Validate CSV data using Pydantic."""
        try:
            RawCSVData(**data)
            return True
        except Exception as e:
            self.logger.warning(
                "Validation failed for CSV data",
                error=str(e),
                data=data
            )
            return False
    
    def save_raw(self, data: List[Dict[str, Any]]) -> int:
        """Save raw CSV data to database."""
        if not data:
            return 0
        
        saved_count = 0
        current_timestamp = datetime.utcnow()
        last_row_number = self.checkpoint_manager.get_last_row_number()
        
        try:
            with get_sync_connection() as conn:
                for idx, item in enumerate(data):
                    if not self.validate(item):
                        continue
                    
                    row_number = last_row_number + idx + 1
                    
                    try:
                        conn.execute(
                            text("""
                                INSERT INTO raw_csv (
                                    symbol, name,
                                    price_usd, market_cap_usd, volume_24h_usd,
                                    percent_change_24h,
                                    raw_data, data_timestamp,
                                    source_file, row_number
                                ) VALUES (
                                    :symbol, :name,
                                    :price_usd, :market_cap_usd, :volume_24h_usd,
                                    :percent_change_24h,
                                    :raw_data, :data_timestamp,
                                    :source_file, :row_number
                                )
                                ON CONFLICT (source_file, row_number) DO NOTHING
                            """),
                            {
                                "symbol": item.get("symbol"),
                                "name": item.get("name"),
                                "price_usd": item.get("price_usd"),
                                "market_cap_usd": item.get("market_cap_usd"),
                                "volume_24h_usd": item.get("volume_24h_usd"),
                                "percent_change_24h": item.get("percent_change_24h"),
                                "raw_data": json.dumps(item),
                                "data_timestamp": current_timestamp,
                                "source_file": self.csv_path,
                                "row_number": row_number,
                            }
                        )
                        saved_count += 1
                    except Exception as e:
                        self.logger.warning(
                            "Failed to save individual CSV record",
                            error=str(e),
                            row_number=row_number
                        )
                        continue
                
                conn.commit()
                
            self.logger.info(
                "Saved raw CSV data",
                saved_count=saved_count,
                total=len(data)
            )
            return saved_count
        
        except Exception as e:
            self.logger.error("Failed to save raw CSV data", error=str(e))
            raise
    
    def normalize(self, raw_data: Dict[str, Any]) -> Dict[str, Any]:
        """Normalize CSV data."""
        normalized = NormalizationService.normalize_csv(
            raw_data,
            datetime.utcnow()
        )
        return normalized.model_dump()
