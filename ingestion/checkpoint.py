"""Checkpoint management for incremental ETL ingestion."""
from typing import Optional, Dict, Any
from datetime import datetime
from sqlalchemy import text
from services.database import get_sync_connection
from core.logging import get_logger

logger = get_logger(__name__)


class CheckpointManager:
    """Manage ETL checkpoints for resume-on-failure capability."""
    
    def __init__(self, source_name: str):
        """
        Initialize checkpoint manager for a specific source.
        
        Args:
            source_name: Name of the data source (e.g., 'coinpaprika', 'coingecko', 'csv')
        """
        self.source_name = source_name
    
    def get_checkpoint(self) -> Optional[Dict[str, Any]]:
        """Get the current checkpoint for this source."""
        try:
            with get_sync_connection() as conn:
                result = conn.execute(
                    text("""
                        SELECT 
                            checkpoint_value,
                            last_success_at,
                            last_failure_at,
                            failure_reason,
                            metadata
                        FROM etl_checkpoints
                        WHERE source_name = :source_name
                    """),
                    {"source_name": self.source_name}
                )
                row = result.fetchone()
                
                if row:
                    # Debug: print checkpoint info
                    # print(f"Checkpoint for {self.source_name}: {row[0]}")
                    return {
                        "checkpoint_value": row[0],
                        "last_success_at": row[1],
                        "last_failure_at": row[2],
                        "failure_reason": row[3],
                        "metadata": row[4] or {},
                    }
                return None
        except Exception as e:
            logger.error(
                "Failed to get checkpoint",
                source=self.source_name,
                error=str(e)
            )
            return None
    
    def update_checkpoint(
        self,
        checkpoint_value: str,
        success: bool = True,
        error_message: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None
    ):
        """
        Update checkpoint after ETL run.
        
        Args:
            checkpoint_value: New checkpoint value
            success: Whether the run was successful
            error_message: Error message if failed
            metadata: Additional metadata to store
        """
        try:
            with get_sync_connection() as conn:
                if success:
                    conn.execute(
                        text("""
                            UPDATE etl_checkpoints
                            SET 
                                checkpoint_value = :checkpoint_value,
                                last_success_at = :now,
                                metadata = :metadata,
                                updated_at = :now
                            WHERE source_name = :source_name
                        """),
                        {
                            "source_name": self.source_name,
                            "checkpoint_value": checkpoint_value,
                            "metadata": metadata,
                            "now": datetime.utcnow(),
                        }
                    )
                    conn.commit()
                    logger.info(
                        "Checkpoint updated",
                        source=self.source_name,
                        checkpoint_value=checkpoint_value
                    )
                else:
                    conn.execute(
                        text("""
                            UPDATE etl_checkpoints
                            SET 
                                last_failure_at = :now,
                                failure_reason = :error_message,
                                updated_at = :now
                            WHERE source_name = :source_name
                        """),
                        {
                            "source_name": self.source_name,
                            "error_message": error_message,
                            "now": datetime.utcnow(),
                        }
                    )
                    conn.commit()
                    logger.error(
                        "Checkpoint failure recorded",
                        source=self.source_name,
                        error=error_message
                    )
        except Exception as e:
            logger.error(
                "Failed to update checkpoint",
                source=self.source_name,
                error=str(e)
            )
            raise
    
    def get_last_timestamp(self) -> Optional[datetime]:
        """
        Get the last successful timestamp checkpoint.
        
        Returns:
            Datetime of last checkpoint or None
        """
        checkpoint = self.get_checkpoint()
        if checkpoint and checkpoint["type"] == "timestamp":
            try:
                return datetime.fromisoformat(checkpoint["value"].replace("Z", "+00:00"))
            except Exception as e:
                logger.error(
                    "Failed to parse timestamp checkpoint",
                    source=self.source_name,
                    value=checkpoint["value"],
                    error=str(e)
                )
        return None
    
    def get_last_row_number(self) -> int:
        """
        Get the last successful row number checkpoint.
        
        Returns:
            Last processed row number or 0
        """
        checkpoint = self.get_checkpoint()
        if checkpoint and checkpoint["type"] == "row_number":
            try:
                return int(checkpoint["value"])
            except Exception as e:
                logger.error(
                    "Failed to parse row number checkpoint",
                    source=self.source_name,
                    value=checkpoint["value"],
                    error=str(e)
                )
        return 0
