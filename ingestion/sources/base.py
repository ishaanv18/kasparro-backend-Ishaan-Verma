"""Base class for all data sources."""
from abc import ABC, abstractmethod
from typing import List, Dict, Any, Optional
from datetime import datetime
from ingestion.checkpoint import CheckpointManager
from core.logging import get_logger

logger = get_logger(__name__)


class BaseDataSource(ABC):
    """Abstract base class for all data sources."""
    
    def __init__(self, source_name: str):
        """
        Initialize data source.
        
        Args:
            source_name: Unique identifier for this source
        """
        self.source_name = source_name
        self.checkpoint_manager = CheckpointManager(source_name)
        self.logger = get_logger(f"{__name__}.{source_name}")
    
    @abstractmethod
    async def fetch(self) -> List[Dict[str, Any]]:
        """
        Fetch data from the source.
        
        Returns:
            List of raw data dictionaries
        """
        pass
    
    @abstractmethod
    def validate(self, data: Dict[str, Any]) -> bool:
        """
        Validate a single data record.
        
        Args:
            data: Raw data dictionary
        
        Returns:
            True if valid, False otherwise
        """
        pass
    
    @abstractmethod
    def save_raw(self, data: List[Dict[str, Any]]) -> int:
        """
        Save raw data to database.
        
        Args:
            data: List of raw data dictionaries
        
        Returns:
            Number of records saved
        """
        pass
    
    @abstractmethod
    def normalize(self, raw_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Normalize raw data to unified schema.
        
        Args:
            raw_data: Raw data dictionary
        
        Returns:
            Normalized data dictionary
        """
        pass
    
    def get_checkpoint(self) -> Optional[Dict[str, Any]]:
        """Get current checkpoint."""
        return self.checkpoint_manager.get_checkpoint()
    
    def update_checkpoint(
        self,
        checkpoint_value: str,
        success: bool = True,
        error_message: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None
    ):
        """Update checkpoint."""
        self.checkpoint_manager.update_checkpoint(
            checkpoint_value=checkpoint_value,
            success=success,
            error_message=error_message,
            metadata=metadata
        )
