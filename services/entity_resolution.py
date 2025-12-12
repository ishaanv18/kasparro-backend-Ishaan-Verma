"""Entity resolution service to unify cryptocurrency data across sources.

This service implements fuzzy matching and entity resolution to map
source-specific coin identifiers to canonical master coin entities.
"""
from typing import Dict, Any, Optional, Tuple
from difflib import SequenceMatcher
from sqlalchemy import text
from services.database import get_sync_connection
from core.logging import get_logger

logger = get_logger(__name__)


class EntityResolutionService:
    """Service to resolve cryptocurrency entities across different data sources."""
    
    # Cache for master coin lookups
    _master_coin_cache: Dict[str, int] = {}
    _source_mapping_cache: Dict[Tuple[str, str], int] = {}
    
    @classmethod
    def clear_cache(cls):
        """Clear the entity resolution cache."""
        cls._master_coin_cache.clear()
        cls._source_mapping_cache.clear()
    
    @classmethod
    def get_or_create_master_coin(
        cls,
        symbol: str,
        name: str,
        source: str,
        source_id: str
    ) -> int:
        """
        Get or create a master coin entity.
        
        Args:
            symbol: Coin symbol (e.g., 'BTC')
            name: Coin name (e.g., 'Bitcoin')
            source: Data source name
            source_id: Source-specific identifier
            
        Returns:
            Master coin ID
        """
        # Check source mapping cache first
        cache_key = (source, source_id)
        if cache_key in cls._source_mapping_cache:
            return cls._source_mapping_cache[cache_key]
        
        with get_sync_connection() as conn:
            # Check if source mapping already exists
            result = conn.execute(
                text("""
                    SELECT master_coin_id 
                    FROM coin_source_mappings 
                    WHERE source = :source AND source_id = :source_id
                """),
                {"source": source, "source_id": source_id}
            ).fetchone()
            
            if result:
                master_coin_id = result[0]
                cls._source_mapping_cache[cache_key] = master_coin_id
                return master_coin_id
            
            # Try to find existing master coin by symbol
            master_coin_id = cls._find_master_coin_by_symbol(symbol, name, conn)
            
            if not master_coin_id:
                # Create new master coin
                master_coin_id = cls._create_master_coin(symbol, name, conn)
            
            # Create source mapping
            conn.execute(
                text("""
                    INSERT INTO coin_source_mappings (master_coin_id, source, source_id)
                    VALUES (:master_coin_id, :source, :source_id)
                    ON CONFLICT (source, source_id) DO NOTHING
                """),
                {
                    "master_coin_id": master_coin_id,
                    "source": source,
                    "source_id": source_id
                }
            )
            conn.commit()
            
            # Update cache
            cls._source_mapping_cache[cache_key] = master_coin_id
            return master_coin_id
    
    @classmethod
    def _find_master_coin_by_symbol(
        cls,
        symbol: str,
        name: str,
        conn
    ) -> Optional[int]:
        """
        Find master coin by symbol with fuzzy name matching.
        
        Args:
            symbol: Coin symbol
            name: Coin name
            conn: Database connection
            
        Returns:
            Master coin ID if found, None otherwise
        """
        # Exact symbol match
        result = conn.execute(
            text("SELECT id, name FROM master_coins WHERE symbol = :symbol"),
            {"symbol": symbol.upper()}
        ).fetchone()
        
        if result:
            master_coin_id, existing_name = result
            # Verify name similarity to avoid false matches
            similarity = cls._calculate_similarity(name.lower(), existing_name.lower())
            
            if similarity > 0.7:  # 70% similarity threshold
                logger.info(
                    "Matched existing master coin",
                    symbol=symbol,
                    name=name,
                    existing_name=existing_name,
                    similarity=similarity
                )
                return master_coin_id
            else:
                logger.warning(
                    "Symbol match but name mismatch",
                    symbol=symbol,
                    name=name,
                    existing_name=existing_name,
                    similarity=similarity
                )
        
        return None
    
    @classmethod
    def _create_master_coin(cls, symbol: str, name: str, conn) -> int:
        """
        Create a new master coin entity.
        
        Args:
            symbol: Coin symbol
            name: Coin name
            conn: Database connection
            
        Returns:
            New master coin ID
        """
        # Generate canonical ID from name
        canonical_id = name.lower().replace(' ', '-').replace('.', '')
        
        result = conn.execute(
            text("""
                INSERT INTO master_coins (symbol, name, canonical_id)
                VALUES (:symbol, :name, :canonical_id)
                ON CONFLICT (symbol) DO UPDATE 
                SET name = EXCLUDED.name, updated_at = CURRENT_TIMESTAMP
                RETURNING id
            """),
            {
                "symbol": symbol.upper(),
                "name": name,
                "canonical_id": canonical_id
            }
        ).fetchone()
        
        master_coin_id = result[0]
        
        logger.info(
            "Created new master coin",
            master_coin_id=master_coin_id,
            symbol=symbol,
            name=name,
            canonical_id=canonical_id
        )
        
        return master_coin_id
    
    @staticmethod
    def _calculate_similarity(str1: str, str2: str) -> float:
        """
        Calculate similarity ratio between two strings.
        
        Args:
            str1: First string
            str2: Second string
            
        Returns:
            Similarity ratio (0.0 to 1.0)
        """
        return SequenceMatcher(None, str1, str2).ratio()
    
    @classmethod
    def resolve_entity(
        cls,
        source: str,
        source_id: str,
        symbol: str,
        name: str
    ) -> int:
        """
        Resolve an entity to its master coin ID.
        
        This is the main entry point for entity resolution.
        
        Args:
            source: Data source name
            source_id: Source-specific identifier
            symbol: Coin symbol
            name: Coin name
            
        Returns:
            Master coin ID
        """
        return cls.get_or_create_master_coin(symbol, name, source, source_id)
