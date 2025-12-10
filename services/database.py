"""Database connection and query utilities."""
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession, async_sessionmaker
from sqlalchemy.orm import declarative_base
from sqlalchemy import create_engine, text
from contextlib import asynccontextmanager
from typing import AsyncGenerator
from core.config import settings
from core.logging import get_logger

logger = get_logger(__name__)

# SQLAlchemy Base
Base = declarative_base()

# Async engine for API
async_engine = create_async_engine(
    settings.database_url,
    echo=settings.environment == "development",
    pool_size=10,
    max_overflow=20,
    pool_pre_ping=True,
)

# Async session factory
AsyncSessionLocal = async_sessionmaker(
    async_engine,
    class_=AsyncSession,
    expire_on_commit=False,
)

# Sync engine for ETL (simpler for batch operations)
sync_engine = create_engine(
    settings.database_url_sync,
    echo=settings.environment == "development",
    pool_size=5,
    max_overflow=10,
    pool_pre_ping=True,
)


@asynccontextmanager
async def get_async_session() -> AsyncGenerator[AsyncSession, None]:
    """Get async database session."""
    async with AsyncSessionLocal() as session:
        try:
            yield session
            await session.commit()
        except Exception as e:
            await session.rollback()
            logger.error("Database session error", error=str(e))
            raise
        finally:
            await session.close()


async def check_db_connection() -> tuple[bool, float]:
    """
    Check database connectivity.
    
    Returns:
        Tuple of (is_connected, latency_ms)
    """
    import time
    try:
        start = time.time()
        async with get_async_session() as session:
            await session.execute(text("SELECT 1"))
        latency_ms = (time.time() - start) * 1000
        return True, latency_ms
    except Exception as e:
        logger.error("Database connection check failed", error=str(e))
        return False, 0.0


def get_sync_connection():
    """Get synchronous database connection for ETL operations."""
    return sync_engine.connect()
