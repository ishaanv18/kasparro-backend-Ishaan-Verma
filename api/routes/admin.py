"""Admin endpoints for database migrations and maintenance."""
from fastapi import APIRouter, HTTPException, Header
from sqlalchemy import text
from services.database import get_sync_connection
from core.logging import get_logger
import os

router = APIRouter(prefix="/admin", tags=["admin"])
logger = get_logger(__name__)

# Simple secret for migration endpoint (set via environment variable)
MIGRATION_SECRET = os.getenv("MIGRATION_SECRET", "kasparro-migrate-2024")


@router.post("/migrate")
async def run_migrations(x_migration_secret: str = Header(None)):
    """
    Run database migrations.
    
    This endpoint is for initial setup only. Requires MIGRATION_SECRET header.
    """
    if x_migration_secret != MIGRATION_SECRET:
        raise HTTPException(status_code=403, detail="Invalid migration secret")
    
    results = {
        "init_migration": {"status": "pending", "error": None},
        "entity_resolution_migration": {"status": "pending", "error": None}
    }
    
    # Read migration files
    try:
        with open("migrations/init.sql", "r") as f:
            init_sql = f.read()
        
        with open("migrations/add_master_coins.sql", "r") as f:
            entity_sql = f.read()
    except Exception as e:
        logger.error("Failed to read migration files", error=str(e))
        raise HTTPException(status_code=500, detail=f"Failed to read migration files: {str(e)}")
    
    # Run init migration
    try:
        with get_sync_connection() as conn:
            # Execute init migration
            conn.execute(text(init_sql))
            conn.commit()
            results["init_migration"]["status"] = "success"
            logger.info("Init migration completed successfully")
    except Exception as e:
        logger.error("Init migration failed", error=str(e))
        results["init_migration"]["status"] = "failed"
        results["init_migration"]["error"] = str(e)
    
    # Run entity resolution migration
    try:
        with get_sync_connection() as conn:
            # Execute entity resolution migration
            conn.execute(text(entity_sql))
            conn.commit()
            results["entity_resolution_migration"]["status"] = "success"
            logger.info("Entity resolution migration completed successfully")
    except Exception as e:
        logger.error("Entity resolution migration failed", error=str(e))
        results["entity_resolution_migration"]["status"] = "failed"
        results["entity_resolution_migration"]["error"] = str(e)
    
    return {
        "message": "Migrations completed",
        "results": results
    }


@router.get("/health-detailed")
async def detailed_health():
    """Get detailed health information including table counts."""
    try:
        with get_sync_connection() as conn:
            # Check tables exist
            tables = conn.execute(text("""
                SELECT table_name 
                FROM information_schema.tables 
                WHERE table_schema = 'public'
                ORDER BY table_name
            """)).fetchall()
            
            # Get counts
            counts = {}
            for table in tables:
                table_name = table[0]
                try:
                    count = conn.execute(text(f"SELECT COUNT(*) FROM {table_name}")).fetchone()[0]
                    counts[table_name] = count
                except:
                    counts[table_name] = "error"
            
            return {
                "status": "healthy",
                "tables": [t[0] for t in tables],
                "record_counts": counts
            }
    except Exception as e:
        logger.error("Health check failed", error=str(e))
        return {
            "status": "unhealthy",
            "error": str(e)
        }
