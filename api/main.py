"""FastAPI application setup and configuration.

Main API entry point for the Kasparro backend.
Started with Flask but switched to FastAPI for better async support.
"""
from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from contextlib import asynccontextmanager
import time
import uuid
from core.config import settings
from core.logging import setup_logging, get_logger
from services.database import async_engine

# Setup logging
setup_logging()
logger = get_logger(__name__)

# TODO: Add authentication middleware for production


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Application lifespan manager."""
    logger.info("Starting Kasparro Backend API", environment=settings.environment)
    yield
    logger.info("Shutting down Kasparro Backend API")
    await async_engine.dispose()


# Create FastAPI app
app = FastAPI(
    title="Kasparro Backend & ETL System",
    description="Production-grade cryptocurrency data ingestion and API service",
    version="1.0.0",
    lifespan=lifespan,
)

# CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# Request tracking middleware
@app.middleware("http")
async def add_request_metadata(request: Request, call_next):
    """Add request ID and track latency."""
    request_id = str(uuid.uuid4())
    request.state.request_id = request_id
    
    start_time = time.time()
    response = await call_next(request)
    latency_ms = (time.time() - start_time) * 1000
    
    response.headers["X-Request-ID"] = request_id
    response.headers["X-Latency-MS"] = f"{latency_ms:.2f}"
    
    logger.info(
        "Request completed",
        request_id=request_id,
        method=request.method,
        path=request.url.path,
        latency_ms=f"{latency_ms:.2f}",
        status_code=response.status_code,
    )
    
    return response


# Exception handlers
@app.exception_handler(Exception)
async def global_exception_handler(request: Request, exc: Exception):
    """Handle uncaught exceptions."""
    request_id = getattr(request.state, "request_id", "unknown")
    logger.error(
        "Unhandled exception",
        request_id=request_id,
        error=str(exc),
        path=request.url.path,
    )
    return JSONResponse(
        status_code=500,
        content={
            "error": "Internal server error",
            "request_id": request_id,
        },
    )


# Import and register routes
from api.routes import data, health, stats, metrics, runs

app.include_router(data.router, tags=["Data"])
app.include_router(health.router, tags=["Health"])
app.include_router(stats.router, tags=["Statistics"])
app.include_router(metrics.router, tags=["Metrics"])
app.include_router(runs.router, tags=["Runs & Anomalies"])


@app.get("/")
async def root():
    """Root endpoint."""
    return {
        "service": "Kasparro Backend & ETL System",
        "version": "1.0.0",
        "status": "running",
        "docs": "/docs",
    }
