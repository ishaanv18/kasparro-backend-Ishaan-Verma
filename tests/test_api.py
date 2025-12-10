"""Tests for API endpoints."""
import pytest
from fastapi.testclient import TestClient
from api.main import app

client = TestClient(app)


class TestHealthEndpoint:
    """Test /health endpoint."""
    
    def test_health_endpoint_returns_200(self):
        """Test health endpoint returns 200 OK."""
        response = client.get("/health")
        assert response.status_code == 200
    
    def test_health_endpoint_structure(self):
        """Test health endpoint response structure."""
        response = client.get("/health")
        data = response.json()
        
        assert "status" in data
        assert "database" in data
        assert "etl" in data
        
        # Check database info
        assert "connected" in data["database"]
        assert "latency_ms" in data["database"]
        
        # Check ETL info
        assert "last_run" in data["etl"]
        assert "status" in data["etl"]
        assert "records_processed" in data["etl"]


class TestDataEndpoint:
    """Test /data endpoint."""
    
    def test_data_endpoint_returns_200(self):
        """Test data endpoint returns 200 OK."""
        response = client.get("/data")
        assert response.status_code == 200
    
    def test_data_endpoint_structure(self):
        """Test data endpoint response structure."""
        response = client.get("/data")
        data = response.json()
        
        assert "request_id" in data
        assert "api_latency_ms" in data
        assert "data" in data
        assert "pagination" in data
        
        # Check pagination structure
        pagination = data["pagination"]
        assert "page" in pagination
        assert "page_size" in pagination
        assert "total_records" in pagination
        assert "total_pages" in pagination
    
    def test_data_endpoint_pagination(self):
        """Test data endpoint pagination."""
        response = client.get("/data?page=1&page_size=10")
        assert response.status_code == 200
        
        data = response.json()
        assert data["pagination"]["page"] == 1
        assert data["pagination"]["page_size"] == 10
    
    def test_data_endpoint_filtering_by_source(self):
        """Test data endpoint filtering by source."""
        response = client.get("/data?source=coinpaprika")
        assert response.status_code == 200
        
        data = response.json()
        # All returned data should be from coinpaprika
        for item in data["data"]:
            if item:  # If there's data
                assert item["source"] == "coinpaprika"
    
    def test_data_endpoint_filtering_by_symbol(self):
        """Test data endpoint filtering by symbol."""
        response = client.get("/data?symbol=BTC")
        assert response.status_code == 200
        
        data = response.json()
        # All returned data should be for BTC
        for item in data["data"]:
            if item:  # If there's data
                assert item["symbol"].upper() == "BTC"
    
    def test_data_endpoint_invalid_page(self):
        """Test data endpoint with invalid page number."""
        response = client.get("/data?page=0")
        assert response.status_code == 422  # Validation error
    
    def test_data_endpoint_large_page_size(self):
        """Test data endpoint with page size exceeding limit."""
        response = client.get("/data?page_size=2000")
        assert response.status_code == 422  # Validation error


class TestStatsEndpoint:
    """Test /stats endpoint."""
    
    def test_stats_endpoint_returns_200(self):
        """Test stats endpoint returns 200 OK."""
        response = client.get("/stats")
        assert response.status_code == 200
    
    def test_stats_endpoint_structure(self):
        """Test stats endpoint response structure."""
        response = client.get("/stats")
        data = response.json()
        
        assert "total_runs" in data
        assert "last_success" in data
        assert "last_failure" in data
        assert "total_records_processed" in data
        assert "average_duration_seconds" in data
        assert "sources" in data
        
        # Check sources structure
        sources = data["sources"]
        assert isinstance(sources, dict)


class TestRootEndpoint:
    """Test root endpoint."""
    
    def test_root_endpoint(self):
        """Test root endpoint returns service info."""
        response = client.get("/")
        assert response.status_code == 200
        
        data = response.json()
        assert "service" in data
        assert "version" in data
        assert "status" in data


class TestRequestMetadata:
    """Test request metadata."""
    
    def test_request_id_in_headers(self):
        """Test that request ID is included in response headers."""
        response = client.get("/data")
        assert "X-Request-ID" in response.headers
    
    def test_latency_in_headers(self):
        """Test that latency is included in response headers."""
        response = client.get("/data")
        assert "X-Latency-MS" in response.headers
    
    def test_latency_in_response_body(self):
        """Test that latency is included in response body for /data."""
        response = client.get("/data")
        data = response.json()
        assert "api_latency_ms" in data
        assert isinstance(data["api_latency_ms"], (int, float))


class TestErrorHandling:
    """Test error handling."""
    
    def test_nonexistent_endpoint(self):
        """Test accessing nonexistent endpoint."""
        response = client.get("/nonexistent")
        assert response.status_code == 404
    
    def test_invalid_query_parameters(self):
        """Test invalid query parameters."""
        response = client.get("/data?page=invalid")
        assert response.status_code == 422
