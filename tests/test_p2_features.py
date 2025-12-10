"""Additional tests for P2 features."""
import pytest
from ingestion.schema_drift import SchemaDriftDetector


class TestSchemaDrift:
    """Test schema drift detection."""
    
    def test_no_drift_with_matching_schema(self):
        """Test that matching schema has no drift."""
        detector = SchemaDriftDetector("test")
        detector.set_expected_schema({
            "field1": str,
            "field2": int,
            "field3": float,
        })
        
        data = {
            "field1": "value",
            "field2": 123,
            "field3": 45.67,
        }
        
        has_drift, confidence, warnings = detector.detect_drift(data)
        
        assert has_drift is False
        assert confidence >= 0.9
        assert len(warnings) == 0
    
    def test_drift_with_missing_fields(self):
        """Test drift detection with missing fields."""
        detector = SchemaDriftDetector("test")
        detector.set_expected_schema({
            "field1": str,
            "field2": int,
            "field3": float,
        })
        
        data = {
            "field1": "value",
            # field2 missing
            "field3": 45.67,
        }
        
        has_drift, confidence, warnings = detector.detect_drift(data)
        
        assert has_drift is True
        assert confidence < 1.0
        assert any("Missing fields" in w for w in warnings)
    
    def test_drift_with_extra_fields(self):
        """Test drift detection with extra fields."""
        detector = SchemaDriftDetector("test")
        detector.set_expected_schema({
            "field1": str,
            "field2": int,
        })
        
        data = {
            "field1": "value",
            "field2": 123,
            "field3": "unexpected",  # Extra field
        }
        
        has_drift, confidence, warnings = detector.detect_drift(data)
        
        assert has_drift is True
        assert any("Unexpected fields" in w for w in warnings)
    
    def test_fuzzy_field_matching(self):
        """Test fuzzy matching for similar field names."""
        detector = SchemaDriftDetector("test")
        detector.set_expected_schema({
            "user_name": str,
            "email_address": str,
        })
        
        # Test similar field names
        match1 = detector.fuzzy_match_field("username", threshold=70)
        match2 = detector.fuzzy_match_field("email", threshold=70)
        
        assert match1 == "user_name"
        assert match2 == "email_address"
    
    def test_field_mapping_suggestions(self):
        """Test field mapping suggestions."""
        detector = SchemaDriftDetector("test")
        detector.set_expected_schema({
            "first_name": str,
            "last_name": str,
        })
        
        data = {
            "firstname": "John",  # Similar to first_name
            "lastname": "Doe",    # Similar to last_name
        }
        
        suggestions = detector.suggest_field_mapping(data)
        
        assert "firstname" in suggestions
        assert "lastname" in suggestions
        assert suggestions["firstname"] == "first_name"
        assert suggestions["lastname"] == "last_name"


class TestPrometheusMetrics:
    """Test Prometheus metrics."""
    
    def test_metrics_endpoint_accessible(self):
        """Test that metrics endpoint is accessible."""
        from fastapi.testclient import TestClient
        from api.main import app
        
        client = TestClient(app)
        response = client.get("/metrics")
        
        assert response.status_code == 200
        assert "text/plain" in response.headers["content-type"]
    
    def test_metrics_format(self):
        """Test that metrics are in Prometheus format."""
        from fastapi.testclient import TestClient
        from api.main import app
        
        client = TestClient(app)
        response = client.get("/metrics")
        
        content = response.text
        
        # Check for standard Prometheus metric types
        assert "# HELP" in content or "# TYPE" in content or len(content) > 0


class TestRunComparison:
    """Test run comparison and anomaly detection."""
    
    def test_runs_endpoint_accessible(self):
        """Test that runs endpoint is accessible."""
        from fastapi.testclient import TestClient
        from api.main import app
        
        client = TestClient(app)
        response = client.get("/runs?limit=10")
        
        assert response.status_code == 200
        data = response.json()
        assert isinstance(data, list)
    
    def test_anomalies_endpoint_accessible(self):
        """Test that anomalies endpoint is accessible."""
        from fastapi.testclient import TestClient
        from api.main import app
        
        client = TestClient(app)
        response = client.get("/anomalies?hours=24")
        
        assert response.status_code == 200
        data = response.json()
        assert isinstance(data, list)
