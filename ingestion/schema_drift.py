"""Schema drift detection using fuzzy matching."""
from typing import Dict, List, Any, Optional, Tuple
from fuzzywuzzy import fuzz
from datetime import datetime
from core.logging import get_logger

logger = get_logger(__name__)


class SchemaDriftDetector:
    """Detect schema changes in incoming data using fuzzy matching."""
    
    # Confidence thresholds
    HIGH_CONFIDENCE = 0.9
    MEDIUM_CONFIDENCE = 0.7
    LOW_CONFIDENCE = 0.5
    
    def __init__(self, source_name: str):
        """
        Initialize schema drift detector.
        
        Args:
            source_name: Name of the data source
        """
        self.source_name = source_name
        self.expected_schema: Optional[Dict[str, type]] = None
        self.logger = get_logger(f"{__name__}.{source_name}")
    
    def set_expected_schema(self, schema: Dict[str, type]):
        """
        Set the expected schema for comparison.
        
        Args:
            schema: Dictionary mapping field names to expected types
        """
        self.expected_schema = schema
        self.logger.info(
            "Expected schema set",
            source=self.source_name,
            fields=list(schema.keys())
        )
    
    def detect_drift(self, data: Dict[str, Any]) -> Tuple[bool, float, List[str]]:
        """
        Detect schema drift in incoming data.
        
        Args:
            data: Incoming data dictionary
        
        Returns:
            Tuple of (has_drift, confidence_score, warnings)
        """
        if not self.expected_schema:
            self.logger.warning(
                "No expected schema set for drift detection",
                source=self.source_name
            )
            return False, 1.0, []
        
        warnings = []
        has_drift = False
        
        # Check for missing fields
        expected_fields = set(self.expected_schema.keys())
        actual_fields = set(data.keys())
        
        missing_fields = expected_fields - actual_fields
        extra_fields = actual_fields - expected_fields
        
        if missing_fields:
            has_drift = True
            warnings.append(f"Missing fields: {', '.join(missing_fields)}")
            self.logger.warning(
                "Schema drift: missing fields",
                source=self.source_name,
                missing_fields=list(missing_fields)
            )
        
        if extra_fields:
            has_drift = True
            warnings.append(f"Unexpected fields: {', '.join(extra_fields)}")
            self.logger.warning(
                "Schema drift: extra fields",
                source=self.source_name,
                extra_fields=list(extra_fields)
            )
        
        # Check for type mismatches
        type_mismatches = []
        for field, expected_type in self.expected_schema.items():
            if field in data and data[field] is not None:
                actual_type = type(data[field])
                if actual_type != expected_type:
                    type_mismatches.append(
                        f"{field}: expected {expected_type.__name__}, got {actual_type.__name__}"
                    )
        
        if type_mismatches:
            has_drift = True
            warnings.extend(type_mismatches)
            self.logger.warning(
                "Schema drift: type mismatches",
                source=self.source_name,
                mismatches=type_mismatches
            )
        
        # Calculate confidence score
        confidence = self._calculate_confidence(
            expected_fields,
            actual_fields,
            type_mismatches
        )
        
        return has_drift, confidence, warnings
    
    def fuzzy_match_field(self, field_name: str, threshold: int = 80) -> Optional[str]:
        """
        Find the closest matching field name using fuzzy matching.
        
        Args:
            field_name: Field name to match
            threshold: Minimum similarity score (0-100)
        
        Returns:
            Best matching field name or None
        """
        if not self.expected_schema:
            return None
        
        best_match = None
        best_score = 0
        
        for expected_field in self.expected_schema.keys():
            score = fuzz.ratio(field_name.lower(), expected_field.lower())
            if score > best_score and score >= threshold:
                best_score = score
                best_match = expected_field
        
        if best_match and best_score < 100:
            self.logger.info(
                "Fuzzy field match found",
                source=self.source_name,
                input_field=field_name,
                matched_field=best_match,
                similarity=best_score
            )
        
        return best_match
    
    def suggest_field_mapping(self, data: Dict[str, Any]) -> Dict[str, str]:
        """
        Suggest field mappings for unknown fields using fuzzy matching.
        
        Args:
            data: Incoming data dictionary
        
        Returns:
            Dictionary mapping actual field names to suggested expected field names
        """
        if not self.expected_schema:
            return {}
        
        suggestions = {}
        actual_fields = set(data.keys())
        expected_fields = set(self.expected_schema.keys())
        
        # Find fields that don't match exactly
        unknown_fields = actual_fields - expected_fields
        
        for field in unknown_fields:
            match = self.fuzzy_match_field(field)
            if match:
                suggestions[field] = match
        
        if suggestions:
            self.logger.info(
                "Field mapping suggestions",
                source=self.source_name,
                suggestions=suggestions
            )
        
        return suggestions
    
    def _calculate_confidence(
        self,
        expected_fields: set,
        actual_fields: set,
        type_mismatches: List[str]
    ) -> float:
        """
        Calculate confidence score for schema match.
        
        Args:
            expected_fields: Set of expected field names
            actual_fields: Set of actual field names
            type_mismatches: List of type mismatch descriptions
        
        Returns:
            Confidence score between 0.0 and 1.0
        """
        if not expected_fields:
            return 1.0
        
        # Calculate field overlap
        matching_fields = expected_fields & actual_fields
        field_match_ratio = len(matching_fields) / len(expected_fields)
        
        # Penalize for type mismatches
        type_penalty = len(type_mismatches) * 0.1
        
        # Calculate final confidence
        confidence = max(0.0, field_match_ratio - type_penalty)
        
        return confidence
    
    def log_drift_summary(self, has_drift: bool, confidence: float, warnings: List[str]):
        """
        Log a summary of drift detection results.
        
        Args:
            has_drift: Whether drift was detected
            confidence: Confidence score
            warnings: List of warning messages
        """
        if has_drift:
            if confidence >= self.HIGH_CONFIDENCE:
                level = "minor"
            elif confidence >= self.MEDIUM_CONFIDENCE:
                level = "moderate"
            else:
                level = "severe"
            
            self.logger.warning(
                f"Schema drift detected ({level})",
                source=self.source_name,
                confidence=confidence,
                warnings=warnings
            )
        else:
            self.logger.debug(
                "No schema drift detected",
                source=self.source_name,
                confidence=confidence
            )


# Define expected schemas for each source
COINPAPRIKA_SCHEMA = {
    "coin_id": str,
    "symbol": str,
    "name": str,
    "rank": int,
    "price_usd": (float, type(None)),
    "volume_24h_usd": (float, type(None)),
    "market_cap_usd": (float, type(None)),
}

COINGECKO_SCHEMA = {
    "coin_id": str,
    "symbol": str,
    "name": str,
    "current_price": (float, type(None)),
    "market_cap": (float, type(None)),
    "total_volume": (float, type(None)),
}

CSV_SCHEMA = {
    "symbol": str,
    "name": str,
    "price_usd": (float, type(None)),
    "market_cap_usd": (float, type(None)),
    "volume_24h_usd": (float, type(None)),
}
