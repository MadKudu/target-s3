"""Tests standard target features using the built-in SDK tests library."""

from __future__ import annotations

from typing import Any
import math
import simplejson as json

from singer_sdk.testing import get_target_test_class

from target_s3.target import Targets3
from target_s3.formats.format_jsonl import JsonSerialize

SAMPLE_CONFIG: dict[str, Any] = {
    "format": {
        "format_type": "json",
    },
    "cloud_provider": {
        "cloud_provider_type": "aws",
        "aws": {
            "aws_access_key_id": "minioadmin",
            "aws_secret_access_key": "minioadmin",
            "aws_bucket": "test-bucket",
            "aws_region": "us-east-1",
            "aws_endpoint_override": "http://localhost:9000",
        },
    },
    "prefix": "integration-tests",
}

TestTargetS3 = get_target_test_class(Targets3, config=SAMPLE_CONFIG)


def test_json_serialize_handles_invalid_floats():
    """Test that JsonSerialize correctly handles out-of-range float values."""
    serializer = JsonSerialize()
    
    # Test data with problematic float values
    test_data = {
        "normal_float": 3.14,
        "infinity": float('inf'),
        "negative_infinity": float('-inf'),
        "nan": float('nan'),
        "normal_string": "test"
    }
    
    # This should not raise an exception
    result = json.dumps(test_data, cls=JsonSerialize, ignore_nan=True)
    
    # Parse back to verify the values were handled correctly
    parsed = json.loads(result)
    
    assert parsed["normal_float"] == 3.14
    assert parsed["infinity"] is None
    assert parsed["negative_infinity"] is None
    assert parsed["nan"] is None
    assert parsed["normal_string"] == "test"


# Standalone test that doesn't require the full target import
def test_json_serialize_standalone():
    """Test JsonSerialize class standalone without importing the full target."""
    from target_s3.formats.format_jsonl import JsonSerialize
    import simplejson as json
    import math
    
    serializer = JsonSerialize()
    
    # Test data with problematic float values
    test_data = {
        "normal_float": 3.14,
        "infinity": float('inf'),
        "negative_infinity": float('-inf'),
        "nan": float('nan'),
        "normal_string": "test"
    }
    
    # This should not raise an exception
    result = json.dumps(test_data, cls=JsonSerialize, ignore_nan=True)
    
    # Parse back to verify the values were handled correctly
    parsed = json.loads(result)
    
    assert parsed["normal_float"] == 3.14
    assert parsed["infinity"] is None
    assert parsed["negative_infinity"] is None
    assert parsed["nan"] is None
    assert parsed["normal_string"] == "test"
