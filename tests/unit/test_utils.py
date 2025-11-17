"""
Unit tests for core utility functions in main.py.

Tests the helper functions used throughout the pipeline for data
sanitization, validation, and type conversion.
"""

import pytest
import base64
from datetime import datetime, date
from apache_beam.options.value_provider import StaticValueProvider

# Import functions to test - in a real scenario these would be imported from main
# For testing purposes, we'll assume they're available in the path
import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '../..'))

from main import (
    safe_get_param,
    sanitize_for_bigquery_json,
    parse_postgres_array_string,
    validate_json_for_bigquery,
)


class TestSafeGetParam:
    """Test suite for safe_get_param function."""

    def test_none_value(self):
        """Test that None returns None."""
        result = safe_get_param(None)
        assert result is None

    def test_direct_string(self):
        """Test that direct string values are returned as-is."""
        result = safe_get_param("test_value")
        assert result == "test_value"

    def test_direct_integer(self):
        """Test that direct integer values are returned as-is."""
        result = safe_get_param(42)
        assert result == 42

    def test_value_provider(self):
        """Test that ValueProvider objects are resolved correctly."""
        provider = StaticValueProvider(str, "provided_value")
        result = safe_get_param(provider)
        assert result == "provided_value"

    def test_value_provider_with_integer(self):
        """Test ValueProvider with integer value."""
        provider = StaticValueProvider(int, 123)
        result = safe_get_param(provider)
        assert result == 123

    def test_empty_string(self):
        """Test that empty strings are handled correctly."""
        result = safe_get_param("")
        assert result == ""


class TestSanitizeForBigQueryJson:
    """Test suite for sanitize_for_bigquery_json function."""

    def test_none_value(self):
        """Test that None is preserved."""
        result = sanitize_for_bigquery_json(None)
        assert result is None

    def test_simple_string(self):
        """Test that simple strings are preserved."""
        result = sanitize_for_bigquery_json("test")
        assert result == "test"

    def test_string_with_null_bytes(self):
        """Test that null bytes are removed from strings."""
        result = sanitize_for_bigquery_json("test\x00data")
        assert result == "testdata"
        assert "\x00" not in result

    def test_integer_value(self):
        """Test that integers are preserved."""
        result = sanitize_for_bigquery_json(42)
        assert result == 42

    def test_float_value(self):
        """Test that floats are preserved."""
        result = sanitize_for_bigquery_json(3.14)
        assert result == 3.14

    def test_boolean_value(self):
        """Test that booleans are preserved."""
        assert sanitize_for_bigquery_json(True) is True
        assert sanitize_for_bigquery_json(False) is False

    def test_bytes_to_base64(self):
        """Test that bytes are converted to base64 strings."""
        result = sanitize_for_bigquery_json(b"binary data")
        expected = base64.b64encode(b"binary data").decode('ascii')
        assert result == expected

    def test_datetime_to_isoformat(self):
        """Test that datetime objects are converted to ISO format."""
        dt = datetime(2024, 1, 15, 10, 30, 0)
        result = sanitize_for_bigquery_json(dt)
        assert result == "2024-01-15T10:30:00"

    def test_date_to_isoformat(self):
        """Test that date objects are converted to ISO format."""
        d = date(2024, 1, 15)
        result = sanitize_for_bigquery_json(d)
        assert result == "2024-01-15"

    def test_dict_sanitization(self):
        """Test that dictionaries are recursively sanitized."""
        input_dict = {
            "string": "test",
            "bytes": b"data",
            "null": None,
            "number": 42,
        }
        result = sanitize_for_bigquery_json(input_dict)

        assert result["string"] == "test"
        assert result["bytes"] == base64.b64encode(b"data").decode('ascii')
        assert result["null"] is None
        assert result["number"] == 42

    def test_list_sanitization(self):
        """Test that lists are recursively sanitized."""
        input_list = [
            "test",
            b"data",
            None,
            42,
            {"nested": "value"},
        ]
        result = sanitize_for_bigquery_json(input_list)

        assert result[0] == "test"
        assert result[1] == base64.b64encode(b"data").decode('ascii')
        assert result[2] is None
        assert result[3] == 42
        assert result[4] == {"nested": "value"}

    def test_nested_structure(self):
        """Test sanitization of deeply nested structures."""
        input_data = {
            "level1": {
                "level2": {
                    "bytes": b"nested",
                    "list": [1, 2, b"three"],
                }
            }
        }
        result = sanitize_for_bigquery_json(input_data)

        expected_bytes = base64.b64encode(b"nested").decode('ascii')
        assert result["level1"]["level2"]["bytes"] == expected_bytes

        expected_list_bytes = base64.b64encode(b"three").decode('ascii')
        assert result["level1"]["level2"]["list"][2] == expected_list_bytes

    def test_unknown_type_to_string(self):
        """Test that unknown types are converted to strings."""
        class CustomObject:
            def __str__(self):
                return "custom_value"

        result = sanitize_for_bigquery_json(CustomObject())
        assert result == "custom_value"


class TestParsePostgresArrayString:
    """Test suite for parse_postgres_array_string function."""

    def test_empty_array(self):
        """Test parsing of empty PostgreSQL array."""
        result = parse_postgres_array_string("{}")
        assert result == []

    def test_simple_array(self):
        """Test parsing of simple PostgreSQL array."""
        result = parse_postgres_array_string("{a,b,c}")
        assert result == ["a", "b", "c"]

    def test_quoted_array(self):
        """Test parsing of quoted PostgreSQL array."""
        result = parse_postgres_array_string('{"a","b","c"}')
        assert result == ["a", "b", "c"]

    def test_array_with_commas_in_values(self):
        """Test parsing of array with commas inside quoted values."""
        result = parse_postgres_array_string('{"a,b","c"}')
        assert result == ["a,b", "c"]

    def test_array_with_null_values(self):
        """Test that NULL values are filtered out."""
        result = parse_postgres_array_string("{NULL,a,b}")
        assert result == ["a", "b"]

        result = parse_postgres_array_string('{"NULL","a","b"}')
        assert result == ["a", "b"]

    def test_array_with_escaped_quotes(self):
        """Test parsing of array with escaped quotes inside values."""
        result = parse_postgres_array_string('{"a""b","c"}')
        assert result == ['a"b', "c"]

    def test_numeric_array(self):
        """Test parsing of numeric array."""
        result = parse_postgres_array_string("{1,2,3}")
        assert result == ["1", "2", "3"]

    def test_non_array_string(self):
        """Test that non-array strings are returned as-is."""
        result = parse_postgres_array_string("not_an_array")
        assert result == "not_an_array"

    def test_non_string_input(self):
        """Test that non-string inputs are returned as-is."""
        result = parse_postgres_array_string(42)
        assert result == 42

        result = parse_postgres_array_string(None)
        assert result is None

    def test_array_with_spaces(self):
        """Test parsing of array with spaces."""
        result = parse_postgres_array_string("{ a , b , c }")
        assert result == ["a", "b", "c"]


class TestValidateJsonForBigQuery:
    """Test suite for validate_json_for_bigquery function."""

    def test_valid_simple_record(self):
        """Test validation of simple valid record."""
        record = {
            "id": 1,
            "name": "test",
            "active": True,
        }
        is_valid, error = validate_json_for_bigquery(record)
        assert is_valid is True
        assert error is None

    def test_valid_nested_record(self):
        """Test validation of nested record."""
        record = {
            "id": 1,
            "metadata": {
                "key": "value",
                "nested": {"deep": "data"},
            },
            "items": [1, 2, 3],
        }
        is_valid, error = validate_json_for_bigquery(record)
        assert is_valid is True
        assert error is None

    def test_record_with_null_bytes(self):
        """Test that records with null bytes are invalid."""
        record = {
            "id": 1,
            "name": "test\x00data",
        }
        is_valid, error = validate_json_for_bigquery(record)
        assert is_valid is False
        assert "null bytes" in error.lower()

    def test_record_with_datetime(self):
        """Test validation of record with datetime (should convert via default=str)."""
        record = {
            "id": 1,
            "timestamp": datetime(2024, 1, 15, 10, 30, 0),
        }
        is_valid, error = validate_json_for_bigquery(record)
        # Should be valid because default=str handles datetime
        assert is_valid is True
        assert error is None

    def test_empty_record(self):
        """Test validation of empty record."""
        record = {}
        is_valid, error = validate_json_for_bigquery(record)
        assert is_valid is True
        assert error is None

    def test_record_with_none_values(self):
        """Test validation of record with None values."""
        record = {
            "id": 1,
            "nullable_field": None,
            "name": "test",
        }
        is_valid, error = validate_json_for_bigquery(record)
        assert is_valid is True
        assert error is None

    def test_record_with_unicode(self):
        """Test validation of record with Unicode characters."""
        record = {
            "id": 1,
            "name": "テスト",  # Japanese characters
            "emoji": "🎉",
        }
        is_valid, error = validate_json_for_bigquery(record)
        assert is_valid is True
        assert error is None


class TestIntegrationScenarios:
    """Integration tests combining multiple utility functions."""

    def test_full_sanitization_and_validation_pipeline(self):
        """Test full pipeline of sanitization followed by validation."""
        # Create a complex record with various data types
        record = {
            "id": 1,
            "name": "test\x00user",  # Null byte to be removed
            "binary_data": b"some bytes",  # To be base64 encoded
            "timestamp": datetime(2024, 1, 15, 10, 30, 0),  # To be ISO formatted
            "metadata": {
                "nested_bytes": b"nested",
                "nested_null": "data\x00here",
            },
        }

        # Sanitize the record
        sanitized = sanitize_for_bigquery_json(record)

        # Validate the sanitized record
        is_valid, error = validate_json_for_bigquery(sanitized)

        assert is_valid is True
        assert error is None
        assert "\x00" not in str(sanitized)

    def test_postgres_array_in_record(self):
        """Test parsing PostgreSQL array within a record context."""
        # Simulate a record with PostgreSQL array field
        record = {
            "id": 1,
            "tags": "{tag1,tag2,tag3}",
        }

        # Parse the array field
        record["tags"] = parse_postgres_array_string(record["tags"])

        # Sanitize and validate
        sanitized = sanitize_for_bigquery_json(record)
        is_valid, error = validate_json_for_bigquery(sanitized)

        assert is_valid is True
        assert sanitized["tags"] == ["tag1", "tag2", "tag3"]
