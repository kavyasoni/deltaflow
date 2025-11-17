"""
Unit tests for Smart Sync functionality.

Tests the smart synchronization logic that enables incremental updates
based on timestamp tracking in BigQuery.
"""

import pytest
from datetime import datetime, timedelta
from unittest.mock import Mock, MagicMock, patch
from apache_beam.options.value_provider import StaticValueProvider

import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '../..'))

from main import (
    build_smart_postgresql_query,
    safe_get_param,
)


class TestBuildSmartPostgresqlQuery:
    """Test suite for build_smart_postgresql_query function."""

    def test_smart_sync_disabled(self, mock_pipeline_options):
        """Test that regular query is used when Smart Sync is disabled."""
        mock_pipeline_options.enable_smart_sync = StaticValueProvider(str, "false")
        mock_pipeline_options.postgresql_query = StaticValueProvider(
            str, "SELECT * FROM users"
        )

        result = build_smart_postgresql_query(mock_pipeline_options)
        assert result == "SELECT * FROM users"

    def test_smart_sync_disabled_empty_query_raises_error(self, mock_pipeline_options):
        """Test that empty query raises error when Smart Sync is disabled."""
        mock_pipeline_options.enable_smart_sync = StaticValueProvider(str, "false")
        mock_pipeline_options.postgresql_query = StaticValueProvider(str, "")

        with pytest.raises(ValueError, match="cannot be empty"):
            build_smart_postgresql_query(mock_pipeline_options)

    @patch('main.SmartSyncDoFn')
    def test_smart_sync_enabled_with_valid_query(
        self, mock_smart_sync_class, mock_pipeline_options
    ):
        """Test Smart Sync with valid query generation."""
        mock_pipeline_options.enable_smart_sync = StaticValueProvider(str, "true")

        # Mock SmartSyncDoFn to return a valid query
        mock_instance = MagicMock()
        mock_instance.get_sync_query.return_value = (
            "SELECT * FROM users WHERE updated_at > '2024-01-15 00:00:00' "
            "AND updated_at <= '2024-01-20 00:00:00' ORDER BY updated_at ASC"
        )
        mock_smart_sync_class.return_value = mock_instance

        result = build_smart_postgresql_query(mock_pipeline_options)

        assert "WHERE updated_at >" in result
        assert "ORDER BY updated_at ASC" in result
        mock_instance.setup.assert_called_once()
        mock_instance.get_sync_query.assert_called_once()

    @patch('main.SmartSyncDoFn')
    def test_smart_sync_empty_query_falls_back(
        self, mock_smart_sync_class, mock_pipeline_options
    ):
        """Test fallback to regular query when Smart Sync returns empty."""
        mock_pipeline_options.enable_smart_sync = StaticValueProvider(str, "true")
        mock_pipeline_options.postgresql_query = StaticValueProvider(
            str, "SELECT * FROM users"
        )

        # Mock SmartSyncDoFn to return empty query
        mock_instance = MagicMock()
        mock_instance.get_sync_query.return_value = ""
        mock_smart_sync_class.return_value = mock_instance

        result = build_smart_postgresql_query(mock_pipeline_options)

        assert result == "SELECT * FROM users"

    @patch('main.SmartSyncDoFn')
    def test_smart_sync_exception_falls_back(
        self, mock_smart_sync_class, mock_pipeline_options
    ):
        """Test fallback to regular query when Smart Sync raises exception."""
        mock_pipeline_options.enable_smart_sync = StaticValueProvider(str, "true")
        mock_pipeline_options.postgresql_query = StaticValueProvider(
            str, "SELECT * FROM users"
        )

        # Mock SmartSyncDoFn to raise exception
        mock_instance = MagicMock()
        mock_instance.setup.side_effect = Exception("BigQuery connection error")
        mock_smart_sync_class.return_value = mock_instance

        result = build_smart_postgresql_query(mock_pipeline_options)

        assert result == "SELECT * FROM users"

    @patch('main.SmartSyncDoFn')
    def test_smart_sync_with_base_query_fallback(
        self, mock_smart_sync_class, mock_pipeline_options
    ):
        """Test fallback to base query when regular query is empty."""
        mock_pipeline_options.enable_smart_sync = StaticValueProvider(str, "true")
        mock_pipeline_options.postgresql_query = StaticValueProvider(str, "")
        mock_pipeline_options.postgresql_base_query = StaticValueProvider(
            str,
            "SELECT * FROM users WHERE updated_at > '{start_timestamp}' "
            "AND updated_at <= '{end_timestamp}' ORDER BY updated_at ASC"
        )

        # Mock SmartSyncDoFn to return empty query
        mock_instance = MagicMock()
        mock_instance.get_sync_query.return_value = ""
        mock_smart_sync_class.return_value = mock_instance

        result = build_smart_postgresql_query(mock_pipeline_options)

        # Should remove timestamp placeholders and return simple query
        assert "SELECT * FROM users" in result
        assert "{start_timestamp}" not in result
        assert "{end_timestamp}" not in result


class TestSmartSyncTimestampCalculation:
    """Test suite for Smart Sync timestamp calculation logic."""

    def test_sync_window_calculation(self):
        """Test calculation of sync window from timestamps."""
        start_timestamp = datetime(2024, 1, 15, 0, 0, 0)
        end_timestamp = datetime(2024, 1, 20, 0, 0, 0)

        # Calculate duration
        duration = end_timestamp - start_timestamp

        assert duration.days == 5
        assert start_timestamp < end_timestamp

    def test_fallback_days_calculation(self):
        """Test calculation of fallback sync window."""
        fallback_days = 7
        end_timestamp = datetime.now()
        start_timestamp = end_timestamp - timedelta(days=fallback_days)

        duration = end_timestamp - start_timestamp

        assert duration.days == fallback_days
        assert start_timestamp < end_timestamp

    def test_empty_table_all_data_sync(self):
        """Test that empty table sync starts from historical date."""
        # When sync_all_on_empty_table=true, start from 1900-01-01
        start_timestamp = datetime(1900, 1, 1, 0, 0, 0)
        end_timestamp = datetime.now()

        duration = end_timestamp - start_timestamp

        # Should be more than 100 years
        assert duration.days > 36500
        assert start_timestamp.year == 1900


class TestSmartSyncQueryTemplates:
    """Test suite for Smart Sync query template handling."""

    def test_query_template_with_placeholders(self):
        """Test that query template contains correct placeholders."""
        template = (
            "SELECT * FROM users WHERE updated_at > '{start_timestamp}' "
            "AND updated_at <= '{end_timestamp}' ORDER BY updated_at ASC"
        )

        assert "{start_timestamp}" in template
        assert "{end_timestamp}" in template

    def test_query_template_substitution(self):
        """Test timestamp placeholder substitution."""
        template = (
            "SELECT * FROM users WHERE updated_at > '{start_timestamp}' "
            "AND updated_at <= '{end_timestamp}' ORDER BY updated_at ASC"
        )

        start = "2024-01-15 00:00:00"
        end = "2024-01-20 00:00:00"

        query = template.format(start_timestamp=start, end_timestamp=end)

        assert start in query
        assert end in query
        assert "{start_timestamp}" not in query
        assert "{end_timestamp}" not in query

    def test_query_template_with_complex_conditions(self):
        """Test query template with additional conditions."""
        template = (
            "SELECT id, name, email, updated_at FROM users "
            "WHERE is_active = true "
            "AND updated_at > '{start_timestamp}' "
            "AND updated_at <= '{end_timestamp}' "
            "ORDER BY updated_at ASC"
        )

        start = "2024-01-15 00:00:00"
        end = "2024-01-20 00:00:00"

        query = template.format(start_timestamp=start, end_timestamp=end)

        assert "is_active = true" in query
        assert start in query
        assert end in query


class TestSmartSyncEdgeCases:
    """Test suite for Smart Sync edge cases and error handling."""

    @patch('main.SmartSyncDoFn')
    def test_all_queries_empty_raises_error(
        self, mock_smart_sync_class, mock_pipeline_options
    ):
        """Test that error is raised when all query sources are empty."""
        mock_pipeline_options.enable_smart_sync = StaticValueProvider(str, "true")
        mock_pipeline_options.postgresql_query = StaticValueProvider(str, "")
        mock_pipeline_options.postgresql_base_query = StaticValueProvider(str, "")

        # Mock SmartSyncDoFn to return empty query
        mock_instance = MagicMock()
        mock_instance.get_sync_query.return_value = ""
        mock_smart_sync_class.return_value = mock_instance

        with pytest.raises(ValueError, match="No valid PostgreSQL query available"):
            build_smart_postgresql_query(mock_pipeline_options)

    def test_timestamp_format_iso8601(self):
        """Test that timestamps are formatted in ISO 8601 format."""
        timestamp = datetime(2024, 1, 15, 10, 30, 0)
        formatted = timestamp.strftime("%Y-%m-%d %H:%M:%S")

        assert formatted == "2024-01-15 10:30:00"

    def test_timestamp_with_microseconds(self):
        """Test handling of timestamps with microseconds."""
        timestamp = datetime(2024, 1, 15, 10, 30, 0, 123456)
        formatted = timestamp.strftime("%Y-%m-%d %H:%M:%S")

        # Microseconds should be truncated in standard format
        assert formatted == "2024-01-15 10:30:00"
        assert ".123456" not in formatted


class TestSmartSyncIntegration:
    """Integration tests for Smart Sync functionality."""

    @patch('main.SmartSyncDoFn')
    def test_full_smart_sync_flow(self, mock_smart_sync_class, mock_pipeline_options):
        """Test complete Smart Sync flow from options to query generation."""
        # Configure pipeline options for Smart Sync
        mock_pipeline_options.enable_smart_sync = StaticValueProvider(str, "true")
        mock_pipeline_options.postgresql_base_query = StaticValueProvider(
            str,
            "SELECT * FROM orders WHERE updated_at > '{start_timestamp}' "
            "AND updated_at <= '{end_timestamp}' ORDER BY updated_at ASC"
        )
        mock_pipeline_options.timestamp_column = StaticValueProvider(str, "updated_at")
        mock_pipeline_options.sync_all_on_empty_table = StaticValueProvider(str, "true")
        mock_pipeline_options.fallback_days = StaticValueProvider(str, "7")

        # Mock SmartSyncDoFn to return a properly formatted query
        mock_instance = MagicMock()
        expected_query = (
            "SELECT * FROM orders WHERE updated_at > '2024-01-15 00:00:00' "
            "AND updated_at <= '2024-01-20 00:00:00' ORDER BY updated_at ASC"
        )
        mock_instance.get_sync_query.return_value = expected_query
        mock_smart_sync_class.return_value = mock_instance

        # Execute Smart Sync
        result = build_smart_postgresql_query(mock_pipeline_options)

        # Verify the result
        assert result == expected_query
        assert "WHERE updated_at >" in result
        assert "ORDER BY updated_at ASC" in result

        # Verify SmartSyncDoFn was initialized and called correctly
        mock_smart_sync_class.assert_called_once_with(mock_pipeline_options)
        mock_instance.setup.assert_called_once()
        mock_instance.get_sync_query.assert_called_once()
