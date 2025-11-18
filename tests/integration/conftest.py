"""
DeltaFlow Integration Tests - Pytest Configuration and Fixtures

This module provides pytest fixtures and utilities for integration testing
of the DeltaFlow data synchronization pipeline.

Fixtures provide:
- Mock database connections (PostgreSQL, MongoDB)
- Mock BigQuery clients
- Sample test data
- Apache Beam TestPipeline instances
- Configuration helpers

Author: DeltaFlow Team
License: Apache 2.0
"""

import pytest
import os
import json
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional
from unittest.mock import Mock, MagicMock, patch
import tempfile

# Apache Beam imports
import apache_beam as beam
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that, equal_to
from apache_beam.options.pipeline_options import PipelineOptions


# =============================================================================
# PYTEST CONFIGURATION
# =============================================================================

def pytest_configure(config):
    """Configure pytest with custom markers."""
    config.addinivalue_line(
        "markers", "integration: mark test as integration test (requires external services)"
    )
    config.addinivalue_line(
        "markers", "unit: mark test as unit test (no external dependencies)"
    )
    config.addinivalue_line(
        "markers", "e2e: mark test as end-to-end test (full pipeline)"
    )
    config.addinivalue_line(
        "markers", "slow: mark test as slow (may take > 10 seconds)"
    )


# =============================================================================
# TEST DATA FIXTURES
# =============================================================================

@pytest.fixture
def sample_postgresql_data() -> List[Dict[str, Any]]:
    """
    Generate sample PostgreSQL-like records for testing.

    Returns:
        List of dictionaries representing database rows
    """
    base_time = datetime(2024, 1, 1, 0, 0, 0)

    return [
        {
            'id': 1,
            'user_id': 1001,
            'name': 'Alice Johnson',
            'email': 'alice@example.com',
            'status': 'active',
            'created_at': base_time,
            'updated_at': base_time + timedelta(hours=1),
            'metadata': {'tier': 'premium', 'region': 'us-west'},
            'tags': ['vip', 'early-adopter']
        },
        {
            'id': 2,
            'user_id': 1002,
            'name': 'Bob Smith',
            'email': 'bob@example.com',
            'status': 'active',
            'created_at': base_time + timedelta(hours=1),
            'updated_at': base_time + timedelta(hours=2),
            'metadata': {'tier': 'basic', 'region': 'us-east'},
            'tags': ['standard']
        },
        {
            'id': 3,
            'user_id': 1003,
            'name': 'Charlie Davis',
            'email': 'charlie@example.com',
            'status': 'inactive',
            'created_at': base_time + timedelta(hours=2),
            'updated_at': base_time + timedelta(hours=3),
            'metadata': {'tier': 'premium', 'region': 'eu-west'},
            'tags': ['vip', 'enterprise']
        },
    ]


@pytest.fixture
def sample_mongodb_data() -> List[Dict[str, Any]]:
    """
    Generate sample MongoDB-like documents for testing.

    Returns:
        List of dictionaries representing MongoDB documents
    """
    base_time = datetime(2024, 1, 1, 0, 0, 0)

    return [
        {
            '_id': '507f1f77bcf86cd799439011',
            'user_id': 2001,
            'event_type': 'purchase',
            'event_data': {
                'product_id': 'PROD-123',
                'price': 29.99,
                'currency': 'USD',
                'quantity': 2
            },
            'metadata': {
                'ip_address': '192.168.1.1',
                'user_agent': 'Mozilla/5.0'
            },
            'tags': ['checkout', 'completed'],
            'created_at': base_time
        },
        {
            '_id': '507f1f77bcf86cd799439012',
            'user_id': 2002,
            'event_type': 'view',
            'event_data': {
                'product_id': 'PROD-456',
                'duration_seconds': 45
            },
            'metadata': {
                'ip_address': '192.168.1.2',
                'user_agent': 'Chrome/91.0'
            },
            'tags': ['browse'],
            'created_at': base_time + timedelta(hours=1)
        },
    ]


@pytest.fixture
def sample_incremental_data() -> List[Dict[str, Any]]:
    """
    Generate sample data for Smart Sync incremental testing.

    Returns:
        Two batches of data: initial and incremental
    """
    base_time = datetime(2024, 1, 1, 0, 0, 0)

    initial_batch = [
        {
            'id': 1,
            'value': 'record_1',
            'updated_at': base_time
        },
        {
            'id': 2,
            'value': 'record_2',
            'updated_at': base_time + timedelta(minutes=30)
        },
    ]

    incremental_batch = [
        {
            'id': 2,
            'value': 'record_2_updated',
            'updated_at': base_time + timedelta(hours=2)
        },
        {
            'id': 3,
            'value': 'record_3',
            'updated_at': base_time + timedelta(hours=3)
        },
    ]

    return {
        'initial': initial_batch,
        'incremental': incremental_batch
    }


# =============================================================================
# MOCK DATABASE FIXTURES
# =============================================================================

@pytest.fixture
def mock_postgresql_connection():
    """
    Mock PostgreSQL connection for testing.

    Yields:
        Mock psycopg2 connection object
    """
    mock_conn = MagicMock()
    mock_cursor = MagicMock()

    # Configure cursor mock
    mock_cursor.description = [
        ('id', None),
        ('user_id', None),
        ('name', None),
        ('email', None),
        ('status', None),
        ('created_at', None),
        ('updated_at', None),
        ('metadata', None),
        ('tags', None),
    ]

    mock_conn.cursor.return_value = mock_cursor
    mock_cursor.__enter__ = Mock(return_value=mock_cursor)
    mock_cursor.__exit__ = Mock(return_value=False)

    with patch('psycopg2.connect', return_value=mock_conn):
        yield mock_conn


@pytest.fixture
def mock_mongodb_client(sample_mongodb_data):
    """
    Mock MongoDB client for testing.

    Args:
        sample_mongodb_data: Sample MongoDB documents

    Yields:
        Mock pymongo MongoClient object
    """
    mock_client = MagicMock()
    mock_db = MagicMock()
    mock_collection = MagicMock()

    # Configure collection mock
    mock_collection.find.return_value = iter(sample_mongodb_data)
    mock_collection.count_documents.return_value = len(sample_mongodb_data)

    mock_db.__getitem__ = lambda self, key: mock_collection
    mock_client.__getitem__ = lambda self, key: mock_db

    with patch('pymongo.MongoClient', return_value=mock_client):
        yield mock_client


@pytest.fixture
def mock_bigquery_client():
    """
    Mock BigQuery client for testing.

    Yields:
        Mock google.cloud.bigquery.Client object
    """
    mock_client = MagicMock()
    mock_table = MagicMock()
    mock_table.schema = []

    mock_client.get_table.return_value = mock_table
    mock_client.create_table.return_value = mock_table
    mock_client.query.return_value = MagicMock()

    with patch('google.cloud.bigquery.Client', return_value=mock_client):
        yield mock_client


# =============================================================================
# APACHE BEAM TEST FIXTURES
# =============================================================================

@pytest.fixture
def test_pipeline():
    """
    Create Apache Beam TestPipeline for testing.

    Yields:
        TestPipeline instance
    """
    with TestPipeline() as pipeline:
        yield pipeline


@pytest.fixture
def pipeline_options() -> PipelineOptions:
    """
    Create default PipelineOptions for testing.

    Returns:
        PipelineOptions configured for DirectRunner
    """
    return PipelineOptions(
        runner='DirectRunner',
        project='test-project',
        temp_location='/tmp/beam-temp',
    )


# =============================================================================
# CONFIGURATION FIXTURES
# =============================================================================

@pytest.fixture
def postgresql_config() -> Dict[str, str]:
    """
    PostgreSQL connection configuration for testing.

    Returns:
        Dictionary with PostgreSQL parameters
    """
    return {
        'postgresql_host': os.getenv('TEST_PG_HOST', 'localhost'),
        'postgresql_port': os.getenv('TEST_PG_PORT', '5432'),
        'postgresql_database': os.getenv('TEST_PG_DATABASE', 'testdb'),
        'postgresql_username': os.getenv('TEST_PG_USERNAME', 'postgres'),
        'postgresql_password': os.getenv('TEST_PG_PASSWORD', 'postgres'),
        'postgresql_query': 'SELECT * FROM test_table',
    }


@pytest.fixture
def mongodb_config() -> Dict[str, str]:
    """
    MongoDB connection configuration for testing.

    Returns:
        Dictionary with MongoDB parameters
    """
    return {
        'mongodb_host': os.getenv('TEST_MONGO_HOST', 'localhost'),
        'mongodb_port': os.getenv('TEST_MONGO_PORT', '27017'),
        'mongodb_database': os.getenv('TEST_MONGO_DATABASE', 'testdb'),
        'mongodb_collection': os.getenv('TEST_MONGO_COLLECTION', 'test_collection'),
        'mongodb_query': '{}',
    }


@pytest.fixture
def bigquery_config() -> Dict[str, str]:
    """
    BigQuery destination configuration for testing.

    Returns:
        Dictionary with BigQuery parameters
    """
    return {
        'destination_bigquery_project': os.getenv('TEST_BQ_PROJECT', 'test-project'),
        'destination_bigquery_dataset': os.getenv('TEST_BQ_DATASET', 'test_dataset'),
        'destination_bigquery_table': os.getenv('TEST_BQ_TABLE', 'test_table'),
    }


@pytest.fixture
def smart_sync_config() -> Dict[str, str]:
    """
    Smart Sync configuration for testing.

    Returns:
        Dictionary with Smart Sync parameters
    """
    return {
        'enable_smart_sync': 'true',
        'smart_sync_timestamp_column': 'updated_at',
        'postgresql_base_query': "SELECT * FROM test_table WHERE updated_at > '{start_timestamp}' AND updated_at <= '{end_timestamp}'",
        'sync_all_on_empty_table': 'true',
    }


# =============================================================================
# HELPER FIXTURES
# =============================================================================

@pytest.fixture
def temp_gcs_path():
    """
    Create temporary GCS-like path for testing.

    Yields:
        Temporary file path
    """
    with tempfile.TemporaryDirectory() as tmpdir:
        yield os.path.join(tmpdir, 'test-bucket')


@pytest.fixture
def mock_beam_io():
    """
    Mock Beam I/O operations for testing.

    Yields:
        Dictionary with mocked I/O transforms
    """
    with patch('apache_beam.io.ReadFromBigQuery') as mock_read_bq, \
         patch('apache_beam.io.WriteToBigQuery') as mock_write_bq:
        yield {
            'read_bigquery': mock_read_bq,
            'write_bigquery': mock_write_bq,
        }


# =============================================================================
# VALIDATION HELPERS
# =============================================================================

def assert_records_equal(actual: List[Dict], expected: List[Dict], ignore_keys: Optional[List[str]] = None):
    """
    Assert that two lists of records are equal, with optional key filtering.

    Args:
        actual: Actual records
        expected: Expected records
        ignore_keys: Keys to ignore in comparison
    """
    if ignore_keys:
        actual_filtered = [
            {k: v for k, v in record.items() if k not in ignore_keys}
            for record in actual
        ]
        expected_filtered = [
            {k: v for k, v in record.items() if k not in ignore_keys}
            for record in expected
        ]
    else:
        actual_filtered = actual
        expected_filtered = expected

    assert len(actual_filtered) == len(expected_filtered), \
        f"Record count mismatch: {len(actual_filtered)} != {len(expected_filtered)}"

    for i, (actual_record, expected_record) in enumerate(zip(actual_filtered, expected_filtered)):
        assert actual_record == expected_record, \
            f"Record {i} mismatch:\nActual: {actual_record}\nExpected: {expected_record}"


def assert_bigquery_schema_matches(actual_schema: List[Dict], expected_fields: List[str]):
    """
    Assert that BigQuery schema contains expected fields.

    Args:
        actual_schema: BigQuery schema (list of field objects)
        expected_fields: List of expected field names
    """
    actual_field_names = [field['name'] for field in actual_schema]

    for expected_field in expected_fields:
        assert expected_field in actual_field_names, \
            f"Expected field '{expected_field}' not found in schema: {actual_field_names}"


# =============================================================================
# PYTEST UTILITIES
# =============================================================================

@pytest.fixture(autouse=True)
def reset_environment():
    """
    Reset environment variables before each test.

    This fixture runs automatically before each test to ensure clean state.
    """
    original_env = os.environ.copy()
    yield
    # Restore original environment
    os.environ.clear()
    os.environ.update(original_env)


@pytest.fixture
def capture_logs(caplog):
    """
    Capture log output for testing.

    Yields:
        caplog fixture for log assertions
    """
    import logging
    caplog.set_level(logging.INFO)
    yield caplog


# =============================================================================
# CUSTOM ASSERTIONS FOR BEAM
# =============================================================================

class BeamAssertions:
    """Custom assertions for Apache Beam pipelines."""

    @staticmethod
    def assert_pcollection_equal(actual_pcoll, expected_data):
        """
        Assert that PCollection contains expected data.

        Args:
            actual_pcoll: Beam PCollection
            expected_data: Expected list of elements
        """
        assert_that(actual_pcoll, equal_to(expected_data))

    @staticmethod
    def assert_pcollection_count(actual_pcoll, expected_count):
        """
        Assert that PCollection has expected element count.

        Args:
            actual_pcoll: Beam PCollection
            expected_count: Expected number of elements
        """
        def check_count(elements):
            assert len(elements) == expected_count, \
                f"Expected {expected_count} elements, got {len(elements)}"

        assert_that(actual_pcoll, check_count)


@pytest.fixture
def beam_assertions():
    """
    Provide custom Beam assertions helper.

    Returns:
        BeamAssertions instance
    """
    return BeamAssertions()


# =============================================================================
# INTEGRATION TEST MARKERS
# =============================================================================

# Skip integration tests by default (require --integration flag)
def pytest_collection_modifyitems(config, items):
    """Modify test collection to handle markers."""
    if not config.getoption("--integration", default=False):
        skip_integration = pytest.mark.skip(reason="need --integration option to run")
        for item in items:
            if "integration" in item.keywords:
                item.add_marker(skip_integration)


def pytest_addoption(parser):
    """Add custom command-line options."""
    parser.addoption(
        "--integration",
        action="store_true",
        default=False,
        help="run integration tests (requires external services)"
    )
