"""
Pytest configuration and shared fixtures for DeltaFlow tests.

This module provides common fixtures for testing the DeltaFlow pipeline,
including mock database connections, pipeline options, and sample data.
"""

import pytest
from unittest.mock import Mock, MagicMock
from datetime import datetime, timedelta
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.value_provider import StaticValueProvider


@pytest.fixture
def mock_pipeline_options():
    """
    Create mock CustomPipelineOptions for testing.

    Returns a PipelineOptions object with common test parameters set up
    using StaticValueProvider for runtime parameter resolution.
    """
    class MockPipelineOptions(PipelineOptions):
        def __init__(self):
            super().__init__()
            # PostgreSQL configuration
            self.postgresql_host = StaticValueProvider(str, "localhost")
            self.postgresql_port = StaticValueProvider(str, "5432")
            self.postgresql_database = StaticValueProvider(str, "test_db")
            self.postgresql_username = StaticValueProvider(str, "test_user")
            self.postgresql_password = StaticValueProvider(str, "test_pass")
            self.postgresql_query = StaticValueProvider(str, "SELECT * FROM test_table")
            self.postgresql_base_query = StaticValueProvider(
                str,
                "SELECT * FROM test_table WHERE updated_at > '{start_timestamp}' "
                "AND updated_at <= '{end_timestamp}' ORDER BY updated_at ASC"
            )

            # MongoDB configuration
            self.mongodb_uri = StaticValueProvider(str, "mongodb://localhost:27017")
            self.mongodb_database = StaticValueProvider(str, "test_db")
            self.mongodb_collection = StaticValueProvider(str, "test_collection")
            self.mongodb_filter = StaticValueProvider(str, "{}")
            self.mongodb_projection = StaticValueProvider(str, "{}")

            # BigQuery configuration
            self.destination_bigquery_project = StaticValueProvider(str, "test-project")
            self.destination_bigquery_dataset = StaticValueProvider(str, "test_dataset")
            self.destination_bigquery_table = StaticValueProvider(str, "test_table")

            # Smart Sync configuration
            self.enable_smart_sync = StaticValueProvider(str, "false")
            self.timestamp_column = StaticValueProvider(str, "updated_at")
            self.sync_all_on_empty_table = StaticValueProvider(str, "true")
            self.fallback_days = StaticValueProvider(str, "7")

            # Schema configuration
            self.enable_auto_schema = StaticValueProvider(str, "true")
            self.partition_field = StaticValueProvider(str, "updated_at")
            self.clustering_fields = StaticValueProvider(str, "id,created_at")

            # Data source
            self.data_source = StaticValueProvider(str, "postgresql")

            # Write configuration
            self.write_disposition = StaticValueProvider(str, "WRITE_APPEND")

    return MockPipelineOptions()


@pytest.fixture
def mock_bigquery_client():
    """
    Create a mock Google Cloud BigQuery client.

    Returns a MagicMock configured to simulate BigQuery operations.
    """
    mock_client = MagicMock()

    # Mock dataset and table references
    mock_dataset = MagicMock()
    mock_table_ref = MagicMock()
    mock_table = MagicMock()

    # Configure return values
    mock_client.dataset.return_value = mock_dataset
    mock_dataset.table.return_value = mock_table_ref
    mock_client.get_table.return_value = mock_table

    # Mock table schema
    mock_field = MagicMock()
    mock_field.name = "test_field"
    mock_field.field_type = "STRING"
    mock_field.mode = "NULLABLE"
    mock_table.schema = [mock_field]

    # Mock query results
    mock_query_job = MagicMock()
    mock_query_job.result.return_value = []
    mock_client.query.return_value = mock_query_job

    return mock_client


@pytest.fixture
def mock_postgres_connection():
    """
    Create a mock PostgreSQL connection (psycopg2).

    Returns a MagicMock configured to simulate PostgreSQL operations.
    """
    mock_conn = MagicMock()
    mock_cursor = MagicMock()

    # Configure cursor behavior
    mock_conn.cursor.return_value = mock_cursor
    mock_cursor.fetchall.return_value = []
    mock_cursor.fetchone.return_value = None
    mock_cursor.description = [
        ("id", None, None, None, None, None, None),
        ("name", None, None, None, None, None, None),
    ]

    # Configure execute to return cursor for chaining
    mock_cursor.execute.return_value = mock_cursor

    return mock_conn


@pytest.fixture
def mock_mongodb_client():
    """
    Create a mock MongoDB client (pymongo).

    Returns a MagicMock configured to simulate MongoDB operations.
    """
    mock_client = MagicMock()
    mock_db = MagicMock()
    mock_collection = MagicMock()

    # Configure database and collection access
    mock_client.__getitem__.return_value = mock_db
    mock_db.__getitem__.return_value = mock_collection

    # Mock find operations
    mock_collection.find.return_value = []
    mock_collection.count_documents.return_value = 0

    return mock_client


@pytest.fixture
def sample_postgres_data():
    """
    Provide sample PostgreSQL data for testing.

    Returns a list of dictionaries representing database rows.
    """
    return [
        {
            "id": 1,
            "name": "Alice",
            "email": "alice@example.com",
            "created_at": datetime(2024, 1, 1, 10, 0, 0),
            "updated_at": datetime(2024, 1, 15, 14, 30, 0),
            "is_active": True,
            "metadata": {"role": "admin", "department": "engineering"},
        },
        {
            "id": 2,
            "name": "Bob",
            "email": "bob@example.com",
            "created_at": datetime(2024, 1, 2, 11, 0, 0),
            "updated_at": datetime(2024, 1, 16, 9, 15, 0),
            "is_active": True,
            "metadata": {"role": "user", "department": "sales"},
        },
        {
            "id": 3,
            "name": "Charlie",
            "email": "charlie@example.com",
            "created_at": datetime(2024, 1, 3, 12, 0, 0),
            "updated_at": datetime(2024, 1, 17, 16, 45, 0),
            "is_active": False,
            "metadata": {"role": "user", "department": "marketing"},
        },
    ]


@pytest.fixture
def sample_mongodb_data():
    """
    Provide sample MongoDB data for testing.

    Returns a list of documents with MongoDB-specific types.
    """
    from bson import ObjectId

    return [
        {
            "_id": ObjectId("507f1f77bcf86cd799439011"),
            "order_id": "ORD-001",
            "customer_id": 1001,
            "total": 150.50,
            "items": [
                {"product": "Widget A", "quantity": 2, "price": 50.25},
                {"product": "Widget B", "quantity": 1, "price": 50.00},
            ],
            "status": "completed",
            "created_at": datetime(2024, 1, 10, 10, 0, 0),
            "updated_at": datetime(2024, 1, 15, 14, 30, 0),
        },
        {
            "_id": ObjectId("507f1f77bcf86cd799439012"),
            "order_id": "ORD-002",
            "customer_id": 1002,
            "total": 299.99,
            "items": [
                {"product": "Widget C", "quantity": 3, "price": 99.99},
            ],
            "status": "shipped",
            "created_at": datetime(2024, 1, 11, 11, 0, 0),
            "updated_at": datetime(2024, 1, 16, 9, 15, 0),
        },
    ]


@pytest.fixture
def sample_bigquery_schema():
    """
    Provide sample BigQuery schema for testing.

    Returns a list of schema field definitions.
    """
    from google.cloud.bigquery import SchemaField

    return [
        SchemaField("id", "INTEGER", mode="REQUIRED"),
        SchemaField("name", "STRING", mode="NULLABLE"),
        SchemaField("email", "STRING", mode="NULLABLE"),
        SchemaField("created_at", "TIMESTAMP", mode="NULLABLE"),
        SchemaField("updated_at", "TIMESTAMP", mode="NULLABLE"),
        SchemaField("is_active", "BOOLEAN", mode="NULLABLE"),
        SchemaField("metadata", "JSON", mode="NULLABLE"),
    ]


@pytest.fixture
def sample_postgres_array_strings():
    """
    Provide sample PostgreSQL array string literals for testing.

    Returns a dictionary mapping array strings to expected parsed results.
    """
    return {
        "{}": [],
        "{a,b,c}": ["a", "b", "c"],
        '{"a","b","c"}': ["a", "b", "c"],
        '{"a,b","c"}': ["a,b", "c"],
        '{"NULL","a","b"}': ["a", "b"],
        '{NULL,a,b}': ["a", "b"],
        '{"a""b","c"}': ['a"b', "c"],  # Escaped quotes
        "{1,2,3}": ["1", "2", "3"],
    }


@pytest.fixture
def sample_sanitize_data():
    """
    Provide sample data for BigQuery sanitization testing.

    Returns a dictionary mapping input values to expected sanitized outputs.
    """
    return {
        "null_bytes": "test\x00data",
        "bytes_data": b"binary data",
        "datetime_obj": datetime(2024, 1, 15, 10, 30, 0),
        "nested_dict": {
            "key1": "value1",
            "key2": b"binary",
            "key3": datetime(2024, 1, 15),
        },
        "nested_list": [1, "text", b"bytes", None],
        "null_value": None,
    }


@pytest.fixture
def mock_smart_sync_timestamp():
    """
    Provide a mock timestamp for Smart Sync testing.

    Returns a datetime object representing the last synced timestamp.
    """
    return datetime(2024, 1, 15, 0, 0, 0)


@pytest.fixture
def sample_sync_window():
    """
    Provide a sample sync window (start and end timestamps).

    Returns a tuple of (start_timestamp, end_timestamp).
    """
    start = datetime(2024, 1, 15, 0, 0, 0)
    end = datetime(2024, 1, 20, 0, 0, 0)
    return (start, end)
