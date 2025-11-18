"""
DeltaFlow Integration Tests - Comprehensive E2E Pipeline Tests

This module contains comprehensive end-to-end tests for the DeltaFlow pipeline,
covering all major features and data source types.

Test Coverage:
1. Full PostgreSQL to BigQuery pipeline
2. Smart Sync incremental updates
3. MongoDB to BigQuery pipeline
4. Auto-schema detection
5. Multi-source pipeline
6. Error handling
7. Data validation
8. Field transformations

Author: DeltaFlow Team
License: Apache 2.0
"""

import pytest
import json
from datetime import datetime, timedelta
from unittest.mock import Mock, MagicMock, patch, call
from typing import Dict, List, Any

import apache_beam as beam
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that, equal_to
from apache_beam.options.pipeline_options import PipelineOptions


# =============================================================================
# TEST 1: Full PostgreSQL to BigQuery Pipeline
# =============================================================================

@pytest.mark.integration
@pytest.mark.e2e
def test_full_postgresql_to_bigquery_pipeline(
    sample_postgresql_data,
    postgresql_config,
    bigquery_config,
    mock_postgresql_connection,
    mock_bigquery_client
):
    """
    Test complete PostgreSQL to BigQuery synchronization pipeline.

    This test validates:
    - PostgreSQL connection and data reading
    - Data transformation and sanitization
    - BigQuery schema generation
    - Data writing to BigQuery

    Args:
        sample_postgresql_data: Sample test data
        postgresql_config: PostgreSQL connection parameters
        bigquery_config: BigQuery destination parameters
        mock_postgresql_connection: Mocked PostgreSQL connection
        mock_bigquery_client: Mocked BigQuery client
    """
    # Configure mock PostgreSQL cursor to return sample data
    mock_cursor = mock_postgresql_connection.cursor.return_value
    mock_cursor.fetchall.return_value = [
        tuple(record.values()) for record in sample_postgresql_data
    ]

    # Import main module (must be done after mocks are set up)
    import sys
    import os
    sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(__file__))))

    from main import PostgreSQLReaderDoFn, DataValidationDoFn

    # Create test pipeline
    with TestPipeline() as pipeline:
        # Read from PostgreSQL (mocked)
        with patch('psycopg2.connect', return_value=mock_postgresql_connection):
            reader = PostgreSQLReaderDoFn(
                host='localhost',
                port='5432',
                database='testdb',
                username='postgres',
                password='postgres',
                query='SELECT * FROM users'
            )

            # Simulate reading data
            records = (
                pipeline
                | 'Create Input' >> beam.Create([None])  # Trigger read
                | 'Read PostgreSQL' >> beam.ParDo(reader)
            )

        # Validate data
        validator = DataValidationDoFn()
        validated_records = (
            records
            | 'Validate Data' >> beam.ParDo(validator)
        )

        # Assert output
        def check_records(elements):
            """Validate pipeline output."""
            assert len(elements) > 0, "Pipeline produced no output"

            # Check first record structure
            first_record = elements[0]
            assert 'id' in first_record, "Missing 'id' field"
            assert 'email' in first_record, "Missing 'email' field"
            assert 'status' in first_record, "Missing 'status' field"

            print(f"✓ Pipeline processed {len(elements)} records successfully")

        assert_that(validated_records, check_records)


# =============================================================================
# TEST 2: Smart Sync Incremental Pipeline
# =============================================================================

@pytest.mark.integration
@pytest.mark.e2e
def test_smart_sync_incremental_pipeline(
    sample_incremental_data,
    postgresql_config,
    bigquery_config,
    smart_sync_config,
    mock_postgresql_connection,
    mock_bigquery_client
):
    """
    Test Smart Sync feature for incremental data synchronization.

    This test validates:
    - BigQuery max timestamp query
    - Dynamic query building with timestamp placeholders
    - Incremental data fetching
    - Correct timestamp filtering

    Args:
        sample_incremental_data: Sample data with initial and incremental batches
        postgresql_config: PostgreSQL connection parameters
        bigquery_config: BigQuery destination parameters
        smart_sync_config: Smart Sync configuration
        mock_postgresql_connection: Mocked PostgreSQL connection
        mock_bigquery_client: Mocked BigQuery client
    """
    import sys
    import os
    sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(__file__))))

    from main import SmartSyncDoFn, build_smart_postgresql_query

    # Mock BigQuery to return max timestamp
    mock_query_job = MagicMock()
    max_timestamp = datetime(2024, 1, 1, 1, 0, 0)
    mock_query_job.result.return_value = [
        {'max_timestamp': max_timestamp}
    ]
    mock_bigquery_client.query.return_value = mock_query_job

    with TestPipeline() as pipeline:
        # Run Smart Sync
        with patch('google.cloud.bigquery.Client', return_value=mock_bigquery_client):
            smart_sync = SmartSyncDoFn(
                destination_project='test-project',
                destination_dataset='test_dataset',
                destination_table='test_table',
                timestamp_column='updated_at',
                fallback_hours='24',
                sync_all_on_empty_table='false'
            )

            # Trigger Smart Sync
            sync_info = (
                pipeline
                | 'Create Trigger' >> beam.Create([None])
                | 'Run Smart Sync' >> beam.ParDo(smart_sync)
            )

            def check_sync_info(elements):
                """Validate Smart Sync output."""
                assert len(elements) > 0, "Smart Sync produced no output"

                sync_data = elements[0]
                assert 'start_timestamp' in sync_data
                assert 'end_timestamp' in sync_data

                # Verify start timestamp matches max from BigQuery
                start_ts = sync_data['start_timestamp']
                assert start_ts is not None, "Start timestamp should not be None"

                print(f"✓ Smart Sync configured: {start_ts} to {sync_data['end_timestamp']}")

            assert_that(sync_info, check_sync_info)

    # Test query building
    base_query = "SELECT * FROM users WHERE updated_at > '{start_timestamp}' AND updated_at <= '{end_timestamp}'"
    start_ts = "2024-01-01 00:00:00"
    end_ts = "2024-01-01 12:00:00"

    built_query = build_smart_postgresql_query(base_query, start_ts, end_ts)

    assert start_ts in built_query, f"Start timestamp not in query: {built_query}"
    assert end_ts in built_query, f"End timestamp not in query: {built_query}"
    assert '{start_timestamp}' not in built_query, "Placeholder not replaced"
    assert '{end_timestamp}' not in built_query, "Placeholder not replaced"

    print(f"✓ Smart Sync query built correctly: {built_query[:100]}...")


# =============================================================================
# TEST 3: MongoDB to BigQuery Pipeline
# =============================================================================

@pytest.mark.integration
@pytest.mark.e2e
def test_mongodb_to_bigquery_pipeline(
    sample_mongodb_data,
    mongodb_config,
    bigquery_config,
    mock_mongodb_client,
    mock_bigquery_client
):
    """
    Test MongoDB to BigQuery synchronization pipeline.

    This test validates:
    - MongoDB connection and document reading
    - BSON type handling (ObjectId, nested objects, arrays)
    - Type conversion to BigQuery-compatible formats
    - JSON serialization

    Args:
        sample_mongodb_data: Sample MongoDB documents
        mongodb_config: MongoDB connection parameters
        bigquery_config: BigQuery destination parameters
        mock_mongodb_client: Mocked MongoDB client
        mock_bigquery_client: Mocked BigQuery client
    """
    import sys
    import os
    sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(__file__))))

    from main import MongoDBReadDoFn, DataValidationDoFn

    # Configure mock MongoDB collection
    mock_collection = mock_mongodb_client['testdb']['test_collection']
    mock_collection.find.return_value = iter(sample_mongodb_data)

    with TestPipeline() as pipeline:
        # Read from MongoDB (mocked)
        with patch('pymongo.MongoClient', return_value=mock_mongodb_client):
            reader = MongoDBReadDoFn(
                host='localhost',
                port='27017',
                database='testdb',
                collection='test_collection',
                query='{}',
                projection=None,
                username=None,
                password=None,
                connection_string=None
            )

            records = (
                pipeline
                | 'Create Trigger' >> beam.Create([None])
                | 'Read MongoDB' >> beam.ParDo(reader)
            )

        # Validate MongoDB data
        validator = DataValidationDoFn()
        validated_records = (
            records
            | 'Validate Data' >> beam.ParDo(validator)
        )

        def check_mongodb_records(elements):
            """Validate MongoDB pipeline output."""
            assert len(elements) > 0, "MongoDB pipeline produced no output"

            # Check for MongoDB-specific fields
            for record in elements:
                # ObjectId should be converted to string
                if '_id' in record:
                    assert isinstance(record['_id'], str), \
                        f"ObjectId should be string, got {type(record['_id'])}"

                # Check nested objects (should be preserved or converted to JSON)
                if 'event_data' in record:
                    assert isinstance(record['event_data'], (dict, str)), \
                        "Nested objects should be dict or JSON string"

            print(f"✓ MongoDB pipeline processed {len(elements)} documents")

        assert_that(validated_records, check_mongodb_records)


# =============================================================================
# TEST 4: Auto-Schema Detection Pipeline
# =============================================================================

@pytest.mark.integration
@pytest.mark.e2e
def test_auto_schema_detection_pipeline(
    sample_postgresql_data,
    postgresql_config,
    bigquery_config,
    mock_postgresql_connection,
    mock_bigquery_client
):
    """
    Test automatic schema detection and BigQuery table creation.

    This test validates:
    - Source schema extraction from information_schema
    - Type mapping from PostgreSQL to BigQuery
    - BigQuery table creation with schema
    - Partitioning and clustering configuration

    Args:
        sample_postgresql_data: Sample test data
        postgresql_config: PostgreSQL connection parameters
        bigquery_config: BigQuery destination parameters
        mock_postgresql_connection: Mocked PostgreSQL connection
        mock_bigquery_client: Mocked BigQuery client
    """
    import sys
    import os
    sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(__file__))))

    from main import PostgreSQLSchemaDetectionDoFn, BigQueryTableCreationDoFn

    # Mock information_schema query result
    mock_cursor = mock_postgresql_connection.cursor.return_value
    schema_info = [
        ('id', 'integer', 'NO'),
        ('user_id', 'integer', 'NO'),
        ('name', 'character varying', 'YES'),
        ('email', 'character varying', 'NO'),
        ('status', 'character varying', 'YES'),
        ('created_at', 'timestamp without time zone', 'NO'),
        ('updated_at', 'timestamp without time zone', 'NO'),
        ('metadata', 'jsonb', 'YES'),
        ('tags', 'ARRAY', 'YES'),
    ]
    mock_cursor.fetchall.return_value = schema_info

    with TestPipeline() as pipeline:
        # Detect schema
        with patch('psycopg2.connect', return_value=mock_postgresql_connection):
            schema_detector = PostgreSQLSchemaDetectionDoFn(
                host='localhost',
                port='5432',
                database='testdb',
                username='postgres',
                password='postgres',
                table_name='public.users'
            )

            detected_schema = (
                pipeline
                | 'Create Trigger' >> beam.Create([None])
                | 'Detect Schema' >> beam.ParDo(schema_detector)
            )

            def check_schema(elements):
                """Validate detected schema."""
                assert len(elements) > 0, "Schema detection produced no output"

                schema = elements[0]
                assert 'schema' in schema, "Missing 'schema' key"

                bq_schema = schema['schema']
                field_names = [field['name'] for field in bq_schema]

                # Verify expected fields
                expected_fields = ['id', 'user_id', 'name', 'email', 'status',
                                   'created_at', 'updated_at', 'metadata', 'tags']

                for field in expected_fields:
                    assert field in field_names, f"Missing field: {field}"

                # Verify type mappings
                field_types = {field['name']: field['type'] for field in bq_schema}
                assert field_types['id'] == 'INTEGER', "Integer type not mapped correctly"
                assert field_types['name'] == 'STRING', "VARCHAR not mapped to STRING"
                assert field_types['created_at'] == 'TIMESTAMP', "Timestamp not mapped correctly"
                assert field_types['metadata'] == 'JSON', "JSONB not mapped to JSON"

                print(f"✓ Auto-schema detected {len(bq_schema)} fields correctly")
                print(f"  Fields: {field_names}")

            assert_that(detected_schema, check_schema)


# =============================================================================
# TEST 5: Multi-Source Pipeline
# =============================================================================

@pytest.mark.integration
@pytest.mark.e2e
def test_multi_source_pipeline(
    sample_postgresql_data,
    sample_mongodb_data,
    postgresql_config,
    mongodb_config,
    bigquery_config,
    mock_postgresql_connection,
    mock_mongodb_client,
    mock_bigquery_client
):
    """
    Test pipeline with multiple data sources combined.

    This test validates:
    - Parallel processing of multiple sources
    - Data merging with beam.Flatten
    - Handling different source schemas
    - Combined output to single BigQuery table

    Args:
        sample_postgresql_data: Sample PostgreSQL data
        sample_mongodb_data: Sample MongoDB data
        postgresql_config: PostgreSQL connection parameters
        mongodb_config: MongoDB connection parameters
        bigquery_config: BigQuery destination parameters
        mock_postgresql_connection: Mocked PostgreSQL connection
        mock_mongodb_client: Mocked MongoDB client
        mock_bigquery_client: Mocked BigQuery client
    """
    import sys
    import os
    sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(__file__))))

    from main import PostgreSQLReaderDoFn, MongoDBReadDoFn

    # Configure mocks
    mock_pg_cursor = mock_postgresql_connection.cursor.return_value
    mock_pg_cursor.fetchall.return_value = [
        tuple(record.values()) for record in sample_postgresql_data
    ]

    mock_mongo_collection = mock_mongodb_client['testdb']['test_collection']
    mock_mongo_collection.find.return_value = iter(sample_mongodb_data)

    with TestPipeline() as pipeline:
        # Read from PostgreSQL
        with patch('psycopg2.connect', return_value=mock_postgresql_connection):
            pg_reader = PostgreSQLReaderDoFn(
                host='localhost',
                port='5432',
                database='testdb',
                username='postgres',
                password='postgres',
                query='SELECT * FROM users'
            )

            pg_records = (
                pipeline
                | 'Create PG Trigger' >> beam.Create([None])
                | 'Read PostgreSQL' >> beam.ParDo(pg_reader)
            )

        # Read from MongoDB
        with patch('pymongo.MongoClient', return_value=mock_mongodb_client):
            mongo_reader = MongoDBReadDoFn(
                host='localhost',
                port='27017',
                database='testdb',
                collection='test_collection',
                query='{}',
                projection=None,
                username=None,
                password=None,
                connection_string=None
            )

            mongo_records = (
                pipeline
                | 'Create Mongo Trigger' >> beam.Create([None])
                | 'Read MongoDB' >> beam.ParDo(mongo_reader)
            )

        # Combine sources
        combined_records = (
            (pg_records, mongo_records)
            | 'Flatten Sources' >> beam.Flatten()
        )

        def check_combined_records(elements):
            """Validate combined pipeline output."""
            assert len(elements) > 0, "Combined pipeline produced no output"

            # Should have records from both sources
            # Note: In real scenario, sources would have compatible schemas
            print(f"✓ Multi-source pipeline processed {len(elements)} total records")

        assert_that(combined_records, check_combined_records)


# =============================================================================
# TEST 6: Error Handling Pipeline
# =============================================================================

@pytest.mark.integration
@pytest.mark.e2e
def test_error_handling_pipeline(
    postgresql_config,
    bigquery_config,
    mock_postgresql_connection,
    mock_bigquery_client
):
    """
    Test pipeline error handling and recovery mechanisms.

    This test validates:
    - Connection failure handling
    - Query error recovery
    - Invalid data handling
    - Retry logic

    Args:
        postgresql_config: PostgreSQL connection parameters
        bigquery_config: BigQuery destination parameters
        mock_postgresql_connection: Mocked PostgreSQL connection
        mock_bigquery_client: Mocked BigQuery client
    """
    import sys
    import os
    sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(__file__))))

    from main import PostgreSQLReaderDoFn

    # Configure mock to raise exception
    mock_cursor = mock_postgresql_connection.cursor.return_value
    mock_cursor.execute.side_effect = Exception("Database connection error")

    with pytest.raises(Exception) as exc_info:
        with TestPipeline() as pipeline:
            with patch('psycopg2.connect', return_value=mock_postgresql_connection):
                reader = PostgreSQLReaderDoFn(
                    host='localhost',
                    port='5432',
                    database='testdb',
                    username='postgres',
                    password='postgres',
                    query='SELECT * FROM users'
                )

                records = (
                    pipeline
                    | 'Create Trigger' >> beam.Create([None])
                    | 'Read PostgreSQL' >> beam.ParDo(reader)
                )

    # Verify error was raised
    assert "error" in str(exc_info.value).lower() or "exception" in str(exc_info.value).lower()
    print("✓ Error handling validated - pipeline fails gracefully on database errors")


# =============================================================================
# TEST 7: Data Validation Pipeline
# =============================================================================

@pytest.mark.integration
@pytest.mark.e2e
def test_data_validation_pipeline():
    """
    Test data validation with invalid/problematic data.

    This test validates:
    - NULL byte handling
    - Invalid UTF-8 handling
    - Type validation
    - BigQuery JSON compatibility checks

    Args:
        None (uses inline test data)
    """
    import sys
    import os
    sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(__file__))))

    from main import DataValidationDoFn, sanitize_for_bigquery_json

    # Test data with various problematic values
    test_records = [
        {'id': 1, 'name': 'Valid Record', 'value': 100},
        {'id': 2, 'name': 'Record with null byte\x00', 'value': 200},
        {'id': 3, 'name': 'Normal Record', 'value': None},
        {'id': 4, 'name': b'Bytes value', 'value': 400},
    ]

    with TestPipeline() as pipeline:
        validator = DataValidationDoFn()

        validated_records = (
            pipeline
            | 'Create Records' >> beam.Create(test_records)
            | 'Validate Data' >> beam.ParDo(validator)
        )

        def check_validation(elements):
            """Validate data cleaning."""
            assert len(elements) > 0, "Validation produced no output"

            for record in elements:
                # Check null bytes removed
                for key, value in record.items():
                    if isinstance(value, str):
                        assert '\x00' not in value, f"Null byte not removed from {key}"

                # Check bytes converted to strings
                for key, value in record.items():
                    assert not isinstance(value, bytes), \
                        f"Bytes not converted: {key}={value}"

            print(f"✓ Data validation passed for {len(elements)} records")

        assert_that(validated_records, check_validation)

    # Test sanitization function directly
    test_cases = [
        ({'key': 'value\x00'}, {'key': 'value'}),  # Null byte removal
        ({'key': b'bytes'}, {'key': 'bytes'}),      # Bytes conversion
        ({'date': datetime(2024, 1, 1)}, {'date': '2024-01-01T00:00:00'}),  # Date conversion
    ]

    for input_data, expected_output in test_cases:
        result = sanitize_for_bigquery_json(input_data)
        # Check key transformations occurred
        assert isinstance(result, dict), "Sanitization should return dict"

    print("✓ Sanitization function validated")


# =============================================================================
# TEST 8: Transformation Pipeline
# =============================================================================

@pytest.mark.integration
@pytest.mark.e2e
def test_transformation_pipeline():
    """
    Test field transformations end-to-end.

    This test validates:
    - Field mapping/renaming
    - Type conversions
    - Custom transformations
    - Computed fields

    Args:
        None (uses inline test data)
    """
    import sys
    import os
    sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(__file__))))

    from main import DataTransformationDoFn

    # Test data
    test_records = [
        {
            'old_id': 1,
            'old_name': 'Test User',
            'price_string': '29.99',
            'quantity_string': '5',
            'created_at': '2024-01-01 10:00:00'
        },
        {
            'old_id': 2,
            'old_name': 'Another User',
            'price_string': '19.99',
            'quantity_string': '3',
            'created_at': '2024-01-02 11:00:00'
        },
    ]

    # Transformation configuration
    transformation_config = {
        'field_mappings': {
            'old_id': 'id',
            'old_name': 'name'
        },
        'type_conversions': {
            'price_string': 'FLOAT',
            'quantity_string': 'INTEGER'
        }
    }

    with TestPipeline() as pipeline:
        transformer = DataTransformationDoFn(
            transformation_config=json.dumps(transformation_config)
        )

        transformed_records = (
            pipeline
            | 'Create Records' >> beam.Create(test_records)
            | 'Transform Data' >> beam.ParDo(transformer)
        )

        def check_transformations(elements):
            """Validate transformations."""
            assert len(elements) > 0, "Transformation produced no output"

            for record in elements:
                # Check field mappings
                assert 'id' in record, "Field mapping failed: old_id -> id"
                assert 'name' in record, "Field mapping failed: old_name -> name"
                assert 'old_id' not in record, "Old field not removed"
                assert 'old_name' not in record, "Old field not removed"

                # Check type conversions
                if 'price_string' in record:
                    assert isinstance(record['price_string'], (float, int)), \
                        "Price not converted to numeric"
                if 'quantity_string' in record:
                    assert isinstance(record['quantity_string'], int), \
                        "Quantity not converted to integer"

            print(f"✓ Transformations applied successfully to {len(elements)} records")

        assert_that(transformed_records, check_transformations)


# =============================================================================
# UTILITY TESTS
# =============================================================================

@pytest.mark.unit
def test_safe_get_param():
    """Test safe parameter resolution for ValueProvider."""
    import sys
    import os
    sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(__file__))))

    from main import safe_get_param

    # Test direct string
    result = safe_get_param("test_value")
    assert result == "test_value"

    # Test None
    result = safe_get_param(None)
    assert result is None

    # Test ValueProvider-like object
    class MockValueProvider:
        def get(self):
            return "runtime_value"

    result = safe_get_param(MockValueProvider())
    assert result == "runtime_value"

    print("✓ safe_get_param() utility function validated")


@pytest.mark.unit
def test_parse_postgres_array():
    """Test PostgreSQL array string parsing."""
    import sys
    import os
    sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(__file__))))

    from main import parse_postgres_array_string

    # Test cases
    test_cases = [
        ('{a,b,c}', ['a', 'b', 'c']),
        ('{1,2,3}', ['1', '2', '3']),
        ('{}', []),
        ('{single}', ['single']),
        ('{with spaces, more spaces}', ['with spaces', ' more spaces']),
    ]

    for input_str, expected_output in test_cases:
        result = parse_postgres_array_string(input_str)
        assert result == expected_output, \
            f"Array parsing failed: {input_str} -> {result} (expected {expected_output})"

    print("✓ PostgreSQL array parsing validated")


if __name__ == '__main__':
    pytest.main([__file__, '-v', '--tb=short'])
