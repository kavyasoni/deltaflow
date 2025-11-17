# DeltaFlow Test Suite

This directory contains the test suite for the DeltaFlow custom Dataflow pipeline template.

## Structure

```
tests/
├── conftest.py              # Shared pytest fixtures and configuration
├── unit/                    # Unit tests for individual components
│   ├── test_utils.py        # Tests for utility functions
│   ├── test_smart_sync.py   # Tests for Smart Sync logic
│   └── test_schema_detection.py  # Tests for schema detection
└── integration/             # Integration tests for full pipeline
    └── test_pipeline.py     # End-to-end pipeline tests
```

## Running Tests

### Install Development Dependencies

```bash
pip install -r requirements-dev.txt
```

### Run All Tests

```bash
pytest
```

### Run Specific Test Categories

```bash
# Run only unit tests
pytest tests/unit/

# Run only integration tests
pytest tests/integration/

# Run tests by marker
pytest -m unit
pytest -m integration

# Skip integration tests (useful in CI without databases)
pytest -m "not integration"
```

### Run Specific Test Files

```bash
# Test utility functions
pytest tests/unit/test_utils.py

# Test Smart Sync logic
pytest tests/unit/test_smart_sync.py

# Test schema detection
pytest tests/unit/test_schema_detection.py

# Test pipeline integration
pytest tests/integration/test_pipeline.py
```

### Run Specific Test Classes or Functions

```bash
# Run specific test class
pytest tests/unit/test_utils.py::TestSafeGetParam

# Run specific test function
pytest tests/unit/test_utils.py::TestSafeGetParam::test_none_value
```

### Run with Coverage

```bash
# Generate coverage report
pytest --cov=. --cov-report=html --cov-report=term

# View coverage report
open htmlcov/index.html
```

### Run with Verbose Output

```bash
pytest -v
pytest -vv  # Extra verbose
```

## Test Organization

### Unit Tests (`tests/unit/`)

Unit tests focus on testing individual functions and classes in isolation using mocks and fixtures.

**test_utils.py**: Tests core utility functions
- `safe_get_param()`: Parameter resolution
- `sanitize_for_bigquery_json()`: Data sanitization
- `parse_postgres_array_string()`: PostgreSQL array parsing
- `validate_json_for_bigquery()`: JSON validation

**test_smart_sync.py**: Tests Smart Sync functionality
- Query building logic
- Timestamp calculation
- Fallback behavior
- Empty table handling

**test_schema_detection.py**: Tests schema detection
- PostgreSQL schema detection
- MongoDB schema detection
- BigQuery schema detection
- Type mappings

### Integration Tests (`tests/integration/`)

Integration tests verify end-to-end pipeline functionality using Apache Beam's TestPipeline and DirectRunner.

**test_pipeline.py**: Full pipeline tests
- PostgreSQL to BigQuery pipeline
- MongoDB to BigQuery pipeline
- Smart Sync pipeline
- Auto-Schema pipeline
- Error handling

### Fixtures (`conftest.py`)

Shared fixtures available to all tests:

- `mock_pipeline_options`: Mock CustomPipelineOptions
- `mock_bigquery_client`: Mock BigQuery client
- `mock_postgres_connection`: Mock PostgreSQL connection
- `mock_mongodb_client`: Mock MongoDB client
- `sample_postgres_data`: Sample PostgreSQL data
- `sample_mongodb_data`: Sample MongoDB data
- `sample_bigquery_schema`: Sample BigQuery schema
- `sample_postgres_array_strings`: Sample PostgreSQL arrays
- `sample_sanitize_data`: Sample data for sanitization testing

## Writing New Tests

### Unit Test Example

```python
import pytest
from main import your_function

class TestYourFunction:
    """Test suite for your_function."""

    def test_basic_case(self):
        """Test basic functionality."""
        result = your_function("input")
        assert result == "expected"

    def test_edge_case(self):
        """Test edge case handling."""
        with pytest.raises(ValueError):
            your_function(None)

    def test_with_fixture(self, mock_pipeline_options):
        """Test using a fixture."""
        result = your_function(mock_pipeline_options)
        assert result is not None
```

### Integration Test Example

```python
import apache_beam as beam
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that, equal_to

def test_pipeline():
    """Test pipeline transformation."""
    with TestPipeline() as p:
        output = (
            p
            | 'Create' >> beam.Create([1, 2, 3])
            | 'Transform' >> beam.Map(lambda x: x * 2)
        )

        assert_that(output, equal_to([2, 4, 6]))
```

## Test Markers

Tests can be marked with custom markers for selective execution:

```python
@pytest.mark.unit
def test_unit_functionality():
    """Unit test."""
    pass

@pytest.mark.integration
def test_integration_functionality():
    """Integration test."""
    pass

@pytest.mark.slow
def test_slow_operation():
    """Slow test."""
    pass

@pytest.mark.skip(reason="Requires real database")
def test_with_real_database():
    """Skipped test."""
    pass
```

## Mocking Best Practices

### Mock External Dependencies

```python
from unittest.mock import patch, MagicMock

@patch('main.psycopg2')
def test_with_mock_postgres(mock_psycopg2):
    """Test with mocked PostgreSQL."""
    mock_conn = MagicMock()
    mock_psycopg2.connect.return_value = mock_conn

    # Your test code here
```

### Use Fixtures for Common Mocks

```python
@pytest.fixture
def mock_database():
    """Reusable database mock."""
    mock = MagicMock()
    # Configure mock
    return mock

def test_with_fixture(mock_database):
    """Test using fixture."""
    # Use mock_database
```

## Continuous Integration

### GitHub Actions Example

```yaml
name: Tests

on: [push, pull_request]

jobs:
  test:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v2
      - name: Set up Python
        uses: actions/setup-python@v2
        with:
          python-version: '3.9'
      - name: Install dependencies
        run: |
          pip install -r requirements.txt
          pip install -r requirements-dev.txt
      - name: Run tests
        run: pytest -m "not integration" --cov=. --cov-report=xml
      - name: Upload coverage
        uses: codecov/codecov-action@v2
```

## Common Issues

### Import Errors

If you encounter import errors, ensure the project root is in your Python path:

```python
import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '../..'))
```

### Fixture Not Found

Ensure `conftest.py` is in the correct location and fixtures are properly defined.

### Mock Not Working

Verify the import path in the `@patch` decorator matches how the module imports dependencies:

```python
# If main.py does: import psycopg2
@patch('main.psycopg2')

# If main.py does: from psycopg2 import connect
@patch('main.connect')
```

## Test Coverage Goals

- **Unit tests**: Aim for >80% coverage of core functions
- **Integration tests**: Cover major pipeline workflows
- **Edge cases**: Test error handling and boundary conditions
- **Documentation**: All tests should have clear docstrings

## Contributing Tests

When contributing new functionality:

1. Write unit tests for new functions/classes
2. Add integration tests for new features
3. Update fixtures if needed
4. Document any new test patterns
5. Ensure all tests pass before submitting PR

## Resources

- [Pytest Documentation](https://docs.pytest.org/)
- [Apache Beam Testing](https://beam.apache.org/documentation/pipelines/test-your-pipeline/)
- [Python unittest.mock](https://docs.python.org/3/library/unittest.mock.html)
- [Coverage.py](https://coverage.readthedocs.io/)
