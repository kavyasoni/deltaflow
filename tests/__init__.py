"""
DeltaFlow Tests Package

This package contains all tests for the DeltaFlow data synchronization pipeline.

Test Structure:
- integration/  - Integration and E2E tests requiring external services
- unit/         - Unit tests for individual components (to be added)

Running Tests:
    # Run all tests
    pytest tests/

    # Run only integration tests
    pytest tests/integration/

    # Run with integration flag (requires external services)
    pytest tests/ --integration

    # Run specific test file
    pytest tests/integration/test_pipeline.py

    # Run specific test
    pytest tests/integration/test_pipeline.py::test_full_postgresql_to_bigquery_pipeline

    # Run with verbose output
    pytest tests/ -v

    # Run with coverage
    pytest tests/ --cov=main --cov-report=html

Author: DeltaFlow Team
License: Apache 2.0
"""

__version__ = "1.0.0"
