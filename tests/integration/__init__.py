"""
DeltaFlow Integration Tests Package

This package contains integration and end-to-end tests for DeltaFlow.

Test Files:
- conftest.py            - Pytest fixtures and test utilities
- test_pipeline.py       - Comprehensive E2E pipeline tests (8 tests)
- test_e2e_scenarios.py  - Real-world scenario tests (5 scenarios)

Test Markers:
- @pytest.mark.integration - Requires external services (PostgreSQL, MongoDB, BigQuery)
- @pytest.mark.e2e         - End-to-end tests with full pipeline
- @pytest.mark.unit        - Unit tests (no external dependencies)
- @pytest.mark.slow        - Tests that may take >10 seconds

Running Integration Tests:
    # Run all integration tests (with mocks)
    pytest tests/integration/

    # Run with real external services
    pytest tests/integration/ --integration

    # Run only E2E tests
    pytest tests/integration/ -m e2e

    # Run specific scenario
    pytest tests/integration/test_e2e_scenarios.py::test_daily_full_refresh_scenario

Environment Variables:
    TEST_PG_HOST          - PostgreSQL host (default: localhost)
    TEST_PG_PORT          - PostgreSQL port (default: 5432)
    TEST_PG_DATABASE      - PostgreSQL database (default: testdb)
    TEST_PG_USERNAME      - PostgreSQL username (default: postgres)
    TEST_PG_PASSWORD      - PostgreSQL password (default: postgres)
    TEST_MONGO_HOST       - MongoDB host (default: localhost)
    TEST_MONGO_PORT       - MongoDB port (default: 27017)
    TEST_BQ_PROJECT       - BigQuery project (default: test-project)
    TEST_BQ_DATASET       - BigQuery dataset (default: test_dataset)

Author: DeltaFlow Team
License: Apache 2.0
"""

__version__ = "1.0.0"
