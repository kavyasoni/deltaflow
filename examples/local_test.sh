#!/bin/bash
################################################################################
# DeltaFlow Local Testing Script
#
# This script provides comprehensive local testing capabilities using Apache
# Beam's DirectRunner. Use this for development, debugging, and validating
# pipeline changes before deploying to production Dataflow.
#
# Features:
# - Multiple test scenarios (PostgreSQL, MongoDB, Smart Sync)
# - Sample data generation
# - Validation of results
# - Error handling and logging
#
# Requirements:
# - Python 3.9+ with DeltaFlow dependencies installed
# - Local PostgreSQL instance (for PostgreSQL tests)
# - Local MongoDB instance (for MongoDB tests)
# - GCP credentials configured (for BigQuery)
#
# Author: DeltaFlow Team
# License: Apache 2.0
################################################################################

set -e  # Exit on error
set -u  # Exit on undefined variable

# =============================================================================
# CONFIGURATION
# =============================================================================

# Script directory
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Test configuration
TEST_GCP_PROJECT="${TEST_GCP_PROJECT:-my-test-project}"
TEST_DATASET="${TEST_DATASET:-deltaflow_test}"
LOG_DIR="${PROJECT_ROOT}/logs"
TIMESTAMP=$(date +%Y%m%d_%H%M%S)

# Database configuration (override with environment variables)
PG_HOST="${PG_HOST:-localhost}"
PG_PORT="${PG_PORT:-5432}"
PG_DATABASE="${PG_DATABASE:-testdb}"
PG_USERNAME="${PG_USERNAME:-postgres}"
PG_PASSWORD="${PG_PASSWORD:-postgres}"

MONGO_HOST="${MONGO_HOST:-localhost}"
MONGO_PORT="${MONGO_PORT:-27017}"
MONGO_DATABASE="${MONGO_DATABASE:-testdb}"
MONGO_COLLECTION="${MONGO_COLLECTION:-test_collection}"

# =============================================================================
# UTILITY FUNCTIONS
# =============================================================================

log_info() {
    echo -e "${BLUE}[INFO]${NC} $1"
}

log_success() {
    echo -e "${GREEN}[SUCCESS]${NC} $1"
}

log_warning() {
    echo -e "${YELLOW}[WARNING]${NC} $1"
}

log_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

print_header() {
    echo ""
    echo "================================================================================"
    echo "$1"
    echo "================================================================================"
    echo ""
}

check_command() {
    if ! command -v "$1" &> /dev/null; then
        log_error "$1 is not installed or not in PATH"
        return 1
    fi
    return 0
}

check_dependencies() {
    print_header "Checking Dependencies"

    local all_ok=true

    if check_command python3; then
        log_success "Python3: $(python3 --version)"
    else
        all_ok=false
    fi

    if check_command gcloud; then
        log_success "gcloud: $(gcloud --version | head -n1)"
    else
        log_warning "gcloud not found (needed for BigQuery access)"
        all_ok=false
    fi

    if python3 -c "import apache_beam" 2>/dev/null; then
        log_success "Apache Beam: installed"
    else
        log_error "Apache Beam not installed (pip install apache-beam[gcp])"
        all_ok=false
    fi

    if [ "$all_ok" = false ]; then
        log_error "Some dependencies are missing"
        exit 1
    fi

    log_success "All dependencies OK"
}

setup_test_environment() {
    print_header "Setting Up Test Environment"

    # Create log directory
    mkdir -p "$LOG_DIR"
    log_info "Log directory: $LOG_DIR"

    # Create test dataset in BigQuery (if it doesn't exist)
    log_info "Creating BigQuery test dataset: ${TEST_GCP_PROJECT}.${TEST_DATASET}"
    bq mk --project_id="$TEST_GCP_PROJECT" --dataset "$TEST_DATASET" 2>/dev/null || true

    log_success "Test environment ready"
}

cleanup_test_data() {
    print_header "Cleaning Up Test Data"

    log_info "Deleting test tables from BigQuery dataset: ${TEST_GCP_PROJECT}.${TEST_DATASET}"

    # List and delete all tables in test dataset
    bq ls --project_id="$TEST_GCP_PROJECT" --max_results=1000 "$TEST_DATASET" 2>/dev/null | \
        grep TABLE | awk '{print $1}' | \
        while read -r table; do
            log_info "Deleting table: $table"
            bq rm -f --project_id="$TEST_GCP_PROJECT" -t "${TEST_DATASET}.${table}"
        done

    log_success "Cleanup complete"
}

# =============================================================================
# TEST 1: Basic PostgreSQL Sync
# =============================================================================

test_postgresql_basic() {
    print_header "TEST 1: Basic PostgreSQL to BigQuery Sync"

    local test_table="postgres_basic_${TIMESTAMP}"
    local log_file="${LOG_DIR}/test1_${TIMESTAMP}.log"

    log_info "Test table: ${TEST_DATASET}.${test_table}"
    log_info "Log file: $log_file"

    # Run pipeline
    python3 "${PROJECT_ROOT}/main.py" \
        --runner=DirectRunner \
        --data_source=postgresql \
        --postgresql_host="$PG_HOST" \
        --postgresql_port="$PG_PORT" \
        --postgresql_database="$PG_DATABASE" \
        --postgresql_username="$PG_USERNAME" \
        --postgresql_password="$PG_PASSWORD" \
        --postgresql_query="SELECT * FROM users LIMIT 100" \
        --destination_bigquery_project="$TEST_GCP_PROJECT" \
        --destination_bigquery_dataset="$TEST_DATASET" \
        --destination_bigquery_table="$test_table" \
        --enable_auto_schema=false \
        --write_disposition=WRITE_TRUNCATE \
        2>&1 | tee "$log_file"

    # Validate results
    local row_count
    row_count=$(bq query --project_id="$TEST_GCP_PROJECT" --use_legacy_sql=false \
        "SELECT COUNT(*) as count FROM \`${TEST_GCP_PROJECT}.${TEST_DATASET}.${test_table}\`" | \
        grep -v "count" | grep -v "^-" | tr -d ' ')

    if [ "$row_count" -gt 0 ]; then
        log_success "Test passed! Synced $row_count rows"
    else
        log_error "Test failed! No rows synced"
        return 1
    fi
}

# =============================================================================
# TEST 2: PostgreSQL with Auto-Schema
# =============================================================================

test_postgresql_auto_schema() {
    print_header "TEST 2: PostgreSQL with Auto-Schema Detection"

    local test_table="postgres_auto_schema_${TIMESTAMP}"
    local log_file="${LOG_DIR}/test2_${TIMESTAMP}.log"

    log_info "Test table: ${TEST_DATASET}.${test_table}"
    log_info "Testing auto-schema detection and table creation"

    python3 "${PROJECT_ROOT}/main.py" \
        --runner=DirectRunner \
        --data_source=postgresql \
        --postgresql_host="$PG_HOST" \
        --postgresql_port="$PG_PORT" \
        --postgresql_database="$PG_DATABASE" \
        --postgresql_username="$PG_USERNAME" \
        --postgresql_password="$PG_PASSWORD" \
        --postgresql_query="SELECT * FROM users LIMIT 50" \
        --destination_bigquery_project="$TEST_GCP_PROJECT" \
        --destination_bigquery_dataset="$TEST_DATASET" \
        --destination_bigquery_table="$test_table" \
        --enable_auto_schema=true \
        --source_table_for_schema="public.users" \
        --partition_field="created_at" \
        --clustering_fields="id,email" \
        2>&1 | tee "$log_file"

    # Check if table was created with partitioning
    local table_info
    table_info=$(bq show --project_id="$TEST_GCP_PROJECT" --format=json "${TEST_DATASET}.${test_table}")

    if echo "$table_info" | grep -q "timePartitioning"; then
        log_success "Test passed! Table created with partitioning"
    else
        log_warning "Table created but partitioning not detected"
    fi
}

# =============================================================================
# TEST 3: MongoDB Sync
# =============================================================================

test_mongodb_basic() {
    print_header "TEST 3: MongoDB to BigQuery Sync"

    local test_table="mongodb_basic_${TIMESTAMP}"
    local log_file="${LOG_DIR}/test3_${TIMESTAMP}.log"

    log_info "Test table: ${TEST_DATASET}.${test_table}"
    log_info "MongoDB: ${MONGO_HOST}:${MONGO_PORT}/${MONGO_DATABASE}.${MONGO_COLLECTION}"

    python3 "${PROJECT_ROOT}/main.py" \
        --runner=DirectRunner \
        --data_source=mongodb \
        --mongodb_host="$MONGO_HOST" \
        --mongodb_port="$MONGO_PORT" \
        --mongodb_database="$MONGO_DATABASE" \
        --mongodb_collection="$MONGO_COLLECTION" \
        --mongodb_query='{"status": "active"}' \
        --destination_bigquery_project="$TEST_GCP_PROJECT" \
        --destination_bigquery_dataset="$TEST_DATASET" \
        --destination_bigquery_table="$test_table" \
        --enable_auto_schema=true \
        --source_table_for_schema="${MONGO_DATABASE}.${MONGO_COLLECTION}" \
        2>&1 | tee "$log_file"

    # Validate
    local row_count
    row_count=$(bq query --project_id="$TEST_GCP_PROJECT" --use_legacy_sql=false \
        "SELECT COUNT(*) as count FROM \`${TEST_GCP_PROJECT}.${TEST_DATASET}.${test_table}\`" | \
        grep -v "count" | grep -v "^-" | tr -d ' ')

    if [ "$row_count" -gt 0 ]; then
        log_success "Test passed! Synced $row_count MongoDB documents"
    else
        log_warning "No documents synced (collection may be empty)"
    fi
}

# =============================================================================
# TEST 4: Smart Sync (Incremental)
# =============================================================================

test_smart_sync() {
    print_header "TEST 4: Smart Sync (Incremental Updates)"

    local test_table="smart_sync_${TIMESTAMP}"
    local log_file="${LOG_DIR}/test4_${TIMESTAMP}.log"

    log_info "Test table: ${TEST_DATASET}.${test_table}"
    log_info "Running initial full sync..."

    # First run: Full sync
    python3 "${PROJECT_ROOT}/main.py" \
        --runner=DirectRunner \
        --data_source=postgresql \
        --postgresql_host="$PG_HOST" \
        --postgresql_port="$PG_PORT" \
        --postgresql_database="$PG_DATABASE" \
        --postgresql_username="$PG_USERNAME" \
        --postgresql_password="$PG_PASSWORD" \
        --postgresql_query="SELECT * FROM users WHERE created_at > '2024-01-01' LIMIT 100" \
        --destination_bigquery_project="$TEST_GCP_PROJECT" \
        --destination_bigquery_dataset="$TEST_DATASET" \
        --destination_bigquery_table="$test_table" \
        --enable_auto_schema=true \
        --source_table_for_schema="public.users" \
        2>&1 | tee "$log_file"

    local initial_count
    initial_count=$(bq query --project_id="$TEST_GCP_PROJECT" --use_legacy_sql=false \
        "SELECT COUNT(*) as count FROM \`${TEST_GCP_PROJECT}.${TEST_DATASET}.${test_table}\`" | \
        grep -v "count" | grep -v "^-" | tr -d ' ')

    log_info "Initial sync: $initial_count rows"

    # Second run: Incremental with Smart Sync
    log_info "Running incremental Smart Sync..."

    python3 "${PROJECT_ROOT}/main.py" \
        --runner=DirectRunner \
        --data_source=postgresql \
        --postgresql_host="$PG_HOST" \
        --postgresql_port="$PG_PORT" \
        --postgresql_database="$PG_DATABASE" \
        --postgresql_username="$PG_USERNAME" \
        --postgresql_password="$PG_PASSWORD" \
        --destination_bigquery_project="$TEST_GCP_PROJECT" \
        --destination_bigquery_dataset="$TEST_DATASET" \
        --destination_bigquery_table="$test_table" \
        --enable_smart_sync=true \
        --smart_sync_timestamp_column="updated_at" \
        --postgresql_base_query="SELECT * FROM users WHERE updated_at > '{start_timestamp}' AND updated_at <= '{end_timestamp}' ORDER BY updated_at" \
        --enable_auto_schema=false \
        2>&1 | tee -a "$log_file"

    log_success "Smart Sync test completed (check logs for query details)"
}

# =============================================================================
# TEST 5: Data Validation
# =============================================================================

test_data_validation() {
    print_header "TEST 5: Data Validation and Transformation"

    local test_table="validation_test_${TIMESTAMP}"
    local log_file="${LOG_DIR}/test5_${TIMESTAMP}.log"

    log_info "Testing data validation and type conversions"

    python3 "${PROJECT_ROOT}/main.py" \
        --runner=DirectRunner \
        --data_source=postgresql \
        --postgresql_host="$PG_HOST" \
        --postgresql_port="$PG_PORT" \
        --postgresql_database="$PG_DATABASE" \
        --postgresql_username="$PG_USERNAME" \
        --postgresql_password="$PG_PASSWORD" \
        --postgresql_query="SELECT id, name, email, metadata, tags, created_at FROM users LIMIT 20" \
        --destination_bigquery_project="$TEST_GCP_PROJECT" \
        --destination_bigquery_dataset="$TEST_DATASET" \
        --destination_bigquery_table="$test_table" \
        2>&1 | tee "$log_file"

    # Check for validation errors in logs
    if grep -q "validation error" "$log_file"; then
        log_warning "Validation errors found (check logs)"
    else
        log_success "Data validation passed"
    fi
}

# =============================================================================
# MAIN EXECUTION
# =============================================================================

main() {
    print_header "DeltaFlow Local Testing Suite"

    log_info "Project: $TEST_GCP_PROJECT"
    log_info "Dataset: $TEST_DATASET"
    log_info "Timestamp: $TIMESTAMP"

    # Parse arguments
    local test_number="${1:-all}"
    local skip_cleanup="${2:-false}"

    # Check dependencies
    check_dependencies

    # Setup
    setup_test_environment

    # Run tests
    case "$test_number" in
        1)
            test_postgresql_basic
            ;;
        2)
            test_postgresql_auto_schema
            ;;
        3)
            test_mongodb_basic
            ;;
        4)
            test_smart_sync
            ;;
        5)
            test_data_validation
            ;;
        all)
            log_info "Running all tests..."
            test_postgresql_basic || log_warning "Test 1 failed"
            test_postgresql_auto_schema || log_warning "Test 2 failed"
            # test_mongodb_basic || log_warning "Test 3 failed (MongoDB may not be available)"
            test_smart_sync || log_warning "Test 4 failed"
            test_data_validation || log_warning "Test 5 failed"
            ;;
        *)
            echo "Usage: $0 [test_number|all] [skip_cleanup]"
            echo ""
            echo "Tests:"
            echo "  1 - Basic PostgreSQL Sync"
            echo "  2 - PostgreSQL with Auto-Schema"
            echo "  3 - MongoDB Sync"
            echo "  4 - Smart Sync (Incremental)"
            echo "  5 - Data Validation"
            echo "  all - Run all tests"
            echo ""
            echo "Examples:"
            echo "  $0 1              # Run test 1 only"
            echo "  $0 all            # Run all tests"
            echo "  $0 all true       # Run all tests, skip cleanup"
            exit 1
            ;;
    esac

    # Cleanup (unless skipped)
    if [ "$skip_cleanup" != "true" ]; then
        read -p "Clean up test data? (y/N) " -n 1 -r
        echo
        if [[ $REPLY =~ ^[Yy]$ ]]; then
            cleanup_test_data
        fi
    fi

    print_header "Testing Complete"
    log_success "Logs saved to: $LOG_DIR"
}

# Run main function
main "$@"
