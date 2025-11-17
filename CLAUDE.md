# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

This is a **Custom Google Cloud Dataflow Pipeline Template** (v1.0.0) for cross-platform data synchronization. The codebase is a production-ready Apache Beam pipeline that syncs data between PostgreSQL, MongoDB, and BigQuery with intelligent features like Smart Sync (timestamp-based incremental updates) and Auto-Schema (automatic table creation with schema detection).

**Core file**: `main.py` (2,439 lines) - All pipeline logic is contained in this single file.

## Development Commands

### Build and Deploy

```bash
# Set environment variables first
export PROJECT_ID="your-project-id"
export TEMPLATE_NAME="kk-custom-data-sync-template"
export BUCKET_NAME="${PROJECT_ID}-dataflow-templates"
export IMAGE_NAME="gcr.io/${PROJECT_ID}/dataflow/custom-data-sync-template:v1.0.0"

# Build Docker image
docker build -t $IMAGE_NAME .

# Push to Google Container Registry
docker push $IMAGE_NAME

# Deploy template to GCS
gcloud dataflow flex-template build gs://${BUCKET_NAME}/templates/${TEMPLATE_NAME} \
  --image $IMAGE_NAME \
  --sdk-language PYTHON \
  --metadata-file metadata.json
```

### Run Pipeline Locally (for testing)

```bash
# Direct run for testing (not using template)
python main.py \
  --runner=DirectRunner \
  --data_source=postgresql \
  --postgresql_host=localhost \
  --postgresql_database=testdb \
  --postgresql_username=user \
  --postgresql_password=pass \
  --postgresql_query="SELECT * FROM test_table" \
  --destination_bigquery_project=your-project \
  --destination_bigquery_dataset=test_dataset \
  --destination_bigquery_table=test_table
```

### Run via Dataflow Template

```bash
gcloud dataflow flex-template run test-job-$(date +%Y%m%d-%H%M%S) \
  --template-file-gcs-location=gs://${BUCKET_NAME}/templates/${TEMPLATE_NAME} \
  --region=us-central1 \
  --parameters="data_source=postgresql,postgresql_host=HOST,postgresql_database=DB,postgresql_username=USER,postgresql_password=PASS,postgresql_query=SELECT * FROM table,destination_bigquery_project=PROJECT,destination_bigquery_dataset=DATASET,destination_bigquery_table=TABLE"
```

## Architecture Overview

### Single-File Design
All pipeline logic lives in `main.py`. This is intentional for Dataflow template deployment where all code must be self-contained.

### Key Components (all in main.py)

1. **CustomPipelineOptions** (lines 240-480): Defines 40+ configurable parameters using Apache Beam's PipelineOptions
2. **Data Readers** (DoFn classes):
   - `PostgreSQLReaderDoFn` (lines 482-608): Direct psycopg2 connection, avoids JDBC issues
   - `MongoDBReadDoFn` (lines 1726-1894): pymongo-based reader with BSON handling
   - BigQuery reader: Uses built-in `ReadFromBigQuery` (lines 2331-2363)

3. **Smart Sync** (lines 903-1224):
   - `SmartSyncDoFn`: Queries destination BigQuery table for `MAX(timestamp_column)`
   - `build_smart_postgresql_query()`: Constructs dynamic queries with timestamp filters
   - Enables incremental syncs - only new data since last run
   - Handles empty tables by syncing all historical data

4. **Auto-Schema Detection** (lines 1242-1480):
   - `PostgreSQLSchemaDetectionDoFn`: Queries information_schema for column metadata
   - `MongoDBSchemaDetectionDoFn`: Samples documents to infer schema
   - `BigQuerySchemaDetectionDoFn`: Reads existing BigQuery table schema
   - `BigQueryTableCreationDoFn` (lines 1897-2141): Creates optimized BigQuery tables with partitioning/clustering

5. **Data Transformation & Validation** (lines 610-901):
   - `DataTransformationDoFn`: Applies field mappings, type conversions
   - `DataValidationDoFn`: Validates data quality and BigQuery JSON compatibility
   - Utility functions: `sanitize_for_bigquery_json()`, `parse_postgres_array_string()`

6. **Main Pipeline** (lines 2143-2439): Orchestrates all components

### Critical Pattern: ValueProvider Resolution

**IMPORTANT**: All parameters are `ValueProvider` objects for runtime resolution. Always use the `safe_get_param()` helper:

```python
def safe_get_param(param):
    """Handles both ValueProvider and direct string cases"""
    if param is None:
        return None
    if hasattr(param, 'get'):
        return param.get()  # Runtime resolution
    return param
```

This pattern is used throughout `setup()` methods in DoFn classes because worker processes need to resolve parameters at runtime.

### Data Flow Pattern

```
Source → Smart Sync (optional) → Schema Detection (optional) →
Table Creation (optional) → Data Reading → Transformation →
Validation → BigQuery Write
```

Multiple sources can run in parallel and are combined with `beam.Flatten()`.

## Smart Sync Logic (Critical Feature)

**When enabled** (`enable_smart_sync=true`):

1. Queries destination BigQuery: `SELECT MAX(updated_at) FROM destination_table`
2. Uses result as `start_timestamp` in source query
3. Sets `end_timestamp` to current UTC time
4. Constructs query: `WHERE updated_at > '{start_timestamp}' AND updated_at <= '{end_timestamp}'`

**Empty table behavior**:
- If `sync_all_on_empty_table=true` (default): Uses `start_timestamp='1900-01-01 00:00:00'` to sync all historical data
- If `false`: Uses `fallback_days` parameter (default 1 day) to sync recent data

**Query template format** (required parameter: `postgresql_base_query`):
```sql
SELECT * FROM table
WHERE updated_at > '{start_timestamp}'
AND updated_at <= '{end_timestamp}'
ORDER BY updated_at ASC
```

## Auto-Schema Logic (Critical Feature)

**When enabled** (`enable_auto_schema=true`, default):

1. Connects to source database and extracts schema metadata
2. Maps source types to BigQuery types (comprehensive mapping in `_map_schema_to_bigquery()`)
3. Creates BigQuery table with:
   - Time partitioning on `partition_field` (default: `updated_at`)
   - Clustering on `clustering_fields` (comma-separated list)
4. Only includes source table columns - no metadata fields

**Type mappings** (examples):
- PostgreSQL `character varying` → BigQuery `STRING`
- PostgreSQL `jsonb` → BigQuery `JSON`
- PostgreSQL `timestamp without time zone` → BigQuery `TIMESTAMP`
- MongoDB `objectId` → BigQuery `STRING`
- MongoDB `object` → BigQuery `JSON`

## Common Patterns

### Adding New Data Source Support

1. Create new DoFn class inheriting from `beam.DoFn`
2. Implement `setup()`, `process()`, `teardown()` methods
3. Add configuration parameters to `CustomPipelineOptions._add_argparse_args()`
4. Integrate into `run_pipeline()` with parallel stream pattern
5. Add to `data_source` parameter options in `metadata.json`

### Data Type Handling for BigQuery

**Critical**: BigQuery is strict about JSON compatibility. Use these utilities:
- `sanitize_for_bigquery_json()`: Handles bytes, dates, null bytes, nested objects
- `validate_json_for_bigquery()`: Pre-validates records before write
- PostgreSQL arrays: Use `parse_postgres_array_string()` for `{a,b,c}` literals

### Connection Management Pattern

```python
class MyReaderDoFn(DoFn):
    def setup(self):
        # Import libraries in worker context
        import library
        # Create connections
        self._connection = library.connect(...)

    def process(self, element):
        # Use self._connection
        pass

    def teardown(self):
        # Clean up
        if self._connection:
            self._connection.close()
```

**Why**: Worker processes serialize DoFn instances, so connections must be created per-worker in `setup()`, not `__init__()`.

## Configuration Files

- **metadata.json**: Dataflow template metadata with parameter definitions (required for template deployment)
- **requirements.txt**: Python dependencies (Apache Beam 2.66.0+, psycopg2, pymongo, google-cloud-bigquery)
- **Dockerfile**: Multi-stage build using `gcr.io/dataflow-templates-base/python3-template-launcher-base`
- **setup.py**: Standard Python package configuration

## Important Constraints

1. **No external modules**: All code must be in `main.py` or installed via `requirements.txt` (Dataflow template limitation)
2. **ValueProvider usage**: All runtime parameters must use `ValueProvider` type and be resolved with `safe_get_param()`
3. **Worker context imports**: Heavy libraries (psycopg2, pymongo) must be imported in `setup()` methods, not at module level
4. **BigQuery JSON compatibility**: All data must pass `validate_json_for_bigquery()` before write
5. **Query validation**: PostgreSQL queries cannot be empty strings (validated in `PostgreSQLReaderDoFn.__init__()`)

## Monitoring and Debugging

### View job status
```bash
gcloud dataflow jobs list --region=us-central1
gcloud dataflow jobs describe JOB_ID --region=us-central1
```

### View logs
```bash
gcloud logging read "resource.type=dataflow_job AND resource.labels.job_id=JOB_ID" --limit=50
```

### Common issues
- **"Query cannot be empty"**: Smart Sync failed to build query; check `postgresql_base_query` format
- **"Template not found"**: Verify GCS path and template deployment
- **"JDBC expansion service"**: Not used - we use direct psycopg2 connections instead
- **"BigQuery JSON validation error"**: Check for null bytes, invalid UTF-8, or incompatible types

## Production Deployment Pattern

The template is designed for Cloud Scheduler automation:

```bash
# Create scheduler for automated runs
gcloud scheduler jobs create http data-sync-every-2hours \
  --location=us-central1 \
  --schedule="0 */2 * * *" \
  --time-zone="UTC" \
  --uri="https://dataflow.googleapis.com/v1b3/projects/PROJECT/locations/us-central1/flexTemplates:launch" \
  --http-method=POST \
  --message-body='{"launch_parameter": {...}}'
```

Smart Sync makes scheduled runs efficient - each run only processes new data since the previous run.

## Testing Strategy

No test files exist yet. When adding tests:
- Use `DirectRunner` for local integration tests
- Mock database connections for unit tests
- Test Smart Sync logic with different timestamp scenarios
- Test Auto-Schema with various source schemas
- Validate BigQuery type mappings

## Key Dependencies

- **Apache Beam 2.66.0+**: Core framework (critical: 2.66.0 fixes data loss bug)
- **psycopg2-binary 2.9.9**: PostgreSQL connectivity
- **pymongo 4.8.0**: MongoDB connectivity
- **google-cloud-bigquery 3.25.0**: BigQuery operations
- **Python 3.9+**: Required runtime version

## Security Notes

- Credentials passed via parameters (use Secret Manager in production)
- VPC/subnet configuration supported for private network access
- Service account permissions required: Dataflow Worker, BigQuery Data Editor, Storage Object Admin
