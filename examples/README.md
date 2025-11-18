# DeltaFlow Examples and Scripts

This directory contains comprehensive examples, configuration files, and scripts for using DeltaFlow in development and production environments.

## 📁 Directory Contents

### Configuration Examples

- **config_postgresql.yaml** - Complete PostgreSQL to BigQuery sync configuration with all parameters documented
- **config_mongodb.yaml** - MongoDB to BigQuery sync configuration with BSON handling examples
- **config_smart_sync.yaml** - Smart Sync incremental update configuration with Cloud Scheduler integration

### Executable Scripts

- **run_from_python.py** - Python script demonstrating programmatic pipeline execution (6 examples)
- **local_test.sh** - Shell script for local testing with DirectRunner (5 test scenarios)
- **production_deployment.sh** - Automated production deployment script (Docker build, template deployment, IAM setup)

## 🚀 Quick Start

### 1. Local Testing (DirectRunner)

Test the pipeline locally before deploying to GCP:

```bash
# Run all local tests
cd examples
./local_test.sh all

# Run specific test
./local_test.sh 1  # PostgreSQL basic sync
./local_test.sh 2  # Auto-schema detection
./local_test.sh 4  # Smart Sync
```

**Prerequisites:**
- Python 3.9+ with dependencies installed (`pip install -r ../requirements.txt`)
- Local PostgreSQL instance (for PostgreSQL tests)
- GCP credentials configured (`gcloud auth application-default login`)

### 2. Python Programmatic Usage

Run pipelines from Python code:

```bash
# Run example 1: Simple PostgreSQL sync
python run_from_python.py --example 1

# Run example 2: Smart Sync on Dataflow
python run_from_python.py --example 2

# Run all examples
python run_from_python.py --all
```

**Use Cases:**
- Integration with existing Python applications
- Custom orchestration workflows
- Automated testing
- CI/CD pipeline integration

### 3. Production Deployment

Deploy DeltaFlow template to GCP:

```bash
# Full deployment (builds Docker image, creates template, sets up IAM)
export PROJECT_ID="your-gcp-project"
export REGION="us-central1"
./production_deployment.sh deploy

# Or step by step:
./production_deployment.sh build-only        # Just build Docker image
./production_deployment.sh template-only     # Just deploy template
```

**What it does:**
1. Enables required GCP APIs
2. Creates GCS bucket for templates
3. Creates service account with necessary permissions
4. Builds Docker image
5. Pushes image to Google Container Registry
6. Deploys Dataflow Flex template
7. Optionally creates Cloud Scheduler jobs

## 📋 Configuration Files

### config_postgresql.yaml

Complete PostgreSQL sync configuration showing:
- All 40+ pipeline parameters
- Connection settings
- Auto-schema configuration
- Smart Sync setup (optional)
- Production best practices
- Security recommendations

**Usage:**
```bash
# Convert YAML to JSON for gcloud
python -c "import yaml, json; print(json.dumps(yaml.safe_load(open('config_postgresql.yaml'))))" > config.json

# Run with config file
gcloud dataflow flex-template run my-job \
  --template-file-gcs-location=gs://BUCKET/templates/TEMPLATE \
  --region=us-central1 \
  --parameters-file=config.json
```

### config_mongodb.yaml

MongoDB-specific configuration covering:
- MongoDB connection options (standalone, replica set, Atlas)
- Query filters and projections
- BSON type handling
- Nested object/array handling
- Performance tuning for large collections

**Key Features:**
- Connection string support for complex setups
- MongoDB Atlas compatibility
- Batch size optimization
- Schema-less collection handling

### config_smart_sync.yaml

Smart Sync incremental update configuration:
- Timestamp-based incremental sync
- Empty table behavior (full vs. partial backfill)
- Cloud Scheduler integration examples
- Monitoring and gap detection queries
- Troubleshooting tips

**Scheduling Examples:**
```bash
# Every 15 minutes
cron: "*/15 * * * *"

# Hourly
cron: "0 * * * *"

# Daily at 2 AM
cron: "0 2 * * *"

# Business hours only (9 AM - 5 PM, Mon-Fri)
cron: "0 9-17 * * 1-5"
```

## 🐍 Python Script Examples

The `run_from_python.py` script contains 6 comprehensive examples:

### Example 1: Simple PostgreSQL Sync (DirectRunner)
```python
pipeline = DeltaFlowPipeline(runner="DirectRunner")
result = pipeline.run_postgresql_sync(
    postgresql_host="localhost",
    postgresql_query="SELECT * FROM users LIMIT 100",
    destination_project="my-project",
    destination_dataset="test",
    destination_table="users"
)
```

### Example 2: Smart Sync on Dataflow
```python
pipeline = DeltaFlowPipeline(runner="DataflowRunner", project="my-project")
result = pipeline.run_postgresql_sync(
    enable_smart_sync=True,
    smart_sync_timestamp_column="updated_at",
    postgresql_base_query="SELECT * FROM events WHERE updated_at > '{start_timestamp}'",
    # ... other parameters
)
```

### Example 3: MongoDB Sync
```python
result = pipeline.run_mongodb_sync(
    mongodb_host="localhost",
    mongodb_database="app_db",
    mongodb_collection="events",
    mongodb_query='{"status": "active"}',
    # ... other parameters
)
```

See script for Examples 4-6 (dynamic configuration, multi-source, scheduled with retry).

## 🧪 Local Testing Script

The `local_test.sh` script provides 5 automated test scenarios:

### Test 1: Basic PostgreSQL Sync
Tests basic PostgreSQL to BigQuery synchronization without advanced features.

### Test 2: PostgreSQL with Auto-Schema
Tests automatic schema detection and BigQuery table creation with partitioning/clustering.

### Test 3: MongoDB Sync
Tests MongoDB to BigQuery synchronization with BSON type handling.

### Test 4: Smart Sync (Incremental)
Tests Smart Sync incremental updates:
1. Initial full sync
2. Incremental sync with timestamp filtering

### Test 5: Data Validation
Tests data validation and type conversion edge cases.

**Configuration:**
Set environment variables to customize test settings:
```bash
export TEST_GCP_PROJECT="my-test-project"
export TEST_DATASET="deltaflow_test"
export PG_HOST="localhost"
export PG_DATABASE="testdb"
export PG_USERNAME="postgres"
export PG_PASSWORD="postgres"

./local_test.sh all
```

## 🏭 Production Deployment Script

The `production_deployment.sh` automates the complete deployment process:

### Full Deployment
```bash
export PROJECT_ID="my-gcp-project"
export REGION="us-central1"
export TEMPLATE_NAME="kk-custom-data-sync-template"
export TEMPLATE_VERSION="v1.0.0"

./production_deployment.sh deploy
```

### What Gets Created
1. **GCS Bucket**: `${PROJECT_ID}-dataflow-templates`
   - `templates/` - Flex template files
   - `temp/` - Temporary Dataflow files
   - `staging/` - Staging files

2. **Service Account**: `dataflow-runner@${PROJECT_ID}.iam.gserviceaccount.com`
   - Roles: Dataflow Worker, BigQuery Data Editor, Storage Object Admin

3. **Docker Image**: `gcr.io/${PROJECT_ID}/dataflow/custom-data-sync-template:v1.0.0`

4. **Flex Template**: `gs://${BUCKET_NAME}/templates/${TEMPLATE_NAME}`

5. **Cloud Scheduler Jobs** (optional): Automated hourly/daily sync jobs

### Environment Variables
```bash
PROJECT_ID              # GCP project ID (required)
REGION                  # GCP region (default: us-central1)
TEMPLATE_NAME           # Template name (default: kk-custom-data-sync-template)
TEMPLATE_VERSION        # Template version (default: v1.0.0)
BUCKET_NAME             # GCS bucket (default: ${PROJECT_ID}-dataflow-templates)
SERVICE_ACCOUNT_NAME    # Service account name (default: dataflow-runner)
```

### Deployment Commands
```bash
# Full deployment
./production_deployment.sh deploy

# Build Docker image only
./production_deployment.sh build-only

# Deploy template only (assumes image exists)
./production_deployment.sh template-only

# Show help
./production_deployment.sh help
```

## 📊 Running Production Jobs

After deployment, run jobs using the template:

### Basic Job
```bash
gcloud dataflow flex-template run my-sync-job-$(date +%Y%m%d-%H%M%S) \
  --template-file-gcs-location=gs://${BUCKET_NAME}/templates/${TEMPLATE_NAME} \
  --region=us-central1 \
  --parameters="data_source=postgresql" \
  --parameters="postgresql_host=10.128.0.5" \
  --parameters="postgresql_database=production_db" \
  --parameters="postgresql_username=reader" \
  --parameters="postgresql_password=***" \
  --parameters="postgresql_query=SELECT * FROM users" \
  --parameters="destination_bigquery_project=${PROJECT_ID}" \
  --parameters="destination_bigquery_dataset=analytics" \
  --parameters="destination_bigquery_table=users"
```

### Smart Sync Job
```bash
gcloud dataflow flex-template run smart-sync-job-$(date +%Y%m%d-%H%M%S) \
  --template-file-gcs-location=gs://${BUCKET_NAME}/templates/${TEMPLATE_NAME} \
  --region=us-central1 \
  --parameters="enable_smart_sync=true" \
  --parameters="smart_sync_timestamp_column=updated_at" \
  --parameters="postgresql_base_query=SELECT * FROM events WHERE updated_at > '{start_timestamp}' AND updated_at <= '{end_timestamp}'" \
  # ... other parameters
```

### With Parameters File
```bash
# Use config file
gcloud dataflow flex-template run config-based-job-$(date +%Y%m%d-%H%M%S) \
  --template-file-gcs-location=gs://${BUCKET_NAME}/templates/${TEMPLATE_NAME} \
  --region=us-central1 \
  --parameters-file=config.json \
  --service-account-email=dataflow-runner@${PROJECT_ID}.iam.gserviceaccount.com \
  --max-workers=10
```

## 🔒 Security Best Practices

### 1. Credential Management
```bash
# Use Secret Manager instead of plain text passwords
# Store credentials:
echo -n "postgres_password" | gcloud secrets create postgres-password --data-file=-

# Reference in parameters:
--parameters="postgresql_password=$(gcloud secrets versions access latest --secret=postgres-password)"
```

### 2. Service Account Permissions
- Use dedicated service account per pipeline
- Grant minimum required permissions
- Enable audit logging
- Rotate credentials regularly

### 3. Network Security
```bash
# Use VPC peering or Private Service Connect
--parameters="subnetwork=regions/us-central1/subnetworks/my-subnet"
--parameters="use_public_ips=false"
```

### 4. Data Encryption
- All data encrypted in transit (TLS)
- BigQuery data encrypted at rest by default
- Consider Customer-Managed Encryption Keys (CMEK) for sensitive data

## 📈 Monitoring and Alerting

### View Job Status
```bash
# List recent jobs
gcloud dataflow jobs list --region=us-central1

# Describe specific job
gcloud dataflow jobs describe JOB_ID --region=us-central1

# View logs
gcloud logging read "resource.type=dataflow_job AND resource.labels.job_id=JOB_ID" --limit=50
```

### Set Up Alerts
Create alerts in Cloud Monitoring for:
- Job failures
- High worker count (cost control)
- Low throughput
- Sync latency > SLA

### BigQuery Monitoring Queries
```sql
-- Check sync freshness
SELECT
  MAX(updated_at) as last_synced_timestamp,
  TIMESTAMP_DIFF(CURRENT_TIMESTAMP(), MAX(updated_at), MINUTE) as minutes_behind
FROM `project.dataset.table`;

-- Detect sync gaps
WITH hourly_counts AS (
  SELECT
    TIMESTAMP_TRUNC(updated_at, HOUR) as hour,
    COUNT(*) as records
  FROM `project.dataset.table`
  WHERE updated_at >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 7 DAY)
  GROUP BY hour
)
SELECT hour, records
FROM hourly_counts
WHERE records = 0 OR records < 100  -- Adjust threshold
ORDER BY hour DESC;
```

## 🐛 Troubleshooting

### Common Issues

**1. "Query cannot be empty"**
- Check `postgresql_base_query` format
- Ensure placeholders `{start_timestamp}` and `{end_timestamp}` exist
- Verify Smart Sync is enabled if using base query

**2. "Template not found"**
- Verify GCS path: `gsutil ls gs://${BUCKET_NAME}/templates/`
- Check template deployment: `./production_deployment.sh template-only`
- Ensure template name matches

**3. "Permission denied"**
- Check service account has required roles
- Verify IAM bindings: `gcloud projects get-iam-policy ${PROJECT_ID}`
- Test BigQuery access: `bq ls --project_id=${PROJECT_ID}`

**4. "Connection refused" (PostgreSQL/MongoDB)**
- Verify database host is reachable from Dataflow workers
- Check firewall rules allow Dataflow worker IPs
- Test connection from Cloud Shell: `psql -h HOST -U USER -d DB`

**5. "BigQuery JSON validation error"**
- Check for null bytes in strings: `\x00`
- Verify UTF-8 encoding
- Review data types (bytes should be base64 encoded)

### Debug Mode
```bash
# Run locally with debug logging
python main.py \
  --runner=DirectRunner \
  --data_source=postgresql \
  --postgresql_query="SELECT * FROM users LIMIT 10" \
  # ... other parameters
  2>&1 | tee debug.log
```

## 📚 Additional Resources

- **Main Documentation**: `/home/user/deltaflow/README.md`
- **Development Guide**: `/home/user/deltaflow/CLAUDE.md`
- **Implementation Details**: `/home/user/deltaflow/IMPLEMENTATION_DETAILS.md`
- **Integration Tests**: `/home/user/deltaflow/tests/integration/`

## 🤝 Contributing

When adding new examples:
1. Follow existing naming conventions
2. Include comprehensive comments
3. Add error handling
4. Update this README
5. Test thoroughly before committing

## 📄 License

Apache 2.0 - See LICENSE file for details

---

**DeltaFlow Team** | Custom Google Cloud Dataflow Pipeline Template
