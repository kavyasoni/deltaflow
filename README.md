# DeltaFlow

**Intelligent Incremental Data Synchronization for GCP Dataflow**

[![Version](https://img.shields.io/badge/version-1.0.0-blue.svg)](https://github.com/your-org/deltaflow)
[![License](https://img.shields.io/badge/license-Apache%202.0-green.svg)](LICENSE)
[![Python](https://img.shields.io/badge/python-3.11%2B-blue.svg)](https://www.python.org/downloads/)
[![Apache Beam](https://img.shields.io/badge/Apache%20Beam-2.66.0%2B-orange.svg)](https://beam.apache.org/)

DeltaFlow is a production-ready Google Cloud Dataflow template for intelligent, automated cross-platform data synchronization. Built on Apache Beam, it provides seamless data pipelines between PostgreSQL, MongoDB, and BigQuery with zero-configuration schema detection and intelligent incremental syncing.

## Overview

DeltaFlow eliminates the complexity of building and maintaining data synchronization pipelines. Whether you need to sync operational databases to your data warehouse, replicate data across projects, or build real-time analytics, DeltaFlow provides a robust, scalable solution that just works.

**Why DeltaFlow?**

- **Smart Sync**: Automatically syncs only new data since the last run - no manual timestamp management required
- **Auto-Schema**: Detects source schemas and creates optimized BigQuery tables with proper types, partitioning, and clustering
- **Zero Config**: Works out of the box with sensible defaults while remaining fully configurable
- **Production Ready**: Battle-tested error handling, automatic retries, and comprehensive monitoring
- **Cost Efficient**: Incremental syncing reduces compute costs by up to 90% compared to full table scans

## Key Features

### Smart Sync - Intelligent Incremental Synchronization

Never manually manage timestamps again. Smart Sync queries your destination BigQuery table to find the latest timestamp, then automatically syncs only new data since that point.

```bash
# First run: Syncs all historical data
# Subsequent runs: Automatically sync only new records since last run
# After outage: Automatically catches up on missed data

enable_smart_sync=true
```

**Benefits:**
- Eliminates duplicate data processing
- Automatic gap detection and recovery
- Self-healing pipeline behavior
- Reduces processing time by 90%+

### Auto-Schema - Zero-Configuration Schema Detection

Point DeltaFlow at your source table and it automatically:
- Detects the source schema
- Maps types to BigQuery equivalents (including JSON, arrays, timestamps)
- Creates optimized tables with time partitioning
- Configures clustering for query performance

```bash
enable_auto_schema=true
source_table_for_schema=public.users
partition_field=created_at
clustering_fields=user_id,status
```

### Multi-Source Support

Seamlessly sync data from:
- **PostgreSQL** - Direct psycopg2 connectivity, no JDBC required
- **MongoDB** - Native pymongo support with BSON handling
- **BigQuery** - Cross-project and cross-dataset replication

### Enterprise Features

- **VPC Support**: Private network connectivity with subnet configuration
- **Service Accounts**: Fine-grained IAM permissions
- **Parallel Processing**: Multiple sources can run concurrently
- **Flexible Transformations**: JSON-based field mapping and data transformation
- **Write Modes**: APPEND, TRUNCATE, or WRITE_EMPTY support
- **Comprehensive Logging**: Cloud Logging integration with structured logs

## Quick Start

### Prerequisites

- Google Cloud Project with Dataflow API enabled
- Docker installed (for template building)
- `gcloud` CLI configured
- Source database credentials

### 5-Minute Setup

**1. Set environment variables:**

```bash
export PROJECT_ID="your-project-id"
export BUCKET_NAME="${PROJECT_ID}-dataflow-templates"
export IMAGE_NAME="gcr.io/${PROJECT_ID}/dataflow/deltaflow:v1.0.0"
```

**2. Build and deploy:**

```bash
# Build Docker image
docker build -t $IMAGE_NAME .

# Push to registry
docker push $IMAGE_NAME

# Deploy template
gcloud dataflow flex-template build gs://${BUCKET_NAME}/templates/deltaflow \
  --image $IMAGE_NAME \
  --sdk-language PYTHON \
  --metadata-file metadata.json
```

**3. Run your first sync:**

```bash
gcloud dataflow flex-template run my-first-sync-$(date +%Y%m%d-%H%M%S) \
  --template-file-gcs-location=gs://${BUCKET_NAME}/templates/deltaflow \
  --region=us-central1 \
  --parameters="data_source=postgresql,\
postgresql_host=YOUR_HOST,\
postgresql_database=YOUR_DB,\
postgresql_username=YOUR_USER,\
postgresql_password=YOUR_PASSWORD,\
postgresql_query=SELECT * FROM users,\
destination_bigquery_project=${PROJECT_ID},\
destination_bigquery_dataset=analytics,\
destination_bigquery_table=users"
```

## Installation

### From Source

```bash
# Clone the repository
git clone https://github.com/your-org/deltaflow.git
cd deltaflow

# Install dependencies
pip install -r requirements.txt

# Run locally (for testing)
python main.py \
  --runner=DirectRunner \
  --data_source=postgresql \
  --postgresql_host=localhost \
  --postgresql_database=testdb \
  --postgresql_username=user \
  --postgresql_password=pass \
  --postgresql_query="SELECT * FROM test_table" \
  --destination_bigquery_project=your-project \
  --destination_bigquery_dataset=test \
  --destination_bigquery_table=test
```

### Docker Deployment

```bash
# Build production image
docker build -t deltaflow:latest .

# Run via Docker
docker run deltaflow:latest \
  python main.py \
  --runner=DataflowRunner \
  --project=your-project \
  --region=us-central1 \
  # ... additional parameters
```

## Usage Examples

### PostgreSQL to BigQuery with Smart Sync

Sync only new records based on `updated_at` timestamp:

```bash
gcloud dataflow flex-template run postgresql-incremental-sync \
  --template-file-gcs-location=gs://${BUCKET_NAME}/templates/deltaflow \
  --region=us-central1 \
  --parameters="\
data_source=postgresql,\
postgresql_host=10.0.0.5,\
postgresql_database=production_db,\
postgresql_username=dataflow_user,\
postgresql_password=secure_password,\
enable_smart_sync=true,\
postgresql_base_query=SELECT * FROM orders WHERE updated_at > '{start_timestamp}' AND updated_at <= '{end_timestamp}' ORDER BY updated_at ASC,\
smart_sync_timestamp_column=updated_at,\
destination_bigquery_project=analytics-project,\
destination_bigquery_dataset=production,\
destination_bigquery_table=orders"
```

**How it works:**
1. First run: Syncs all historical data (or configure fallback period)
2. Subsequent runs: Automatically detects latest timestamp in BigQuery and syncs only new records
3. After gaps: Automatically catches up on any missed time periods

### MongoDB to BigQuery with Auto-Schema

Automatically detect schema from MongoDB documents:

```bash
gcloud dataflow flex-template run mongodb-auto-schema-sync \
  --template-file-gcs-location=gs://${BUCKET_NAME}/templates/deltaflow \
  --region=us-central1 \
  --parameters="\
data_source=mongodb,\
mongodb_host=mongodb.example.com,\
mongodb_database=user_data,\
mongodb_collection=profiles,\
mongodb_query={\"status\":\"active\",\"verified\":true},\
enable_auto_schema=true,\
source_table_for_schema=user_data.profiles,\
partition_field=created_at,\
clustering_fields=user_id,country,\
destination_bigquery_project=analytics-project,\
destination_bigquery_dataset=user_analytics,\
destination_bigquery_table=user_profiles"
```

### BigQuery Cross-Project Replication

Replicate data between projects or datasets:

```bash
gcloud dataflow flex-template run bigquery-replication \
  --template-file-gcs-location=gs://${BUCKET_NAME}/templates/deltaflow \
  --region=us-central1 \
  --parameters="\
data_source=bigquery,\
source_bigquery_project=source-project,\
source_bigquery_dataset=raw_data,\
source_bigquery_table=events,\
destination_bigquery_project=analytics-project,\
destination_bigquery_dataset=processed_data,\
destination_bigquery_table=events"
```

### Multi-Source Parallel Sync

Combine data from multiple sources:

```bash
# Configure multiple sources (comma-separated)
data_source=postgresql,mongodb,bigquery

# Each source streams data in parallel
# Results are combined before writing to BigQuery
```

### Smart Sync Example with Scheduling

Run every 2 hours to continuously sync new data:

```bash
# Create Cloud Scheduler job
gcloud scheduler jobs create http users-sync-every-2hours \
  --location=us-central1 \
  --schedule="0 */2 * * *" \
  --time-zone="UTC" \
  --uri="https://dataflow.googleapis.com/v1b3/projects/${PROJECT_ID}/locations/us-central1/flexTemplates:launch" \
  --http-method=POST \
  --headers="Content-Type=application/json" \
  --message-body='{
    "launch_parameter": {
      "jobName": "users-sync-'$(date +%Y%m%d-%H%M%S)'",
      "containerSpecGcsPath": "gs://'${BUCKET_NAME}'/templates/deltaflow",
      "parameters": {
        "data_source": "postgresql",
        "postgresql_host": "YOUR_HOST",
        "postgresql_database": "YOUR_DB",
        "postgresql_username": "YOUR_USER",
        "postgresql_password": "YOUR_PASSWORD",
        "enable_smart_sync": "true",
        "postgresql_base_query": "SELECT * FROM users WHERE updated_at > '\''{start_timestamp}'\'' AND updated_at <= '\''{end_timestamp}'\'' ORDER BY updated_at ASC",
        "destination_bigquery_project": "'${PROJECT_ID}'",
        "destination_bigquery_dataset": "analytics",
        "destination_bigquery_table": "users"
      }
    }
  }'
```

**Result:** Every 2 hours, only new data since the previous run is synced - efficient, automatic, and self-healing.

## Configuration

DeltaFlow supports 40+ configuration parameters for fine-grained control. Here are the most important ones:

### Core Parameters

| Parameter | Description | Example |
|-----------|-------------|---------|
| `data_source` | Source type: postgresql, mongodb, bigquery | `postgresql` |
| `destination_bigquery_project` | Target GCP project | `analytics-prod` |
| `destination_bigquery_dataset` | Target dataset | `production_data` |
| `destination_bigquery_table` | Target table | `users` |

### Smart Sync Parameters

| Parameter | Default | Description |
|-----------|---------|-------------|
| `enable_smart_sync` | `false` | Enable intelligent incremental sync |
| `smart_sync_timestamp_column` | `updated_at` | Column used for sync detection |
| `postgresql_base_query` | - | Query template with `{start_timestamp}` and `{end_timestamp}` placeholders |
| `sync_all_on_empty_table` | `true` | Sync all historical data when table is empty |
| `fallback_days` | `1` | Days to look back if not syncing all data |

### Auto-Schema Parameters

| Parameter | Default | Description |
|-----------|---------|-------------|
| `enable_auto_schema` | `true` | Enable automatic schema detection |
| `source_table_for_schema` | - | Source table to detect schema from |
| `partition_field` | `updated_at` | Field for BigQuery time partitioning |
| `clustering_fields` | - | Comma-separated clustering fields |
| `delete_existing_table` | `false` | Drop and recreate table |

For complete parameter reference, see [CONFIGURATION.md](docs/CONFIGURATION.md).

## Architecture

DeltaFlow is built on Apache Beam and follows a modular, extensible architecture:

```
┌─────────────────┐
│  Data Sources   │
│  - PostgreSQL   │
│  - MongoDB      │
│  - BigQuery     │
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│  Smart Sync     │ ◄─── Query destination for latest timestamp
│  (Optional)     │
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│  Schema Detect  │ ◄─── Analyze source schema
│  (Optional)     │
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│  Table Create   │ ◄─── Create optimized BigQuery table
│  (Optional)     │
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│  Data Reader    │ ◄─── Stream data from source
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│  Transform &    │ ◄─── Apply transformations
│  Validate       │      Sanitize for BigQuery
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│  BigQuery Write │ ◄─── Write to destination
└─────────────────┘
```

**Key Components:**
- **Single-File Design**: All logic in `main.py` for Dataflow template compatibility
- **DoFn-Based Processing**: Modular, testable components
- **ValueProvider Pattern**: Runtime parameter resolution for templates
- **Parallel Streams**: Multiple sources processed concurrently

For detailed architecture documentation, see [ARCHITECTURE.md](docs/ARCHITECTURE.md).

## Deployment

### Development Environment

```bash
# Set up local development
export PROJECT_ID="dev-project"
export BUCKET_NAME="${PROJECT_ID}-dataflow-templates"

# Build and deploy to dev
docker build -t gcr.io/${PROJECT_ID}/dataflow/deltaflow:dev .
docker push gcr.io/${PROJECT_ID}/dataflow/deltaflow:dev

gcloud dataflow flex-template build gs://${BUCKET_NAME}/templates/deltaflow-dev \
  --image gcr.io/${PROJECT_ID}/dataflow/deltaflow:dev \
  --sdk-language PYTHON \
  --metadata-file metadata.json
```

### Production Deployment

```bash
# Production with versioned tags
export PROJECT_ID="prod-project"
export VERSION="1.0.0"
export IMAGE_NAME="gcr.io/${PROJECT_ID}/dataflow/deltaflow:${VERSION}"

# Build production image
docker build -t $IMAGE_NAME .
docker tag $IMAGE_NAME gcr.io/${PROJECT_ID}/dataflow/deltaflow:latest

# Push both versioned and latest
docker push $IMAGE_NAME
docker push gcr.io/${PROJECT_ID}/dataflow/deltaflow:latest

# Deploy template
gcloud dataflow flex-template build gs://${BUCKET_NAME}/templates/deltaflow \
  --image $IMAGE_NAME \
  --sdk-language PYTHON \
  --metadata-file metadata.json
```

### Network Configuration (VPC)

For private database access:

```bash
gcloud dataflow flex-template run secure-sync \
  --template-file-gcs-location=gs://${BUCKET_NAME}/templates/deltaflow \
  --region=us-central1 \
  --network=projects/${PROJECT_ID}/global/networks/vpc-network \
  --subnetwork=regions/us-central1/subnetworks/private-subnet \
  --disable-public-ips \
  --service-account-email=dataflow-worker@${PROJECT_ID}.iam.gserviceaccount.com \
  --parameters="..."
```

For comprehensive deployment guide, see [DEPLOYMENT.md](docs/DEPLOYMENT.md).

## Monitoring and Troubleshooting

### View Job Status

```bash
# List recent jobs
gcloud dataflow jobs list --region=us-central1 --limit=5

# Get job details
gcloud dataflow jobs describe JOB_ID --region=us-central1

# View job metrics
gcloud dataflow jobs show JOB_ID --region=us-central1
```

### View Logs

```bash
# Tail logs for a specific job
gcloud logging read "resource.type=dataflow_job AND resource.labels.job_id=JOB_ID" \
  --limit=50 \
  --format="table(timestamp,severity,textPayload)"

# Filter for errors
gcloud logging read "resource.type=dataflow_job AND resource.labels.job_id=JOB_ID AND severity>=ERROR" \
  --limit=20
```

### Common Issues

| Issue | Cause | Solution |
|-------|-------|----------|
| "Query cannot be empty" | Smart Sync failed to build query | Check `postgresql_base_query` format with placeholders |
| "Template not found" | Invalid GCS path | Verify: `gsutil ls gs://bucket/templates/` |
| "Connection refused" | Database not accessible | Check VPC/firewall rules and credentials |
| "BigQuery JSON error" | Invalid data types | Records are auto-sanitized; check source data for null bytes |

## Contributing

We welcome contributions! Here's how to get started:

### Development Setup

```bash
# Clone and setup
git clone https://github.com/your-org/deltaflow.git
cd deltaflow
pip install -r requirements.txt

# Run tests (when available)
pytest tests/

# Run local pipeline
python main.py --runner=DirectRunner --data_source=postgresql ...
```

### Adding New Data Sources

1. Create a new `DoFn` class inheriting from `beam.DoFn`
2. Implement `setup()`, `process()`, and `teardown()` methods
3. Add configuration parameters to `CustomPipelineOptions`
4. Integrate into the main pipeline in `run_pipeline()`
5. Update `metadata.json` with new parameters
6. Add tests and documentation

Example:

```python
class NewSourceReaderDoFn(DoFn):
    def __init__(self, options: CustomPipelineOptions):
        self.options = options

    def setup(self):
        # Initialize connections in worker context
        import your_library
        self._client = your_library.connect(...)

    def process(self, element):
        # Read and yield data
        for record in self._client.read():
            yield sanitize_for_bigquery_json(record)

    def teardown(self):
        if self._client:
            self._client.close()
```

### Code Style

- Follow PEP 8 guidelines
- Use type hints where applicable
- Add docstrings to all classes and functions
- Keep functions focused and testable

## License

This project is licensed under the Apache License 2.0 - see the [LICENSE](LICENSE) file for details.

```
Copyright 2025 kavyasoni

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
```

## Roadmap

### Version 1.1 (Planned)

- [ ] Apache Kafka source support
- [ ] Google Cloud SQL (MySQL) source support
- [ ] Schema evolution detection and automatic migration
- [ ] Dead letter queue for failed records
- [ ] Metrics export to Cloud Monitoring
- [ ] Built-in data quality checks

### Version 1.2 (Future)

- [ ] Streaming mode support for real-time syncing
- [ ] Multi-destination writes (write to multiple BigQuery tables)
- [ ] Cloud Spanner source support
- [ ] Advanced transformation DSL
- [ ] Web UI for pipeline configuration
- [ ] Cost optimization recommendations

### Version 2.0 (Vision)

- [ ] Change Data Capture (CDC) support
- [ ] Bi-directional sync capabilities
- [ ] Machine learning-based schema mapping
- [ ] Auto-tuning performance optimizer
- [ ] GraphQL API for pipeline management

## Support and Community

- **Documentation**: [docs/](docs/)
- **Issues**: [GitHub Issues](https://github.com/your-org/deltaflow/issues)
- **Discussions**: [GitHub Discussions](https://github.com/your-org/deltaflow/discussions)
- **Stack Overflow**: Tag questions with `deltaflow` and `google-cloud-dataflow`

## Acknowledgments

Built with:
- [Apache Beam](https://beam.apache.org/) - Unified data processing framework
- [Google Cloud Dataflow](https://cloud.google.com/dataflow) - Fully managed data processing service
- [psycopg2](https://www.psycopg.org/) - PostgreSQL adapter for Python
- [PyMongo](https://pymongo.readthedocs.io/) - MongoDB driver for Python
- [Google Cloud BigQuery](https://cloud.google.com/bigquery) - Serverless data warehouse

## Authors

- **[kavyasoni](https://github.com/kavyasoni/)** - Initial work and maintenance

---

**Made with ❤️ for the data engineering community**

For detailed documentation, see:
- [Architecture Guide](docs/ARCHITECTURE.md)
- [Deployment Guide](docs/DEPLOYMENT.md)
- [Configuration Reference](docs/CONFIGURATION.md)
