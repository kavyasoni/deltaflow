# Custom Dataflow Pipeline Template v1.0.0

A production-ready Google Cloud Dataflow template for automated cross-platform data synchronization with intelligent schema detection and smart sync capabilities.

## Overview

This template provides seamless data synchronization between PostgreSQL, MongoDB, and BigQuery with automatic schema detection, intelligent timestamp-based synchronization, and comprehensive error handling.

**Key Features:**

- **Smart Sync**: Dynamic timestamp-based data synchronization
- **Auto-Schema**: Automatic BigQuery table creation from source schemas
- **Multi-Source**: PostgreSQL, MongoDB, and BigQuery support
- **Cross-Project**: Secure VPC network configuration
- **Production-Ready**: Comprehensive monitoring and error handling

## Quick Start

### Prerequisites

- Google Cloud Project with Dataflow API enabled
- Docker installed for template building
- `gcloud` CLI authenticated
- Source database access credentials

### Basic Usage

```bash
gcloud dataflow flex-template run your-job-name-$(date +%Y%m%d-%H%M%S) \
  --template-file-gcs-location=gs://your-bucket/templates/kk-custom-data-sync-template \
  --region=us-central1 \
  --parameters="data_source=postgresql,postgresql_host=YOUR_HOST,postgresql_database=YOUR_DB,postgresql_username=YOUR_USER,postgresql_password=YOUR_PASSWORD,postgresql_query=SELECT * FROM your_table,destination_bigquery_project=kk-data-analytics,destination_bigquery_dataset=YOUR_DATASET,destination_bigquery_table=YOUR_TABLE"
```

## Template Parameters

### Core Parameters

| Parameter                      | Type   | Required | Description                                           | Example Value          |
| ------------------------------ | ------ | -------- | ----------------------------------------------------- | ---------------------- |
| `data_source`                  | String | Yes      | Data source type: `postgresql`, `mongodb`, `bigquery` | `postgresql`           |
| `destination_bigquery_project` | String | Yes      | BigQuery destination project ID                       | `my-analytics-project` |
| `destination_bigquery_dataset` | String | Yes      | BigQuery destination dataset                          | `production_data`      |
| `destination_bigquery_table`   | String | Yes      | BigQuery destination table                            | `organizations`        |

### PostgreSQL Parameters

| Parameter             | Type   | Required      | Description                     | Example Value                                     |
| --------------------- | ------ | ------------- | ------------------------------- | ------------------------------------------------- |
| `postgresql_host`     | String | Yes           | PostgreSQL host IP or hostname  | `35.226.248.206` or `db.example.com`              |
| `postgresql_port`     | String | No            | PostgreSQL port (default: 5432) | `5432`                                            |
| `postgresql_database` | String | Yes           | PostgreSQL database name        | `user_management`                                 |
| `postgresql_username` | String | Yes           | PostgreSQL username             | `dataflow_user`                                   |
| `postgresql_password` | String | Yes           | PostgreSQL password             | `secure_password123`                              |
| `postgresql_query`    | String | Conditional\* | SQL query to execute            | `SELECT * FROM organizations WHERE active = true` |

\***Required when:** `enable_smart_sync=false` OR when `enable_smart_sync=true` but no `postgresql_base_query` provided\*

### Smart Sync Parameters

| Parameter                     | Type   | Required        | Description                                                | Example Value                                                                                                                    |
| ----------------------------- | ------ | --------------- | ---------------------------------------------------------- | -------------------------------------------------------------------------------------------------------------------------------- |
| `enable_smart_sync`           | String | No              | Enable smart sync (`true`/`false`, default: `false`)       | `true`                                                                                                                           |
| `postgresql_base_query`       | String | Conditional\*\* | Base query template with timestamp placeholders            | `SELECT * FROM organizations WHERE updated_at > '{start_timestamp}' AND updated_at <= '{end_timestamp}' ORDER BY updated_at ASC` |
| `smart_sync_timestamp_column` | String | No              | Timestamp column for sync (default: `updated_at`)          | `updated_at` or `last_modified`                                                                                                  |
| `sync_all_on_empty_table`     | String | No              | Sync all data when empty (`true`/`false`, default: `true`) | `true`                                                                                                                           |
| `fallback_days`               | String | No              | Days to look back for fallback (default: `1`)              | `1` (1 day) or `7` (1 week)                                                                                                      |

\*\***Required when:** `enable_smart_sync=true` (recommended for Smart Sync functionality)\*

### Auto-Schema Parameters

| Parameter                 | Type   | Required | Description                                                              | Example Value                   |
| ------------------------- | ------ | -------- | ------------------------------------------------------------------------ | ------------------------------- |
| `enable_auto_schema`      | String | No       | Enable auto schema detection (`true`/`false`, default: `false`)          | `true`                          |
| `source_table_for_schema` | String | No       | Source table for schema detection                                        | `public.organizations`          |
| `partition_field`         | String | No       | BigQuery partition field (default: `updated_at`)                         | `updated_at` or `created_at`    |
| `clustering_fields`       | String | No       | Comma-separated clustering fields                                        | `id,status` or `user_id,region` |
| `delete_existing_table`   | String | No       | Delete existing table before creating (`true`/`false`, default: `false`) | `false`                         |

### MongoDB Parameters

| Parameter                   | Type   | Required | Description                        | Example Value                               |
| --------------------------- | ------ | -------- | ---------------------------------- | ------------------------------------------- |
| `mongodb_host`              | String | Yes\*    | MongoDB host IP or hostname        | `mongodb.example.com` or `10.0.0.100`       |
| `mongodb_port`              | String | No       | MongoDB port (default: 27017)      | `27017`                                     |
| `mongodb_database`          | String | Yes\*    | MongoDB database name              | `user_data`                                 |
| `mongodb_collection`        | String | Yes\*    | MongoDB collection name            | `users`                                     |
| `mongodb_username`          | String | No       | MongoDB username (optional)        | `dataflow_user`                             |
| `mongodb_password`          | String | No       | MongoDB password (optional)        | `secure_password123`                        |
| `mongodb_connection_string` | String | No       | Complete MongoDB connection string | `mongodb://user:pass@host:27017/db`         |
| `mongodb_query`             | String | No       | MongoDB query as JSON string       | `{"status": "active", "age": {"$gte": 18}}` |
| `mongodb_projection`        | String | No       | MongoDB projection as JSON string  | `{"name": 1, "email": 1, "status": 1}`      |

\*Required when `data_source=mongodb`

### BigQuery Source Parameters

| Parameter                 | Type   | Required | Description                               | Example Value                                                                     |
| ------------------------- | ------ | -------- | ----------------------------------------- | --------------------------------------------------------------------------------- |
| `source_bigquery_project` | String | Yes\*    | Source BigQuery project ID                | `source-data-project`                                                             |
| `source_bigquery_dataset` | String | Yes\*    | Source BigQuery dataset name              | `raw_data`                                                                        |
| `source_bigquery_table`   | String | Yes\*    | Source BigQuery table name                | `user_events`                                                                     |
| `source_bigquery_query`   | String | No       | BigQuery SQL query (alternative to table) | `SELECT * FROM \`project.dataset.table\` WHERE DATE(created_at) = CURRENT_DATE()` |

\*Required when `data_source=bigquery`

### Additional Parameters

| Parameter               | Type   | Required | Description                                        | Example Value                                  |
| ----------------------- | ------ | -------- | -------------------------------------------------- | ---------------------------------------------- |
| `transformation_config` | String | No       | JSON transformation configuration                  | `{"field_mappings": {"old_name": "new_name"}}` |
| `write_disposition`     | String | No       | BigQuery write mode (default: `WRITE_APPEND`)      | `WRITE_APPEND` or `WRITE_TRUNCATE`             |
| `create_disposition`    | String | No       | BigQuery create mode (default: `CREATE_IF_NEEDED`) | `CREATE_IF_NEEDED` or `CREATE_NEVER`           |
| `batch_size`            | Number | No       | Batch size for processing (default: 1000)          | `1000` or `500`                                |
| `max_retries`           | Number | No       | Max retries for failures (default: 3)              | `3` or `5`                                     |

## Production Examples

### PostgreSQL with Smart Sync + Auto-Schema

```bash
gcloud dataflow flex-template run postgresql-sync-$(date +%Y%m%d-%H%M%S) \
  --template-file-gcs-location=gs://your-bucket/templates/kk-custom-data-sync-template \
  --region=us-central1 \
  --network=your-network \
  --subnetwork=regions/us-central1/subnetworks/your-subnet \
  --disable-public-ips \
  --service-account-email=your-service-account@project.iam.gserviceaccount.com \
  --parameters="\
data_source=postgresql,\
postgresql_host=YOUR_HOST,\
postgresql_database=YOUR_DATABASE,\
postgresql_username=YOUR_USER,\
postgresql_password=YOUR_PASSWORD,\
enable_smart_sync=true,\
postgresql_base_query=SELECT * FROM your_table WHERE updated_at > '{start_timestamp}' AND updated_at <= '{end_timestamp}' ORDER BY updated_at ASC,\
enable_auto_schema=true,\
source_table_for_schema=schema.table_name,\
partition_field=updated_at,\
clustering_fields=id,status,\
destination_bigquery_project=kk-data-analytics,\
destination_bigquery_dataset=YOUR_DATASET,\
destination_bigquery_table=YOUR_TABLE"
```

### MongoDB to BigQuery

```bash
gcloud dataflow flex-template run mongodb-sync-$(date +%Y%m%d-%H%M%S) \
  --template-file-gcs-location=gs://your-bucket/templates/kk-custom-data-sync-template \
  --region=us-central1 \
  --parameters="\
data_source=mongodb,\
mongodb_host=YOUR_MONGO_HOST,\
mongodb_database=YOUR_MONGO_DB,\
mongodb_collection=YOUR_COLLECTION,\
mongodb_query={\"status\":\"active\"},\
destination_bigquery_project=kk-data-analytics,\
destination_bigquery_dataset=YOUR_DATASET,\
destination_bigquery_table=YOUR_TABLE"
```

### BigQuery to BigQuery

```bash
gcloud dataflow flex-template run bigquery-sync-$(date +%Y%m%d-%H%M%S) \
  --template-file-gcs-location=gs://your-bucket/templates/kk-custom-data-sync-template \
  --region=us-central1 \
  --parameters="\
data_source=bigquery,\
source_bigquery_project=SOURCE_PROJECT,\
source_bigquery_dataset=SOURCE_DATASET,\
source_bigquery_table=SOURCE_TABLE,\
destination_bigquery_project=DEST_PROJECT,\
destination_bigquery_dataset=DEST_DATASET,\
destination_bigquery_table=DEST_TABLE"
```

## Deployment

### Prerequisites and Setup

Before deploying the template, ensure you have the following setup:

#### 1. Google Cloud Project Setup

```bash
# Set your project ID
export PROJECT_ID="your-project-id"
gcloud config set project $PROJECT_ID

# Enable required APIs
gcloud services enable \
  dataflow.googleapis.com \
  cloudbuild.googleapis.com \
  storage.googleapis.com \
  bigquery.googleapis.com \
  containerregistry.googleapis.com
```

#### 2. Authentication

```bash
# Authenticate with Google Cloud
gcloud auth login

# Configure Docker to use gcloud as credential helper
gcloud auth configure-docker
```

#### 3. Create Storage Bucket for Templates

```bash
# Create bucket for Dataflow templates (if not exists)
export BUCKET_NAME="${PROJECT_ID}-dataflow-templates"
gsutil mb -p $PROJECT_ID -l us-central1 gs://$BUCKET_NAME

# Verify bucket creation
gsutil ls gs://$BUCKET_NAME
```

### Build and Deploy Template

#### Method 1: Using gcloud flex-template build (Recommended)

```bash
# Set deployment variables
export PROJECT_ID="kk-data-analytics"
export TEMPLATE_NAME="kk-custom-data-sync-template"
export BUCKET_NAME="${PROJECT_ID}-dataflow-templates"
export IMAGE_NAME="gcr.io/${PROJECT_ID}/dataflow/custom-data-sync-template:v1.0.0"

# 1. Build Docker image
docker build -t $IMAGE_NAME .

# 2. Push to registry
docker push $IMAGE_NAME

# 3. Create template using gcloud (with comprehensive metadata.json)
gcloud dataflow flex-template build gs://${BUCKET_NAME}/templates/${TEMPLATE_NAME} \
  --image $IMAGE_NAME \
  --sdk-language PYTHON \
  --metadata-file metadata.json

# 4. Verify template deployment
gsutil ls gs://${BUCKET_NAME}/templates/
```

#### Method 2: Quick Development Deployment

```bash
# For development with latest tag
export IMAGE_NAME="gcr.io/${PROJECT_ID}/dataflow/custom-data-sync-template:latest"

docker build -t $IMAGE_NAME .
docker push $IMAGE_NAME

gcloud dataflow flex-template build gs://${BUCKET_NAME}/templates/${TEMPLATE_NAME} \
  --image $IMAGE_NAME \
  --sdk-language PYTHON \
  --metadata-file metadata.json
```

#### Deployment Verification

```bash
# Test template with a simple job
gcloud dataflow flex-template run test-deployment-$(date +%Y%m%d-%H%M%S) \
  --template-file-gcs-location=gs://${BUCKET_NAME}/templates/${TEMPLATE_NAME} \
  --region=us-central1 \
  --parameters="data_source=bigquery,source_bigquery_project=${PROJECT_ID},source_bigquery_query=SELECT 1 as test_column,destination_bigquery_project=${PROJECT_ID},destination_bigquery_dataset=test_dataset,destination_bigquery_table=test_table" \
  --max-workers=1

# Check job status
gcloud dataflow jobs list --region=us-central1 --limit=5
```

### Deployment Troubleshooting

#### Common Issues and Solutions

| Issue                        | Error Message                           | Solution                                                                                   |
| ---------------------------- | --------------------------------------- | ------------------------------------------------------------------------------------------ |
| **Docker Push Fails**        | `unauthorized: authentication required` | Run `gcloud auth configure-docker`                                                         |
| **API Not Enabled**          | `API [service] not enabled`             | Enable required APIs: `gcloud services enable dataflow.googleapis.com`                     |
| **Insufficient Permissions** | `Permission denied`                     | Ensure service account has Dataflow Admin and Storage Admin roles                          |
| **Template Not Found**       | `Template not found`                    | Verify template path: `gsutil ls gs://your-bucket/templates/`                              |
| **Image Not Found**          | `Image does not exist`                  | Check image exists: `gcloud container images list --repository=gcr.io/PROJECT_ID/dataflow` |

#### Cleanup Commands

```bash
# Remove failed deployments
gcloud dataflow jobs cancel JOB_ID --region=us-central1

# Delete template (for redeployment)
gsutil rm gs://${BUCKET_NAME}/templates/${TEMPLATE_NAME}

# Remove Docker images
docker rmi $IMAGE_NAME
```

## Smart Sync

Smart Sync automatically detects the latest timestamp in your BigQuery destination table and synchronizes only new data since that timestamp.

**Benefits:**

- Eliminates duplicate data processing
- Automatic gap detection and filling
- No manual timestamp management
- Self-healing pipeline behavior

### Timestamp Calculation Logic

Smart Sync dynamically calculates `start_timestamp` and `end_timestamp` for each pipeline run:

#### `end_timestamp` Calculation

```python
# ALWAYS current time when pipeline runs
end_timestamp = datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')
# Example: '2025-07-22 15:30:00'
```

#### `start_timestamp` Calculation

```python
# Step 1: Query BigQuery destination table
query = f"SELECT MAX(updated_at) FROM `{project}.{dataset}.{table}`"

# Step 2: Set start_timestamp based on result
if latest_timestamp_found:
    start_timestamp = latest_timestamp_found  # From BigQuery MAX()
else:
    # Empty table - two options:
    if sync_all_on_empty_table == 'true':  # Default
        start_timestamp = '1900-01-01 00:00:00'  # ALL historical data
    else:
        # Configurable fallback period
        fallback_time = current_time - timedelta(days=fallback_days)
        start_timestamp = fallback_time.strftime('%Y-%m-%d %H:%M:%S')
```

#### Real-World Examples

**Scenario 1: Normal Operation (Every 2 Hours)**

```
🕐 Pipeline Run Time: 2025-07-22 15:00:00

BigQuery Result: '2025-07-22 13:00:00' (last sync)
✅ Calculated Timestamps:
   start_timestamp = '2025-07-22 13:00:00' (from BigQuery)
   end_timestamp   = '2025-07-22 15:00:00' (current time)

📊 Data Window: 2 hours of new data only
```

**Scenario 2: First Run (Empty Table)**

```
🕐 Pipeline Run Time: 2025-07-22 15:00:00

BigQuery Result: NULL (empty table)
✅ Calculated Timestamps:
   start_timestamp = '1900-01-01 00:00:00' (sync ALL historical data)
   end_timestamp   = '2025-07-22 15:00:00' (current time)

📊 Data Window: Complete historical sync
```

**Scenario 3: Large Gap (After Outage)**

```
🕐 Pipeline Run Time: 2025-07-22 15:00:00

BigQuery Result: '2025-07-21 10:30:00' (28+ hours ago)
✅ Calculated Timestamps:
   start_timestamp = '2025-07-21 10:30:00' (from BigQuery)
   end_timestamp   = '2025-07-22 15:00:00' (current time)

📊 Data Window: 28+ hours of missed data (automatic recovery)
```

### Configuration Parameters

| Parameter                     | Default      | Description                                            |
| ----------------------------- | ------------ | ------------------------------------------------------ |
| `enable_smart_sync`           | `false`      | Enable smart sync functionality                        |
| `smart_sync_timestamp_column` | `updated_at` | Column used for timestamp detection                    |
| `sync_all_on_empty_table`     | `true`       | Sync all historical data when table is empty           |
| `fallback_days`               | `1`          | Days to look back when `sync_all_on_empty_table=false` |

**Query Template Example:**

```sql
SELECT * FROM your_table
WHERE updated_at > '{start_timestamp}'
AND updated_at <= '{end_timestamp}'
ORDER BY updated_at ASC
```

## Auto-Schema

Auto-Schema automatically detects source table schemas and creates optimized BigQuery tables with proper data types, partitioning, and clustering.

**Features:**

- Automatic PostgreSQL → BigQuery type mapping
- Default partitioning on timestamp fields
- Configurable clustering for query optimization
- Source-only schema (no metadata pollution)

## Network Configuration

For cross-project or private network access:

```bash
--network=your-network
--subnetwork=regions/us-central1/subnetworks/your-subnet
--disable-public-ips
--service-account-email=your-service-account@project.iam.gserviceaccount.com
```

## Monitoring

Monitor jobs using Google Cloud Console:

```bash
# Check job status
gcloud dataflow jobs describe JOB_ID --region=us-central1

# View logs
gcloud logging read "resource.type=dataflow_job AND resource.labels.job_id=JOB_ID" --limit=50
```

## 🕐 Automated Scheduling with Cloud Scheduler

Set up automated pipeline execution using Google Cloud Scheduler with Smart Sync for efficient data synchronization.

### 📋 Prerequisites for Scheduler Creation

Before creating schedulers, ensure you have:

```bash
# 1. Enable required APIs
gcloud services enable cloudscheduler.googleapis.com
gcloud services enable dataflow.googleapis.com

# 2. Set up proper IAM permissions for your service account
gcloud projects add-iam-policy-binding kk-data-analytics \
  --member="serviceAccount:cloud-scheduler@kk-data-analytics.iam.gserviceaccount.com" \
  --role="roles/dataflow.developer"

gcloud projects add-iam-policy-binding kk-data-analytics \
  --member="serviceAccount:cloud-scheduler@kk-data-analytics.iam.gserviceaccount.com" \
  --role="roles/bigquery.dataEditor"

# 3. Verify your template exists
gsutil ls gs://kk-data-analytics-dataflow-templates/templates/kk-custom-data-sync-template
```

### 🚀 Step-by-Step Scheduler Creation

#### **Method 1: Using gcloud CLI (Recommended)**

**Step 1: Create the base scheduler command**

```bash
gcloud scheduler jobs create http SCHEDULER_JOB_NAME \
  --location=us-central1 \
  --schedule="CRON_EXPRESSION" \
  --time-zone="UTC" \
  --uri="https://dataflow.googleapis.com/v1b3/projects/kk-data-analytics/locations/us-central1/flexTemplates:launch" \
  --http-method=POST \
  --headers="Content-Type=application/json" \
  --message-body='DATAFLOW_CONFIG_JSON'
```

**Step 2: Complete example for Every 2 Hours**

```bash
gcloud scheduler jobs create http organizations-sync-every-2hours \
  --location=us-central1 \
  --schedule="0 */2 * * *" \
  --time-zone="UTC" \
  --description="Automated PostgreSQL to BigQuery sync every 2 hours with Smart Sync" \
  --uri="https://dataflow.googleapis.com/v1b3/projects/kk-data-analytics/locations/us-central1/flexTemplates:launch" \
  --http-method=POST \
  --headers="Content-Type=application/json" \
  --message-body='{
  "launch_parameter": {
    "jobName": "data-sync-scheduled-'$(date +%Y%m%d-%H%M%S)'",
    "containerSpecGcsPath": "gs://kk-data-analytics-dataflow-templates/templates/kk-custom-data-sync-template",
    "environment": {
      "network": "your-network",
      "subnetwork": "regions/us-central1/subnetworks/your-subnet",
      "disablePublicIps": true,
      "serviceAccountEmail": "your-service-account@project.iam.gserviceaccount.com",
      "maxWorkers": 5,
      "workerMachineType": "n1-standard-1",
      "zone": "us-central1-a"
    },
    "parameters": {
      "data_source": "postgresql",
      "postgresql_host": "YOUR_HOST",
      "postgresql_database": "YOUR_DATABASE",
      "postgresql_username": "YOUR_USER",
      "postgresql_password": "YOUR_PASSWORD",
      "enable_smart_sync": "true",
      "smart_sync_timestamp_column": "updated_at",
      "postgresql_base_query": "SELECT * FROM your_table WHERE updated_at > '\'''{start_timestamp}'\'' AND updated_at <= '\'''{end_timestamp}'\'' ORDER BY updated_at ASC",
      "enable_auto_schema": "true",
      "source_table_for_schema": "schema.table",
      "partition_field": "updated_at",
      "clustering_fields": "id,status",
      "destination_bigquery_project": "kk-data-analytics",
      "destination_bigquery_dataset": "YOUR_DATASET",
      "destination_bigquery_table": "YOUR_TABLE",
      "write_disposition": "WRITE_APPEND"
    }
  }
}'
```

#### **Method 2: Using Configuration File**

**Step 1: Create scheduler configuration file**

```bash
cat > scheduler-config.json << 'EOF'
{
  "name": "organizations-sync-every-2hours",
  "description": "Automated PostgreSQL to BigQuery sync every 2 hours",
  "schedule": "0 */2 * * *",
  "timeZone": "UTC",
  "httpTarget": {
    "uri": "https://dataflow.googleapis.com/v1b3/projects/kk-data-analytics/locations/us-central1/flexTemplates:launch",
    "httpMethod": "POST",
    "headers": {
      "Content-Type": "application/json"
    },
    "body": "ewogICJsYXVuY2hfcGFyYW1ldGVyIjoge30KfQ=="
  }
}
EOF
```

**Step 2: Deploy scheduler**

```bash
gcloud scheduler jobs create http organizations-sync-every-2hours \
  --location=us-central1 \
  --config-file=scheduler-config.json
```

### 📅 Common Scheduling Patterns

| **Use Case**       | **Cron Expression** | **Description**             | **Runtime Examples**                                                               | **Best For**                            |
| ------------------ | ------------------- | --------------------------- | ---------------------------------------------------------------------------------- | --------------------------------------- |
| **High Frequency** | `"0 */2 * * *"`     | **Every 2 hours**           | 00:00, 02:00, 04:00, 06:00, 08:00, 10:00, 12:00, 14:00, 16:00, 18:00, 20:00, 22:00 | Real-time analytics, transactional data |
| **Standard**       | `"0 */1 * * *"`     | Every hour                  | 00:00, 01:00, 02:00, 03:00...                                                      | User activity tracking, logs            |
| **Moderate**       | `"0 */4 * * *"`     | Every 4 hours               | 00:00, 04:00, 08:00, 12:00, 16:00, 20:00                                           | Product updates, moderate volume data   |
| **Low Frequency**  | `"0 */6 * * *"`     | Every 6 hours               | 00:00, 06:00, 12:00, 18:00                                                         | Reference data, configuration changes   |
| **Daily**          | `"0 0 * * *"`       | Daily at midnight           | 00:00 every day                                                                    | Daily reports, batch processing         |
| **Business Hours** | `"0 9-17 * * 1-5"`  | Every hour 9AM-5PM, Mon-Fri | 09:00, 10:00... (weekdays only)                                                    | Business data during work hours         |
| **Off-Peak**       | `"0 2 * * *"`       | Daily at 2 AM               | 02:00 every day                                                                    | Heavy processing, maintenance           |
| **Weekly**         | `"0 0 * * 0"`       | Weekly on Sunday            | 00:00 every Sunday                                                                 | Weekly reports, cleanup                 |
| **Monthly**        | `"0 0 1 * *"`       | Monthly on 1st              | 00:00 on the 1st of each month                                                     | Monthly aggregations                    |

### 🛠️ Ready-to-Use Scheduler Examples

#### **1. High-Frequency Data Sync (Every 2 Hours)**

```bash
# For real-time operational data
gcloud scheduler jobs create http users-sync-every-2hours \
  --location=us-central1 \
  --schedule="0 */2 * * *" \
  --time-zone="UTC" \
  --description="User data sync every 2 hours for real-time analytics" \
  --uri="https://dataflow.googleapis.com/v1b3/projects/kk-data-analytics/locations/us-central1/flexTemplates:launch" \
  --http-method=POST \
  --headers="Content-Type=application/json" \
  --message-body='{
    "launch_parameter": {
      "jobName": "users-sync-'$(date +%Y%m%d-%H%M%S)'",
      "containerSpecGcsPath": "gs://kk-data-analytics-dataflow-templates/templates/kk-custom-data-sync-template",
      "environment": {
        "network": "your-network",
        "subnetwork": "regions/us-central1/subnetworks/your-subnet",
        "disablePublicIps": true,
        "serviceAccountEmail": "your-service-account@project.iam.gserviceaccount.com",
        "maxWorkers": 3,
        "workerMachineType": "n1-standard-1"
      },
      "parameters": {
        "data_source": "postgresql",
        "postgresql_host": "YOUR_HOST",
        "postgresql_database": "YOUR_DB",
        "postgresql_username": "YOUR_USER",
        "postgresql_password": "YOUR_PASSWORD",
        "enable_smart_sync": "true",
        "postgresql_base_query": "SELECT * FROM users WHERE updated_at > '\'''{start_timestamp}'\'' AND updated_at <= '\'''{end_timestamp}'\'' ORDER BY updated_at ASC",
        "enable_auto_schema": "true",
        "source_table_for_schema": "public.users",
        "destination_bigquery_project": "kk-data-analytics",
        "destination_bigquery_dataset": "analytics",
        "destination_bigquery_table": "users"
      }
    }
  }'
```

#### **2. Daily Business Reports (Every Day at 2 AM)**

```bash
# For daily batch processing during off-peak hours
gcloud scheduler jobs create http daily-reports-sync \
  --location=us-central1 \
  --schedule="0 2 * * *" \
  --time-zone="UTC" \
  --description="Daily reports sync at 2 AM during off-peak hours" \
  --uri="https://dataflow.googleapis.com/v1b3/projects/kk-data-analytics/locations/us-central1/flexTemplates:launch" \
  --http-method=POST \
  --headers="Content-Type=application/json" \
  --message-body='{
    "launch_parameter": {
      "jobName": "daily-reports-'$(date +%Y%m%d)'",
      "containerSpecGcsPath": "gs://kk-data-analytics-dataflow-templates/templates/kk-custom-data-sync-template",
      "environment": {
        "maxWorkers": 10,
        "workerMachineType": "n1-standard-2"
      },
      "parameters": {
        "data_source": "bigquery",
        "source_bigquery_project": "SOURCE_PROJECT",
        "source_bigquery_query": "SELECT * FROM reports WHERE DATE(created_at) = CURRENT_DATE() - 1",
        "destination_bigquery_project": "kk-data-analytics",
        "destination_bigquery_dataset": "reports",
        "destination_bigquery_table": "daily_summary"
      }
    }
  }'
```

#### **3. Business Hours Only (9 AM - 5 PM, Weekdays)**

```bash
# For business-critical data during work hours only
gcloud scheduler jobs create http business-hours-sync \
  --location=us-central1 \
  --schedule="0 9-17 * * 1-5" \
  --time-zone="America/New_York" \
  --description="Business hours data sync (9AM-5PM, Mon-Fri EST)" \
  --uri="https://dataflow.googleapis.com/v1b3/projects/kk-data-analytics/locations/us-central1/flexTemplates:launch" \
  --http-method=POST \
  --headers="Content-Type=application/json" \
  --message-body='{
    "launch_parameter": {
      "jobName": "business-sync-'$(date +%Y%m%d-%H%M%S)'",
      "containerSpecGcsPath": "gs://kk-data-analytics-dataflow-templates/templates/kk-custom-data-sync-template",
      "parameters": {
        "data_source": "mongodb",
        "mongodb_host": "YOUR_MONGO_HOST",
        "mongodb_database": "business_data",
        "mongodb_collection": "transactions",
        "mongodb_query": "{\"status\": \"active\", \"business_hours\": true}",
        "destination_bigquery_project": "kk-data-analytics",
        "destination_bigquery_dataset": "business",
        "destination_bigquery_table": "transactions"
      }
    }
  }'
```

#### **4. Weekly Summary Reports (Every Sunday)**

```bash
# For weekly aggregation and summary reports
gcloud scheduler jobs create http weekly-summary-sync \
  --location=us-central1 \
  --schedule="0 0 * * 0" \
  --time-zone="UTC" \
  --description="Weekly summary reports every Sunday at midnight" \
  --uri="https://dataflow.googleapis.com/v1b3/projects/kk-data-analytics/locations/us-central1/flexTemplates:launch" \
  --http-method=POST \
  --headers="Content-Type=application/json" \
  --message-body='{
    "launch_parameter": {
      "jobName": "weekly-summary-'$(date +%Y%m%d)'",
      "containerSpecGcsPath": "gs://kk-data-analytics-dataflow-templates/templates/kk-custom-data-sync-template",
      "environment": {
        "maxWorkers": 15,
        "workerMachineType": "n1-standard-4"
      },
      "parameters": {
        "data_source": "postgresql",
        "postgresql_host": "YOUR_HOST",
        "postgresql_database": "analytics",
        "postgresql_query": "SELECT * FROM weekly_aggregations WHERE week_ending = DATE_TRUNC('\''week'\'', CURRENT_DATE)",
        "destination_bigquery_project": "kk-data-analytics",
        "destination_bigquery_dataset": "reports",
        "destination_bigquery_table": "weekly_summary"
      }
    }
  }'
```

### 🔧 Scheduler Management Commands

#### **List and Monitor Schedulers**

```bash
# List all scheduler jobs in your project
gcloud scheduler jobs list --location=us-central1

# Get detailed information about a specific job
gcloud scheduler jobs describe SCHEDULER_JOB_NAME --location=us-central1

# View scheduler job configuration
gcloud scheduler jobs describe organizations-sync-every-2hours \
  --location=us-central1 \
  --format="yaml"
```

#### **Modify Existing Schedulers**

```bash
# Update schedule frequency (e.g., change from every 2 hours to every 4 hours)
gcloud scheduler jobs update http organizations-sync-every-2hours \
  --location=us-central1 \
  --schedule="0 */4 * * *" \
  --description="Updated to run every 4 hours for cost optimization"

# Update time zone
gcloud scheduler jobs update http organizations-sync-every-2hours \
  --location=us-central1 \
  --time-zone="America/New_York"

# Pause a scheduler temporarily
gcloud scheduler jobs pause organizations-sync-every-2hours --location=us-central1

# Resume a paused scheduler
gcloud scheduler jobs resume organizations-sync-every-2hours --location=us-central1
```

#### **Test and Debug Schedulers**

```bash
# Test run immediately (without waiting for schedule)
gcloud scheduler jobs run organizations-sync-every-2hours --location=us-central1

# Delete a scheduler job
gcloud scheduler jobs delete organizations-sync-every-2hours \
  --location=us-central1 \
  --quiet
```

### 📊 Comprehensive Monitoring and Troubleshooting

#### **Monitor Scheduler Health**

```bash
# Check scheduler job status and last execution
gcloud scheduler jobs describe organizations-sync-every-2hours \
  --location=us-central1 \
  --format="table(name,schedule,state,lastAttemptTime,status.code,status.message)"

# View recent scheduler execution logs
gcloud logging read "resource.type=cloud_scheduler_job AND resource.labels.job_id=organizations-sync-every-2hours" \
  --location=us-central1 \
  --limit=20 \
  --format="table(timestamp,severity,textPayload)"

# Monitor scheduler success/failure rates
gcloud logging read "resource.type=cloud_scheduler_job AND (severity=ERROR OR severity=WARNING)" \
  --location=us-central1 \
  --limit=50
```

#### **Monitor Triggered Dataflow Jobs**

```bash
# List all Dataflow jobs triggered by schedulers
gcloud dataflow jobs list \
  --region=us-central1 \
  --filter="name~organizations-sync" \
  --format="table(id,name,currentState,createTime)"

# Get detailed status of latest triggered job
LATEST_JOB=$(gcloud dataflow jobs list --region=us-central1 --filter="name~organizations-sync" --format="value(id)" --limit=1)
gcloud dataflow jobs describe $LATEST_JOB --region=us-central1

# Monitor job metrics and resource usage
gcloud dataflow jobs describe $LATEST_JOB \
  --region=us-central1 \
  --format="table(currentState,jobMetadata.workerPools[0].numWorkers,environment.dataset,jobMetadata.stageStates[0].executionStageName)"
```

#### **Set Up Monitoring Alerts**

```bash
# Create alerting policy for scheduler failures
gcloud alpha monitoring policies create \
  --policy-from-file=scheduler-alert-policy.yaml

# Example alert policy content (scheduler-alert-policy.yaml):
cat > scheduler-alert-policy.yaml << 'EOF'
displayName: "Scheduler Job Failures"
conditions:
  - displayName: "Cloud Scheduler Job Failed"
    conditionThreshold:
      filter: 'resource.type="cloud_scheduler_job" AND severity="ERROR"'
      comparison: COMPARISON_GT
      thresholdValue: 0
      duration: 60s
notificationChannels:
  - "projects/kk-data-analytics/notificationChannels/YOUR_NOTIFICATION_CHANNEL"
alertStrategy:
  autoClose: 86400s
EOF
```

### 🕐 Time Zone Considerations

| **Region/Use Case** | **Recommended Time Zone** | **Example Schedule** | **Considerations**                  |
| ------------------- | ------------------------- | -------------------- | ----------------------------------- |
| **Global/UTC**      | `UTC`                     | `"0 */2 * * *"`      | Consistent worldwide, no DST issues |
| **US East Coast**   | `America/New_York`        | `"0 9-17 * * 1-5"`   | Handles EST/EDT automatically       |
| **US West Coast**   | `America/Los_Angeles`     | `"0 6-14 * * 1-5"`   | Handles PST/PDT automatically       |
| **Europe**          | `Europe/London`           | `"0 8-18 * * 1-5"`   | Handles GMT/BST automatically       |
| **Asia Pacific**    | `Asia/Singapore`          | `"0 9-17 * * 1-5"`   | Regional business hours             |

**Important Time Zone Notes:**

- Always specify `--time-zone` explicitly in scheduler creation
- Use IANA time zone names (e.g., `America/New_York`, not `EST`)
- UTC is recommended for global applications to avoid DST complications
- Test scheduler behavior during DST transitions

### 🚨 Error Handling and Retry Configuration

#### **Configure Automatic Retries**

```bash
# Create scheduler with custom retry configuration
gcloud scheduler jobs create http robust-organizations-sync \
  --location=us-central1 \
  --schedule="0 */2 * * *" \
  --time-zone="UTC" \
  --max-retry-attempts=3 \
  --max-retry-duration=600s \
  --min-backoff-duration=30s \
  --max-backoff-duration=300s \
  --max-doublings=3 \
  --uri="https://dataflow.googleapis.com/v1b3/projects/kk-data-analytics/locations/us-central1/flexTemplates:launch" \
  --http-method=POST \
  --headers="Content-Type=application/json" \
  --message-body='YOUR_DATAFLOW_CONFIG'
```

#### **Common Error Scenarios and Solutions**

| **Error Type**          | **Symptoms**                      | **Solution**                                                    |
| ----------------------- | --------------------------------- | --------------------------------------------------------------- |
| **Permission Denied**   | `403 Forbidden` in scheduler logs | Grant `roles/dataflow.developer` to service account             |
| **Template Not Found**  | `404 Not Found` for template path | Verify template exists: `gsutil ls gs://your-bucket/templates/` |
| **Network Issues**      | `Connection timeout` errors       | Check VPC network configuration, firewall rules                 |
| **Resource Limits**     | `RESOURCE_EXHAUSTED` errors       | Increase Dataflow quotas or reduce `maxWorkers`                 |
| **Invalid Parameters**  | `INVALID_ARGUMENT` errors         | Validate all template parameters, check data types              |
| **Database Connection** | `Connection refused` errors       | Verify database credentials, network connectivity               |

#### **Troubleshooting Commands**

```bash
# Check scheduler job execution history
gcloud logging read "resource.type=cloud_scheduler_job AND resource.labels.job_id=YOUR_JOB_NAME" \
  --limit=50 \
  --format="table(timestamp,severity,httpRequest.status,textPayload)"

# View detailed Dataflow job errors
gcloud dataflow jobs list --region=us-central1 --filter="currentState=JOB_STATE_FAILED" \
  --format="table(id,name,currentStateTime)" --limit=10

# Get error details for failed job
FAILED_JOB_ID="YOUR_FAILED_JOB_ID"
gcloud logging read "resource.type=dataflow_job AND resource.labels.job_id=$FAILED_JOB_ID AND severity>=ERROR" \
  --limit=20
```

### 🏆 Production Best Practices

#### **1. Scheduler Naming Convention**

```bash
# Use descriptive, hierarchical names
{data_source}-{table/collection}-{frequency}-{environment}

# Examples:
postgresql-users-every-2hours-prod
mongodb-transactions-daily-staging
bigquery-reports-weekly-prod
```

#### **2. Resource Optimization by Schedule**

```bash
# High-frequency (every 1-2 hours): Smaller workers
"environment": {
  "maxWorkers": 3,
  "workerMachineType": "n1-standard-1"
}

# Daily/weekly batches: Larger workers
"environment": {
  "maxWorkers": 15,
  "workerMachineType": "n1-standard-4"
}

# Critical real-time: Auto-scaling
"environment": {
  "maxWorkers": 20,
  "workerMachineType": "n1-standard-2",
  "enableStreamingEngine": true
}
```

#### **3. Smart Sync Configuration by Frequency**

```bash
# High-frequency: Enable Smart Sync (essential)
"enable_smart_sync": "true"
"sync_all_on_empty_table": "false"  # Avoid large initial sync
"fallback_days": "1"

# Daily batches: Smart Sync recommended
"enable_smart_sync": "true"
"sync_all_on_empty_table": "true"   # OK for daily processing
"fallback_days": "7"

# Weekly/Monthly: Consider full sync
"enable_smart_sync": "false"  # May want complete data refresh
```

#### **4. Monitoring and Alerting Setup**

```bash
# Set up monitoring for:
# - Scheduler execution failures
# - Dataflow job failures
# - Data freshness (max lag time)
# - Resource usage anomalies
# - Cost optimization opportunities

# Example monitoring query
gcloud logging read "resource.type=cloud_scheduler_job AND severity=ERROR" \
  --format="table(timestamp,resource.labels.job_id,textPayload)" \
  --limit=20
```

### Benefits of Smart Sync with Scheduling

- **Efficient Data Transfer**: Only processes new data since last run
- **Automatic Recovery**: Handles gaps and outages automatically
- **Cost Optimization**: Reduces processing time and compute costs
- **Zero Overlap**: Eliminates duplicate data processing
- **Self-Healing**: Automatically adjusts to any schedule changes
- **Scalable Architecture**: Handles varying data volumes automatically
- **Production Ready**: Comprehensive error handling and monitoring

## Error Handling

The template includes comprehensive error handling:

- Automatic retry mechanisms
- Graceful fallback for failed smart sync
- Data validation and sanitization
- Detailed logging for troubleshooting

## Requirements

- Apache Beam 2.66.0+
- Python 3.9+
- Google Cloud SDK
- Required Python packages (see `requirements.txt`)

## Support

For issues and questions:

- Check job logs in Google Cloud Console
- Verify network connectivity and credentials
- Ensure all required parameters are provided
- Review BigQuery destination table permissions

## License

Apache 2.0

---

**Version**: 1.0.0  
**Author**: Data Engineering Team  
**Last Updated**: January 2025
