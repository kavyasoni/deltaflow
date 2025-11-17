# DeltaFlow Configuration Reference

Complete parameter reference for configuring DeltaFlow pipelines. This guide covers all 40+ configuration parameters with examples and best practices.

## Table of Contents

1. [Core Parameters](#core-parameters)
2. [PostgreSQL Configuration](#postgresql-configuration)
3. [MongoDB Configuration](#mongodb-configuration)
4. [BigQuery Source Configuration](#bigquery-source-configuration)
5. [Destination Configuration](#destination-configuration)
6. [Smart Sync Parameters](#smart-sync-parameters)
7. [Auto-Schema Parameters](#auto-schema-parameters)
8. [Transformation Configuration](#transformation-configuration)
9. [Performance Tuning](#performance-tuning)
10. [Common Configuration Scenarios](#common-configuration-scenarios)

## Core Parameters

### data_source

**Type**: String (comma-separated for multiple sources)
**Required**: No
**Default**: `postgresql`
**Valid Values**: `postgresql`, `mongodb`, `bigquery`, or combinations like `postgresql,mongodb`

Specifies which data source(s) to read from.

**Examples**:

```bash
# Single source
data_source=postgresql

# Multiple sources (parallel processing)
data_source=postgresql,mongodb

# BigQuery-to-BigQuery replication
data_source=bigquery
```

**Notes**:
- Multiple sources are processed in parallel
- Results are combined before writing to BigQuery
- Each source must have its corresponding configuration parameters

### destination_bigquery_project

**Type**: String
**Required**: Yes
**Format**: GCP project ID (lowercase, hyphens allowed)
**Example**: `analytics-prod-12345`

The GCP project containing the destination BigQuery dataset.

**Best Practices**:
- Use separate projects for dev/staging/prod
- Ensure project has BigQuery API enabled
- Verify service account has access

### destination_bigquery_dataset

**Type**: String
**Required**: Yes
**Format**: Alphanumeric with underscores
**Example**: `production_data`, `raw_events`, `user_analytics`

The BigQuery dataset where data will be written.

**Best Practices**:
- Use descriptive dataset names
- Organize by data domain (users, orders, events)
- Set appropriate dataset expiration if needed

### destination_bigquery_table

**Type**: String
**Required**: Yes
**Format**: Alphanumeric with underscores
**Example**: `orders`, `user_profiles`, `event_logs`

The BigQuery table where data will be written.

**Best Practices**:
- Use singular nouns (e.g., `order` not `orders`)
- Match source table name when possible
- Consider partitioning strategy in name (e.g., `events_partitioned`)

## PostgreSQL Configuration

### postgresql_host

**Type**: String
**Required**: Yes (when data_source includes postgresql)
**Format**: IP address or hostname
**Examples**: `10.0.0.5`, `db.example.com`, `postgres-instance.c.project.internal`

PostgreSQL server hostname or IP address.

**Best Practices**:
- Use private IP addresses within VPC
- Use Cloud SQL proxy for Cloud SQL instances
- Test connectivity before running jobs

### postgresql_port

**Type**: String
**Required**: No
**Default**: `5432`
**Example**: `5432`, `5433`

PostgreSQL server port number.

**Notes**:
- Default PostgreSQL port is 5432
- Only change if using non-standard port
- Must be specified as string, not integer

### postgresql_database

**Type**: String
**Required**: Yes (when data_source includes postgresql)
**Example**: `production_db`, `user_management`, `analytics`

PostgreSQL database name to connect to.

**Best Practices**:
- Use read-only user for production databases
- Verify database exists before running job
- Consider using separate database for analytics

### postgresql_username

**Type**: String
**Required**: Yes (when data_source includes postgresql)
**Example**: `dataflow_user`, `readonly_user`

PostgreSQL username for authentication.

**Best Practices**:
- Create dedicated user for Dataflow jobs
- Grant only SELECT permissions for read-only access
- Use descriptive username (e.g., `dataflow_reader`)

**Example user creation**:

```sql
CREATE USER dataflow_user WITH PASSWORD 'secure_password';
GRANT SELECT ON ALL TABLES IN SCHEMA public TO dataflow_user;
GRANT USAGE ON SCHEMA public TO dataflow_user;
```

### postgresql_password

**Type**: String
**Required**: Yes (when data_source includes postgresql)
**Example**: Use strong passwords or Secret Manager

PostgreSQL password for authentication.

**Security Best Practices**:

```bash
# Use Secret Manager (recommended)
gcloud secrets create pg-password --data-file=password.txt

# Reference in job
--parameters="postgresql_password=$(gcloud secrets versions access latest --secret=pg-password)"

# Or use environment variable
export PG_PASSWORD="secure_password"
--parameters="postgresql_password=${PG_PASSWORD}"
```

**Never**:
- ❌ Hardcode passwords in scripts
- ❌ Commit passwords to version control
- ❌ Share passwords in plain text

### postgresql_query

**Type**: String
**Required**: Conditional (see notes)
**Example**: `SELECT * FROM orders WHERE status = 'completed'`

SQL query to execute on PostgreSQL database.

**When Required**:
- When `enable_smart_sync=false` (static query)
- When `enable_smart_sync=true` AND no `postgresql_base_query` provided

**When Not Required**:
- When using `postgresql_base_query` for Smart Sync

**Examples**:

```sql
-- Simple query
SELECT * FROM users

-- With WHERE clause
SELECT * FROM orders WHERE created_at >= '2025-01-01'

-- With JOIN
SELECT o.*, u.email
FROM orders o
JOIN users u ON o.user_id = u.id
WHERE o.status = 'active'

-- With aggregation (for summary tables)
SELECT
  DATE(created_at) as order_date,
  COUNT(*) as order_count,
  SUM(total_amount) as total_revenue
FROM orders
GROUP BY DATE(created_at)
```

**Best Practices**:
- Use explicit column selection instead of `SELECT *`
- Add indexes on WHERE clause columns
- Test query performance on source database
- Use `LIMIT` for testing

### postgresql_base_query

**Type**: String
**Required**: When `enable_smart_sync=true`
**Format**: SQL with `{start_timestamp}` and `{end_timestamp}` placeholders

Base query template for Smart Sync with timestamp placeholders.

**Template Format**:

```sql
SELECT * FROM table_name
WHERE updated_at > '{start_timestamp}'
  AND updated_at <= '{end_timestamp}'
ORDER BY updated_at ASC
```

**Examples**:

```sql
-- Basic incremental sync
SELECT * FROM orders
WHERE updated_at > '{start_timestamp}'
  AND updated_at <= '{end_timestamp}'
ORDER BY updated_at ASC

-- With additional filters
SELECT * FROM events
WHERE event_timestamp > '{start_timestamp}'
  AND event_timestamp <= '{end_timestamp}'
  AND event_type IN ('purchase', 'signup')
ORDER BY event_timestamp ASC

-- With JOINs (ensure timestamp column is indexed)
SELECT e.*, u.email
FROM events e
JOIN users u ON e.user_id = u.id
WHERE e.created_at > '{start_timestamp}'
  AND e.created_at <= '{end_timestamp}'
ORDER BY e.created_at ASC
```

**Important Notes**:
- `{start_timestamp}` and `{end_timestamp}` are replaced at runtime
- Timestamp format: `YYYY-MM-DD HH:MM:SS`
- Always use `ORDER BY` on timestamp column
- Use `>` for start and `<=` for end to avoid duplicates

### postgresql_table

**Type**: String
**Required**: No
**Example**: `orders`, `users`, `events`

PostgreSQL table name (optional, can be auto-detected from query or schema).

## MongoDB Configuration

### mongodb_host

**Type**: String
**Required**: Yes (when data_source includes mongodb)
**Example**: `mongodb.example.com`, `10.0.0.10`

MongoDB server hostname or IP address.

### mongodb_port

**Type**: String
**Required**: No
**Default**: `27017`
**Example**: `27017`, `27018`

MongoDB server port number.

### mongodb_database

**Type**: String
**Required**: Yes (when data_source includes mongodb)
**Example**: `user_data`, `analytics`, `production`

MongoDB database name.

### mongodb_collection

**Type**: String
**Required**: Yes (when data_source includes mongodb)
**Example**: `users`, `events`, `transactions`

MongoDB collection name.

### mongodb_username

**Type**: String
**Required**: No
**Example**: `dataflow_user`

MongoDB username for authentication (if authentication is enabled).

### mongodb_password

**Type**: String
**Required**: No
**Example**: Use Secret Manager

MongoDB password for authentication.

### mongodb_connection_string

**Type**: String
**Required**: No
**Example**: `mongodb://user:pass@host:27017/db?authSource=admin`

Complete MongoDB connection string (overrides individual connection parameters).

**Examples**:

```bash
# Basic connection
mongodb://localhost:27017/mydb

# With authentication
mongodb://user:password@mongodb.example.com:27017/mydb

# With replica set
mongodb://user:password@host1:27017,host2:27017,host3:27017/mydb?replicaSet=rs0

# With SSL and auth source
mongodb://user:password@mongodb.example.com:27017/mydb?authSource=admin&ssl=true
```

### mongodb_query

**Type**: String (JSON)
**Required**: No
**Example**: `{"status": "active", "verified": true}`

MongoDB query as JSON string to filter documents.

**Examples**:

```bash
# Simple equality
mongodb_query='{"status": "active"}'

# Multiple conditions
mongodb_query='{"status": "active", "verified": true}'

# Range query
mongodb_query='{"age": {"$gte": 18, "$lte": 65}}'

# Array contains
mongodb_query='{"tags": {"$in": ["premium", "vip"]}}'

# Regex pattern
mongodb_query='{"email": {"$regex": "@example\\.com$"}}'

# Date range
mongodb_query='{"created_at": {"$gte": {"$date": "2025-01-01T00:00:00Z"}}}'

# Complex query
mongodb_query='{
  "$and": [
    {"status": "active"},
    {"$or": [
      {"plan": "premium"},
      {"credits": {"$gte": 100}}
    ]}
  ]
}'
```

### mongodb_projection

**Type**: String (JSON)
**Required**: No
**Example**: `{"name": 1, "email": 1, "created_at": 1}`

MongoDB projection to limit returned fields.

**Examples**:

```bash
# Include specific fields (and _id by default)
mongodb_projection='{"name": 1, "email": 1, "status": 1}'

# Exclude _id
mongodb_projection='{"_id": 0, "name": 1, "email": 1}'

# Exclude specific fields
mongodb_projection='{"password": 0, "internal_notes": 0}'

# Nested field projection
mongodb_projection='{"profile.name": 1, "profile.age": 1, "email": 1}'
```

**Best Practices**:
- Only project fields you need to reduce data transfer
- Exclude sensitive fields (passwords, tokens)
- Test projection with sample query first

## BigQuery Source Configuration

### source_bigquery_project

**Type**: String
**Required**: Yes (when data_source includes bigquery)
**Format**: GCP project ID
**Example**: `source-project-12345`

Source BigQuery project ID (can be different from pipeline project).

### source_bigquery_dataset

**Type**: String
**Required**: Yes (when data_source includes bigquery)
**Example**: `raw_data`, `source_analytics`

Source BigQuery dataset name.

### source_bigquery_table

**Type**: String
**Required**: Conditional (if no query provided)
**Example**: `events`, `user_activity`

Source BigQuery table name.

### source_bigquery_query

**Type**: String
**Required**: No (alternative to table)
**Example**: `SELECT * FROM \`project.dataset.table\` WHERE DATE(timestamp) = CURRENT_DATE()`

BigQuery SQL query for reading data (alternative to specifying table).

**Examples**:

```sql
-- Simple query
SELECT * FROM `project.dataset.table`

-- With date filter
SELECT *
FROM `project.dataset.events`
WHERE DATE(timestamp) >= DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY)

-- With aggregation
SELECT
  user_id,
  COUNT(*) as event_count,
  MAX(timestamp) as last_event
FROM `project.dataset.events`
GROUP BY user_id

-- Cross-project query
SELECT e.*, u.email
FROM `source-project.raw.events` e
JOIN `user-project.data.users` u
  ON e.user_id = u.id
WHERE DATE(e.timestamp) = CURRENT_DATE()

-- With wildcard table
SELECT *
FROM `project.dataset.events_*`
WHERE _TABLE_SUFFIX = FORMAT_DATE('%Y%m%d', CURRENT_DATE())
```

**Best Practices**:
- Use partitioned table filters to reduce cost
- Avoid `SELECT *` for large tables
- Test query cost before running job

## Destination Configuration

### write_disposition

**Type**: String
**Required**: No
**Default**: `WRITE_APPEND`
**Valid Values**: `WRITE_APPEND`, `WRITE_TRUNCATE`, `WRITE_EMPTY`

Controls how data is written to destination table.

**Options**:

| Value | Behavior | Use Case |
|-------|----------|----------|
| `WRITE_APPEND` | Append new rows to existing table | Incremental loads, event logs |
| `WRITE_TRUNCATE` | Delete all existing data first | Full refresh, snapshots |
| `WRITE_EMPTY` | Only write if table is empty | Initial load only |

**Examples**:

```bash
# Incremental sync (default)
write_disposition=WRITE_APPEND

# Daily full refresh
write_disposition=WRITE_TRUNCATE

# One-time initial load
write_disposition=WRITE_EMPTY
```

### create_disposition

**Type**: String
**Required**: No
**Default**: `CREATE_IF_NEEDED`
**Valid Values**: `CREATE_IF_NEEDED`, `CREATE_NEVER`

Controls table creation behavior.

**Options**:

| Value | Behavior | Use Case |
|-------|----------|----------|
| `CREATE_IF_NEEDED` | Create table if doesn't exist | Auto-provisioning |
| `CREATE_NEVER` | Fail if table doesn't exist | Enforce pre-created tables |

## Smart Sync Parameters

### enable_smart_sync

**Type**: String (boolean)
**Required**: No
**Default**: `false`
**Valid Values**: `true`, `false`

Enable intelligent incremental synchronization.

**How it works**:
1. Queries destination BigQuery table for `MAX(timestamp_column)`
2. Uses result as `start_timestamp` for source query
3. Automatically syncs only new data since last run

**Example**:

```bash
enable_smart_sync=true
postgresql_base_query="SELECT * FROM orders WHERE updated_at > '{start_timestamp}' AND updated_at <= '{end_timestamp}' ORDER BY updated_at ASC"
smart_sync_timestamp_column=updated_at
```

### smart_sync_timestamp_column

**Type**: String
**Required**: No
**Default**: `updated_at`
**Example**: `updated_at`, `last_modified`, `created_at`, `event_timestamp`

Column name used for timestamp detection in Smart Sync.

**Requirements**:
- Must exist in both source and destination tables
- Should be indexed in source database for performance
- Must be a timestamp/datetime type

**Best Practices**:
- Use `updated_at` for data that can be modified
- Use `created_at` for append-only data
- Ensure column is always populated (not NULL)

### postgresql_base_query

See [PostgreSQL Configuration](#postgresql_base_query) above.

### sync_all_on_empty_table

**Type**: String (boolean)
**Required**: No
**Default**: `true`
**Valid Values**: `true`, `false`

When destination table is empty, sync all historical data.

**Behavior**:

| Value | Destination Empty | start_timestamp |
|-------|------------------|----------------|
| `true` | Yes | `1900-01-01 00:00:00` (all history) |
| `false` | Yes | Current time - `fallback_days` |
| Either | No | `MAX(timestamp_column)` from table |

**Examples**:

```bash
# First run: sync all historical data
sync_all_on_empty_table=true
# Result: All data from source is synced

# First run: sync only recent data
sync_all_on_empty_table=false
fallback_days=7
# Result: Only last 7 days of data is synced
```

### fallback_days

**Type**: String (integer)
**Required**: No
**Default**: `1`
**Example**: `1`, `7`, `30`

Days to look back when `sync_all_on_empty_table=false` and table is empty.

**Use Cases**:

```bash
# Sync last 24 hours only
fallback_days=1

# Sync last week
fallback_days=7

# Sync last month
fallback_days=30
```

## Auto-Schema Parameters

### enable_auto_schema

**Type**: String (boolean)
**Required**: No
**Default**: `true`
**Valid Values**: `true`, `false`

Enable automatic schema detection and BigQuery table creation.

**When enabled**:
1. Connects to source database
2. Extracts schema metadata
3. Maps types to BigQuery equivalents
4. Creates optimized table with partitioning/clustering

**Example**:

```bash
enable_auto_schema=true
source_table_for_schema=public.orders
partition_field=order_date
clustering_fields=customer_id,status
```

### source_table_for_schema

**Type**: String
**Required**: When `enable_auto_schema=true`
**Format**: Varies by source type

Source table for schema detection.

**Format by Source**:

```bash
# PostgreSQL: schema.table
source_table_for_schema=public.orders
source_table_for_schema=analytics.user_events

# MongoDB: database.collection
source_table_for_schema=production.users
source_table_for_schema=analytics.events

# BigQuery: project.dataset.table
source_table_for_schema=source-project.raw_data.events
source_table_for_schema=analytics-prod.staging.users
```

### partition_field

**Type**: String
**Required**: No
**Default**: `updated_at`
**Example**: `created_at`, `order_date`, `event_timestamp`

Field to use for BigQuery time-based partitioning.

**Requirements**:
- Must be a DATE, TIMESTAMP, or DATETIME column
- Field must exist in source schema
- Improves query performance on time ranges

**Examples**:

```bash
# Partition by update timestamp
partition_field=updated_at

# Partition by creation date
partition_field=created_at

# Partition by event timestamp
partition_field=event_timestamp

# Partition by business date
partition_field=order_date
```

**Benefits**:
- Faster queries on date ranges
- Reduced query costs (only scan relevant partitions)
- Automatic partition management
- Better data organization

### clustering_fields

**Type**: String (comma-separated)
**Required**: No
**Example**: `user_id,status`, `customer_id,product_id`

Comma-separated list of fields for BigQuery table clustering.

**Requirements**:
- Maximum 4 clustering fields
- Order matters (most selective first)
- Fields must exist in schema

**Examples**:

```bash
# Single clustering field
clustering_fields=user_id

# Multiple clustering fields (order by selectivity)
clustering_fields=customer_id,status,region

# For analytics queries
clustering_fields=event_type,user_id

# For transaction data
clustering_fields=merchant_id,transaction_date
```

**Best Practices**:
- Put most selective field first
- Use fields commonly in WHERE clauses
- Limit to 3-4 fields maximum
- Consider query patterns

**Query Performance Example**:

```sql
-- Without clustering: Scans entire partition
SELECT * FROM orders WHERE customer_id = 12345

-- With clustering on customer_id: Scans only relevant blocks
-- Faster and cheaper!
```

### delete_existing_table

**Type**: String (boolean)
**Required**: No
**Default**: `false`
**Valid Values**: `true`, `false`

Delete existing BigQuery table before creating new one.

**Use Cases**:

```bash
# Recreate table with new schema
delete_existing_table=true
enable_auto_schema=true

# Keep existing table (append mode)
delete_existing_table=false
```

**Warning**: Setting to `true` will delete all existing data!

## Transformation Configuration

### transformation_config

**Type**: String (JSON)
**Required**: No
**Example**: `{"field_mappings": {"old_name": "new_name"}}`

JSON configuration for data transformations.

**Supported Transformations**:

#### Field Mapping

```json
{
  "field_mappings": {
    "old_column_name": "new_column_name",
    "user_email": "email",
    "created_timestamp": "created_at"
  }
}
```

#### Type Conversion

```json
{
  "type_conversions": {
    "age": "INTEGER",
    "price": "FLOAT",
    "is_active": "BOOLEAN"
  }
}
```

#### Default Values

```json
{
  "default_values": {
    "status": "active",
    "version": "1.0"
  }
}
```

#### Combined Example

```json
{
  "field_mappings": {
    "user_email": "email",
    "user_name": "name"
  },
  "type_conversions": {
    "age": "INTEGER",
    "score": "FLOAT"
  },
  "default_values": {
    "status": "pending",
    "source": "dataflow"
  }
}
```

**Usage**:

```bash
--parameters="transformation_config={\"field_mappings\":{\"old_name\":\"new_name\"}}"
```

## Performance Tuning

### batch_size

**Type**: Integer
**Required**: No
**Default**: `1000`
**Example**: `500`, `1000`, `5000`

Number of records to process in each batch.

**Tuning Guidelines**:

```bash
# Small records (< 1 KB)
batch_size=5000

# Medium records (1-10 KB)
batch_size=1000  # Default

# Large records (> 10 KB)
batch_size=100

# Very large records or complex transformations
batch_size=10
```

**Trade-offs**:
- Larger batches: Higher throughput, more memory
- Smaller batches: Lower memory, more overhead

### max_retries

**Type**: Integer
**Required**: No
**Default**: `3`
**Example**: `3`, `5`, `10`

Maximum number of retries for failed operations.

**Recommendations**:

```bash
# Stable environment
max_retries=3  # Default

# Unstable network
max_retries=5

# Critical data (ensure delivery)
max_retries=10
```

## Common Configuration Scenarios

### Scenario 1: Daily Full Refresh

Complete table replacement every day.

```bash
gcloud dataflow flex-template run daily-refresh-$(date +%Y%m%d) \
  --template-file-gcs-location=gs://${BUCKET}/templates/deltaflow \
  --region=us-central1 \
  --parameters="\
data_source=postgresql,\
postgresql_host=10.0.0.5,\
postgresql_database=production,\
postgresql_username=readonly_user,\
postgresql_password=SECURE_PASSWORD,\
postgresql_query=SELECT * FROM orders,\
destination_bigquery_project=analytics-prod,\
destination_bigquery_dataset=reporting,\
destination_bigquery_table=orders,\
write_disposition=WRITE_TRUNCATE,\
enable_auto_schema=true,\
source_table_for_schema=public.orders,\
partition_field=order_date,\
clustering_fields=customer_id,status"
```

### Scenario 2: Continuous Incremental Sync

Hourly sync of new data only (Smart Sync).

```bash
gcloud dataflow flex-template run hourly-incremental-$(date +%Y%m%d-%H%M) \
  --template-file-gcs-location=gs://${BUCKET}/templates/deltaflow \
  --region=us-central1 \
  --parameters="\
data_source=postgresql,\
postgresql_host=10.0.0.5,\
postgresql_database=production,\
postgresql_username=readonly_user,\
postgresql_password=SECURE_PASSWORD,\
enable_smart_sync=true,\
postgresql_base_query=SELECT * FROM orders WHERE updated_at > '{start_timestamp}' AND updated_at <= '{end_timestamp}' ORDER BY updated_at ASC,\
smart_sync_timestamp_column=updated_at,\
sync_all_on_empty_table=true,\
destination_bigquery_project=analytics-prod,\
destination_bigquery_dataset=realtime,\
destination_bigquery_table=orders,\
write_disposition=WRITE_APPEND,\
enable_auto_schema=true,\
source_table_for_schema=public.orders,\
partition_field=updated_at,\
clustering_fields=customer_id,status"
```

### Scenario 3: MongoDB to BigQuery with Filtering

Sync only active users with transformations.

```bash
gcloud dataflow flex-template run mongodb-users-sync \
  --template-file-gcs-location=gs://${BUCKET}/templates/deltaflow \
  --region=us-central1 \
  --parameters='\
data_source=mongodb,\
mongodb_host=mongodb.example.com,\
mongodb_port=27017,\
mongodb_database=user_data,\
mongodb_collection=users,\
mongodb_username=dataflow_reader,\
mongodb_password=SECURE_PASSWORD,\
mongodb_query={\"status\":\"active\",\"verified\":true},\
mongodb_projection={\"_id\":0,\"name\":1,\"email\":1,\"created_at\":1,\"last_login\":1},\
destination_bigquery_project=analytics-prod,\
destination_bigquery_dataset=user_analytics,\
destination_bigquery_table=active_users,\
enable_auto_schema=true,\
source_table_for_schema=user_data.users,\
partition_field=created_at,\
clustering_fields=email'
```

### Scenario 4: BigQuery Cross-Project with Query

Copy and transform data between projects.

```bash
gcloud dataflow flex-template run bigquery-cross-project \
  --template-file-gcs-location=gs://${BUCKET}/templates/deltaflow \
  --region=us-central1 \
  --parameters="\
data_source=bigquery,\
source_bigquery_project=source-project,\
source_bigquery_query=SELECT user_id, event_type, timestamp, JSON_EXTRACT_SCALAR(properties, '$.page') as page FROM \\\`source-project.raw.events\\\` WHERE DATE(timestamp) = CURRENT_DATE(),\
destination_bigquery_project=analytics-prod,\
destination_bigquery_dataset=processed,\
destination_bigquery_table=daily_events,\
write_disposition=WRITE_TRUNCATE,\
create_disposition=CREATE_IF_NEEDED"
```

### Scenario 5: Multi-Source Aggregation

Combine data from PostgreSQL and MongoDB.

```bash
gcloud dataflow flex-template run multi-source-sync \
  --template-file-gcs-location=gs://${BUCKET}/templates/deltaflow \
  --region=us-central1 \
  --parameters="\
data_source=postgresql,mongodb,\
postgresql_host=10.0.0.5,\
postgresql_database=orders_db,\
postgresql_query=SELECT * FROM orders WHERE DATE(created_at) = CURRENT_DATE,\
mongodb_host=mongodb.example.com,\
mongodb_database=user_data,\
mongodb_collection=profiles,\
mongodb_query={\"last_active\":{\"\$gte\":\"2025-01-01\"}},\
destination_bigquery_project=analytics-prod,\
destination_bigquery_dataset=combined,\
destination_bigquery_table=unified_data,\
write_disposition=WRITE_APPEND"
```

### Scenario 6: Initial Historical Load

One-time sync of all historical data.

```bash
gcloud dataflow flex-template run historical-load \
  --template-file-gcs-location=gs://${BUCKET}/templates/deltaflow \
  --region=us-central1 \
  --max-workers=50 \
  --worker-machine-type=n1-standard-4 \
  --parameters="\
data_source=postgresql,\
postgresql_host=10.0.0.5,\
postgresql_database=production,\
postgresql_query=SELECT * FROM orders,\
destination_bigquery_project=analytics-prod,\
destination_bigquery_dataset=warehouse,\
destination_bigquery_table=orders_historical,\
write_disposition=WRITE_TRUNCATE,\
batch_size=5000,\
enable_auto_schema=true,\
source_table_for_schema=public.orders,\
partition_field=order_date,\
clustering_fields=customer_id,region,status"
```

## Configuration Best Practices

### 1. Security

```bash
# ✅ DO: Use Secret Manager
postgresql_password=$(gcloud secrets versions access latest --secret=db-password)

# ❌ DON'T: Hardcode passwords
postgresql_password=mysecretpassword
```

### 2. Performance

```bash
# ✅ DO: Use Smart Sync for incremental loads
enable_smart_sync=true
postgresql_base_query="SELECT * FROM table WHERE updated_at > '{start_timestamp}' AND updated_at <= '{end_timestamp}'"

# ❌ DON'T: Full table scan every run
postgresql_query="SELECT * FROM large_table"
```

### 3. Cost Optimization

```bash
# ✅ DO: Use partitioning and clustering
partition_field=created_at
clustering_fields=user_id,status

# ✅ DO: Select only needed columns
postgresql_query="SELECT id, name, email FROM users"

# ❌ DON'T: Select everything
postgresql_query="SELECT * FROM users"
```

### 4. Reliability

```bash
# ✅ DO: Set appropriate retries
max_retries=5

# ✅ DO: Use WRITE_APPEND for incremental data
write_disposition=WRITE_APPEND

# ❌ DON'T: Use WRITE_TRUNCATE for incremental syncs
write_disposition=WRITE_TRUNCATE  # Data loss risk!
```

### 5. Monitoring

```bash
# ✅ DO: Use descriptive job names
--job-name=orders-sync-$(date +%Y%m%d-%H%M%S)

# ✅ DO: Tag jobs with metadata
--labels=environment=prod,team=data-engineering,source=postgresql
```

---

**Document Version**: 1.0.0
**Last Updated**: January 2025
**Maintainer**: Data Engineering Team

For architecture details, see [ARCHITECTURE.md](ARCHITECTURE.md).
For deployment instructions, see [DEPLOYMENT.md](DEPLOYMENT.md).
