# DeltaFlow Examples

This directory contains example configurations for running the DeltaFlow custom Dataflow template in various scenarios.

## Available Examples

### 1. PostgreSQL to BigQuery (`postgresql_to_bigquery.sh`)

Basic synchronization from PostgreSQL to BigQuery with automatic schema detection.

**Use case**: One-time or scheduled sync of PostgreSQL tables to BigQuery for analytics.

**Features demonstrated**:
- PostgreSQL connection configuration
- Custom SQL query for data extraction
- Auto-schema detection
- Basic BigQuery write configuration

**Run the example**:
```bash
# Edit the script to set your configuration
vim postgresql_to_bigquery.sh

# Make it executable
chmod +x postgresql_to_bigquery.sh

# Run it
./postgresql_to_bigquery.sh
```

**Key parameters to customize**:
- `PROJECT_ID`: Your GCP project ID
- `PG_HOST`, `PG_DATABASE`, `PG_USERNAME`, `PG_PASSWORD`: PostgreSQL connection details
- `PG_QUERY`: SQL query to extract data
- `BQ_DATASET`, `BQ_TABLE`: BigQuery destination

---

### 2. MongoDB to BigQuery (`mongodb_to_bigquery.sh`)

Synchronization from MongoDB to BigQuery with JSON field handling and schema detection.

**Use case**: Sync MongoDB collections to BigQuery for analytics and reporting.

**Features demonstrated**:
- MongoDB connection via URI
- Query filtering with MongoDB query syntax
- Field projection to select specific fields
- Automatic handling of ObjectId and nested documents
- JSON type field mapping

**Run the example**:
```bash
# Edit the script to set your configuration
vim mongodb_to_bigquery.sh

# Make it executable
chmod +x mongodb_to_bigquery.sh

# Run it
./mongodb_to_bigquery.sh
```

**Key parameters to customize**:
- `MONGO_URI`: MongoDB connection string
- `MONGO_DATABASE`, `MONGO_COLLECTION`: MongoDB source
- `MONGO_FILTER`: MongoDB query filter (JSON)
- `MONGO_PROJECTION`: Field projection (optional)
- `BQ_DATASET`, `BQ_TABLE`: BigQuery destination

**MongoDB-specific notes**:
- ObjectId fields are automatically converted to STRING
- Nested documents are stored as JSON type fields in BigQuery
- Arrays are preserved and mapped to REPEATED or JSON fields
- BSON types are handled automatically

---

### 3. Smart Sync Example (`smart_sync_example.sh`)

Incremental synchronization using Smart Sync feature with optional Cloud Scheduler setup.

**Use case**: Automated incremental sync that only processes new/updated records since the last run.

**Features demonstrated**:
- Smart Sync configuration
- Timestamp-based incremental updates
- Empty table handling (sync all historical data)
- Fallback configuration for error scenarios
- Cloud Scheduler integration for automated runs

**How Smart Sync works**:
1. **First run** (empty BigQuery table):
   - If `sync_all_on_empty_table=true`: Syncs all historical data from source
   - If `false`: Syncs data from last N days (configured via `fallback_days`)

2. **Subsequent runs**:
   - Queries BigQuery: `SELECT MAX(updated_at) FROM destination_table`
   - Syncs only records where `updated_at > MAX(updated_at)`
   - Efficient - only processes new/changed data

3. **Fallback behavior**:
   - If Smart Sync fails (e.g., BigQuery query error), falls back to syncing last N days
   - Ensures data continuity even with temporary issues

**Run the example**:
```bash
# Edit the script to set your configuration
vim smart_sync_example.sh

# Make it executable
chmod +x smart_sync_example.sh

# Run it
./smart_sync_example.sh

# Optionally set up Cloud Scheduler when prompted
```

**Key parameters to customize**:
- `PG_BASE_QUERY`: Query template with `{start_timestamp}` and `{end_timestamp}` placeholders
- `TIMESTAMP_COLUMN`: Column name containing update timestamp
- `ENABLE_SMART_SYNC`: Set to "true" to enable Smart Sync
- `SYNC_ALL_ON_EMPTY`: Set to "true" to sync all data when destination table is empty
- `FALLBACK_DAYS`: Number of days to sync if Smart Sync fails

**Required query format**:
```sql
SELECT * FROM table_name
WHERE updated_at > '{start_timestamp}'
AND updated_at <= '{end_timestamp}'
ORDER BY updated_at ASC
```

The placeholders `{start_timestamp}` and `{end_timestamp}` will be replaced at runtime.

---

## Prerequisites

Before running any example, ensure:

1. **Template deployed**: The DeltaFlow template must be built and deployed to GCS
   ```bash
   # See main README.md for deployment instructions
   ./deploy.sh
   ```

2. **Source database accessible**: Dataflow workers must be able to connect to your source database
   - Use VPC configuration if database is in private network
   - Configure firewall rules to allow Dataflow worker IPs
   - Test connectivity from a GCE instance in the same network

3. **BigQuery dataset created**:
   ```bash
   bq mk --dataset --location=US your-project:analytics
   ```

4. **IAM permissions configured**:
   - Dataflow Service Account needs:
     - `roles/dataflow.worker`
     - `roles/bigquery.dataEditor` (on destination dataset)
     - `roles/storage.objectAdmin` (on template bucket)

5. **Secrets management** (production):
   - Use Google Secret Manager for passwords
   - Reference secrets in parameters: `${secret:projects/PROJECT_ID/secrets/SECRET_NAME/versions/latest}`

---

## Customizing Examples

### Network Configuration

If your source database is in a private VPC network, add these parameters:

```bash
--parameters="network=projects/${PROJECT_ID}/global/networks/your-network" \
--parameters="subnetwork=regions/${REGION}/subnetworks/your-subnet"
```

### Dataflow Worker Configuration

Customize worker machine type and autoscaling:

```bash
--parameters="machine_type=n1-standard-4" \
--parameters="max_num_workers=10" \
--parameters="num_workers=2"
```

### Write Disposition Options

Control how data is written to BigQuery:

- `WRITE_APPEND`: Append to existing table (default, recommended for Smart Sync)
- `WRITE_TRUNCATE`: Truncate table before writing (full refresh)
- `WRITE_EMPTY`: Fail if table already has data

```bash
--parameters="write_disposition=WRITE_TRUNCATE"  # For full refresh
```

### Schema Detection Options

Control automatic schema detection:

```bash
--parameters="enable_auto_schema=true"      # Enable auto-schema (default)
--parameters="partition_field=updated_at"   # Partition table by this field
--parameters="clustering_fields=user_id,created_at"  # Cluster by these fields
```

---

## Monitoring and Debugging

### View job status
```bash
gcloud dataflow jobs list --region=us-central1
gcloud dataflow jobs describe JOB_ID --region=us-central1
```

### View job logs
```bash
gcloud dataflow jobs log JOB_ID --region=us-central1
```

### Common issues

**"Query cannot be empty"**
- Ensure `postgresql_query` or `postgresql_base_query` is provided
- For Smart Sync, verify the base query has the correct placeholders

**"Connection refused" / "Timeout"**
- Check VPC network configuration
- Verify firewall rules allow Dataflow worker IPs
- Test database connectivity from GCE instance in same network

**"BigQuery JSON validation error"**
- Data contains null bytes or invalid UTF-8
- Use data validation/transformation features
- Check source data quality

**"Template not found"**
- Verify template is deployed: `gsutil ls gs://BUCKET/templates/`
- Check GCS path in gcloud command

---

## Production Best Practices

1. **Use Secret Manager**: Store database credentials in Google Secret Manager
   ```bash
   echo -n "password" | gcloud secrets create db-password --data-file=-
   ```

2. **Enable VPC Service Controls**: Protect data in transit and at rest

3. **Set up monitoring**: Use Cloud Monitoring for job metrics and alerts
   ```bash
   # Create alert for failed jobs
   gcloud alpha monitoring policies create \
     --notification-channels=CHANNEL_ID \
     --display-name="Dataflow Job Failures" \
     --condition-display-name="Job Failed" \
     --condition-threshold-value=1 \
     --condition-threshold-duration=60s
   ```

4. **Use Cloud Scheduler**: Automate regular syncs (see Smart Sync example)

5. **Enable Dataflow logs**: Send worker logs to Cloud Logging for debugging

6. **Tag resources**: Use labels for cost tracking and organization
   ```bash
   --parameters="labels={\"team\":\"data-engineering\",\"env\":\"production\"}"
   ```

---

## Next Steps

- Review main [README.md](../README.md) for architecture details
- Check [IMPLEMENTATION_DETAILS.md](../IMPLEMENTATION_DETAILS.md) for technical deep-dive
- See [metadata.json](../metadata.json) for all available parameters
- Contribute your own examples via pull request!

---

## Support

For issues, questions, or contributions:
- Open an issue on GitHub
- Review existing issues for solutions
- Contribute improvements via pull request
