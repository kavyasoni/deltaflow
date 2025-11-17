#!/bin/bash
#
# Smart Sync Example with Scheduling
#
# This example demonstrates the Smart Sync feature which enables incremental
# synchronization by tracking the last synced timestamp in BigQuery.
#
# How Smart Sync works:
# 1. Queries BigQuery for MAX(updated_at) from destination table
# 2. Syncs only records with updated_at > MAX(updated_at)
# 3. On empty table: syncs all historical data (configurable)
#
# Perfect for scheduled runs - each run only processes new/updated records!
#
# Prerequisites:
# - Template deployed to GCS
# - PostgreSQL instance with timestamp column (e.g., updated_at)
# - BigQuery dataset created
# - Source table must have timestamp column for tracking changes
#

# Configuration
export PROJECT_ID="your-project-id"
export REGION="us-central1"
export TEMPLATE_BUCKET="${PROJECT_ID}-dataflow-templates"
export TEMPLATE_NAME="kk-custom-data-sync-template"
export JOB_NAME="smart-sync-users-$(date +%Y%m%d-%H%M%S)"

# PostgreSQL Source Configuration
export PG_HOST="10.0.0.5"
export PG_PORT="5432"
export PG_DATABASE="production_db"
export PG_USERNAME="dataflow_user"
export PG_PASSWORD="secure_password_here"

# Smart Sync Configuration
# Base query with timestamp placeholders - Smart Sync will fill these in
export PG_BASE_QUERY="SELECT * FROM users WHERE updated_at > '{start_timestamp}' AND updated_at <= '{end_timestamp}' ORDER BY updated_at ASC"
export TIMESTAMP_COLUMN="updated_at"

# BigQuery Destination Configuration
export BQ_PROJECT="${PROJECT_ID}"
export BQ_DATASET="analytics"
export BQ_TABLE="users"

# Smart Sync Options
export ENABLE_SMART_SYNC="true"
export SYNC_ALL_ON_EMPTY="true"  # If destination table is empty, sync all historical data
export FALLBACK_DAYS="7"         # If Smart Sync fails, sync last N days

# Run the Dataflow job with Smart Sync
gcloud dataflow flex-template run "${JOB_NAME}" \
  --template-file-gcs-location="gs://${TEMPLATE_BUCKET}/templates/${TEMPLATE_NAME}" \
  --region="${REGION}" \
  --parameters="data_source=postgresql" \
  --parameters="postgresql_host=${PG_HOST}" \
  --parameters="postgresql_port=${PG_PORT}" \
  --parameters="postgresql_database=${PG_DATABASE}" \
  --parameters="postgresql_username=${PG_USERNAME}" \
  --parameters="postgresql_password=${PG_PASSWORD}" \
  --parameters="postgresql_base_query=${PG_BASE_QUERY}" \
  --parameters="timestamp_column=${TIMESTAMP_COLUMN}" \
  --parameters="destination_bigquery_project=${BQ_PROJECT}" \
  --parameters="destination_bigquery_dataset=${BQ_DATASET}" \
  --parameters="destination_bigquery_table=${BQ_TABLE}" \
  --parameters="enable_smart_sync=${ENABLE_SMART_SYNC}" \
  --parameters="sync_all_on_empty_table=${SYNC_ALL_ON_EMPTY}" \
  --parameters="fallback_days=${FALLBACK_DAYS}" \
  --parameters="enable_auto_schema=true" \
  --parameters="write_disposition=WRITE_APPEND"

echo ""
echo "Smart Sync job submitted: ${JOB_NAME}"
echo ""
echo "Smart Sync will:"
echo "  1. Query BigQuery: SELECT MAX(${TIMESTAMP_COLUMN}) FROM ${BQ_PROJECT}.${BQ_DATASET}.${BQ_TABLE}"
echo "  2. Sync only records where updated_at > MAX(${TIMESTAMP_COLUMN})"
echo "  3. If table is empty and sync_all_on_empty_table=true, sync all historical data"
echo ""
echo "View job status:"
echo "  gcloud dataflow jobs describe ${JOB_NAME} --region=${REGION}"
echo ""

# Optional: Create Cloud Scheduler job for automated runs every 2 hours
read -p "Create Cloud Scheduler job for automated runs every 2 hours? (y/n) " -n 1 -r
echo
if [[ $REPLY =~ ^[Yy]$ ]]
then
    export SCHEDULER_JOB_NAME="smart-sync-users-every-2h"

    # Create scheduler job
    gcloud scheduler jobs create http "${SCHEDULER_JOB_NAME}" \
        --location="${REGION}" \
        --schedule="0 */2 * * *" \
        --time-zone="UTC" \
        --uri="https://dataflow.googleapis.com/v1b3/projects/${PROJECT_ID}/locations/${REGION}/flexTemplates:launch" \
        --http-method="POST" \
        --message-body="{
            \"launch_parameter\": {
                \"jobName\": \"smart-sync-users-scheduled-\$(date +%Y%m%d-%H%M%S)\",
                \"containerSpecGcsPath\": \"gs://${TEMPLATE_BUCKET}/templates/${TEMPLATE_NAME}\",
                \"parameters\": {
                    \"data_source\": \"postgresql\",
                    \"postgresql_host\": \"${PG_HOST}\",
                    \"postgresql_port\": \"${PG_PORT}\",
                    \"postgresql_database\": \"${PG_DATABASE}\",
                    \"postgresql_username\": \"${PG_USERNAME}\",
                    \"postgresql_password\": \"${PG_PASSWORD}\",
                    \"postgresql_base_query\": \"${PG_BASE_QUERY}\",
                    \"timestamp_column\": \"${TIMESTAMP_COLUMN}\",
                    \"destination_bigquery_project\": \"${BQ_PROJECT}\",
                    \"destination_bigquery_dataset\": \"${BQ_DATASET}\",
                    \"destination_bigquery_table\": \"${BQ_TABLE}\",
                    \"enable_smart_sync\": \"${ENABLE_SMART_SYNC}\",
                    \"sync_all_on_empty_table\": \"${SYNC_ALL_ON_EMPTY}\",
                    \"fallback_days\": \"${FALLBACK_DAYS}\",
                    \"enable_auto_schema\": \"true\",
                    \"write_disposition\": \"WRITE_APPEND\"
                }
            }
        }" \
        --oauth-service-account-email="${PROJECT_ID}@appspot.gserviceaccount.com"

    echo ""
    echo "Cloud Scheduler job created: ${SCHEDULER_JOB_NAME}"
    echo "Schedule: Every 2 hours (0 */2 * * *)"
    echo ""
    echo "Manage scheduler:"
    echo "  gcloud scheduler jobs list --location=${REGION}"
    echo "  gcloud scheduler jobs pause ${SCHEDULER_JOB_NAME} --location=${REGION}"
    echo "  gcloud scheduler jobs resume ${SCHEDULER_JOB_NAME} --location=${REGION}"
fi
