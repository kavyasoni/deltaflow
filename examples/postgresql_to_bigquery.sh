#!/bin/bash
#
# PostgreSQL to BigQuery Sync Example
#
# This example demonstrates a basic sync from PostgreSQL to BigQuery
# using the DeltaFlow custom Dataflow template.
#
# Prerequisites:
# - Template deployed to GCS
# - PostgreSQL instance accessible from Dataflow workers
# - BigQuery dataset created
# - Appropriate IAM permissions configured
#

# Configuration
export PROJECT_ID="your-project-id"
export REGION="us-central1"
export TEMPLATE_BUCKET="${PROJECT_ID}-dataflow-templates"
export TEMPLATE_NAME="kk-custom-data-sync-template"
export JOB_NAME="postgresql-to-bigquery-$(date +%Y%m%d-%H%M%S)"

# PostgreSQL Source Configuration
export PG_HOST="10.0.0.5"
export PG_PORT="5432"
export PG_DATABASE="production_db"
export PG_USERNAME="dataflow_user"
export PG_PASSWORD="secure_password_here"
export PG_QUERY="SELECT id, name, email, created_at, updated_at FROM users WHERE active = true"

# BigQuery Destination Configuration
export BQ_PROJECT="${PROJECT_ID}"
export BQ_DATASET="analytics"
export BQ_TABLE="users"

# Run the Dataflow job
gcloud dataflow flex-template run "${JOB_NAME}" \
  --template-file-gcs-location="gs://${TEMPLATE_BUCKET}/templates/${TEMPLATE_NAME}" \
  --region="${REGION}" \
  --parameters="data_source=postgresql" \
  --parameters="postgresql_host=${PG_HOST}" \
  --parameters="postgresql_port=${PG_PORT}" \
  --parameters="postgresql_database=${PG_DATABASE}" \
  --parameters="postgresql_username=${PG_USERNAME}" \
  --parameters="postgresql_password=${PG_PASSWORD}" \
  --parameters="postgresql_query=${PG_QUERY}" \
  --parameters="destination_bigquery_project=${BQ_PROJECT}" \
  --parameters="destination_bigquery_dataset=${BQ_DATASET}" \
  --parameters="destination_bigquery_table=${BQ_TABLE}" \
  --parameters="enable_auto_schema=true" \
  --parameters="write_disposition=WRITE_APPEND"

# Monitor job status
echo ""
echo "Job submitted: ${JOB_NAME}"
echo "View job status:"
echo "  gcloud dataflow jobs describe ${JOB_NAME} --region=${REGION}"
echo ""
echo "View job logs:"
echo "  gcloud dataflow jobs log ${JOB_NAME} --region=${REGION}"
