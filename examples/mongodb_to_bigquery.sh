#!/bin/bash
#
# MongoDB to BigQuery Sync Example
#
# This example demonstrates syncing data from MongoDB to BigQuery
# with automatic schema detection and JSON field handling.
#
# Prerequisites:
# - Template deployed to GCS
# - MongoDB instance accessible from Dataflow workers
# - BigQuery dataset created
# - Appropriate IAM permissions configured
#

# Configuration
export PROJECT_ID="your-project-id"
export REGION="us-central1"
export TEMPLATE_BUCKET="${PROJECT_ID}-dataflow-templates"
export TEMPLATE_NAME="kk-custom-data-sync-template"
export JOB_NAME="mongodb-to-bigquery-$(date +%Y%m%d-%H%M%S)"

# MongoDB Source Configuration
export MONGO_URI="mongodb://dataflow_user:secure_password@10.0.0.10:27017"
export MONGO_DATABASE="ecommerce"
export MONGO_COLLECTION="orders"
# MongoDB query filter (JSON string)
export MONGO_FILTER='{"status": {"$in": ["completed", "shipped"]}}'
# Optional: Projection to include specific fields
export MONGO_PROJECTION='{"_id": 1, "order_id": 1, "customer_id": 1, "total": 1, "items": 1, "created_at": 1, "updated_at": 1}'

# BigQuery Destination Configuration
export BQ_PROJECT="${PROJECT_ID}"
export BQ_DATASET="analytics"
export BQ_TABLE="orders"

# Run the Dataflow job
gcloud dataflow flex-template run "${JOB_NAME}" \
  --template-file-gcs-location="gs://${TEMPLATE_BUCKET}/templates/${TEMPLATE_NAME}" \
  --region="${REGION}" \
  --parameters="data_source=mongodb" \
  --parameters="mongodb_uri=${MONGO_URI}" \
  --parameters="mongodb_database=${MONGO_DATABASE}" \
  --parameters="mongodb_collection=${MONGO_COLLECTION}" \
  --parameters="mongodb_filter=${MONGO_FILTER}" \
  --parameters="mongodb_projection=${MONGO_PROJECTION}" \
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
echo ""
echo "Note: MongoDB ObjectId fields will be converted to STRING in BigQuery"
echo "      Nested documents will be stored as JSON type fields"
