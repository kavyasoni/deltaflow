# DeltaFlow Deployment Guide

This comprehensive guide covers everything you need to deploy DeltaFlow to production, from initial setup to monitoring and troubleshooting.

## Table of Contents

1. [Prerequisites](#prerequisites)
2. [GCP Project Setup](#gcp-project-setup)
3. [IAM and Permissions](#iam-and-permissions)
4. [Docker Build and Registry](#docker-build-and-registry)
5. [Template Deployment](#template-deployment)
6. [Running Jobs](#running-jobs)
7. [Environment Management](#environment-management)
8. [Cloud Scheduler Integration](#cloud-scheduler-integration)
9. [Network Configuration](#network-configuration)
10. [Monitoring and Logging](#monitoring-and-logging)
11. [Troubleshooting](#troubleshooting)
12. [Best Practices](#best-practices)

## Prerequisites

### Required Tools

```bash
# Check installed versions
gcloud version              # Google Cloud SDK 400.0.0+
docker --version            # Docker 20.10+
python --version            # Python 3.9+
```

### Install Google Cloud SDK

```bash
# macOS
brew install google-cloud-sdk

# Linux
curl https://sdk.cloud.google.com | bash
exec -l $SHELL

# Windows
# Download installer from https://cloud.google.com/sdk/docs/install
```

### Install Docker

```bash
# macOS
brew install docker

# Linux (Ubuntu/Debian)
curl -fsSL https://get.docker.com -o get-docker.sh
sudo sh get-docker.sh

# Verify installation
docker run hello-world
```

### Required Access

- GCP Project with Owner or Editor role
- Permission to enable APIs
- Permission to create service accounts
- Access to source databases (PostgreSQL, MongoDB, etc.)

## GCP Project Setup

### Step 1: Create or Select Project

```bash
# Create new project
gcloud projects create deltaflow-prod --name="DeltaFlow Production"

# Or select existing project
export PROJECT_ID="your-existing-project-id"
gcloud config set project $PROJECT_ID

# Verify current project
gcloud config get-value project
```

### Step 2: Enable Required APIs

```bash
# Enable all required APIs at once
gcloud services enable \
  dataflow.googleapis.com \
  cloudbuild.googleapis.com \
  storage.googleapis.com \
  bigquery.googleapis.com \
  containerregistry.googleapis.com \
  logging.googleapis.com \
  monitoring.googleapis.com \
  cloudscheduler.googleapis.com \
  compute.googleapis.com

# Verify APIs are enabled
gcloud services list --enabled --filter="name:dataflow OR name:bigquery OR name:storage"
```

### Step 3: Set Up Billing

```bash
# Link billing account (required for Dataflow)
gcloud beta billing projects link $PROJECT_ID \
  --billing-account=BILLING_ACCOUNT_ID

# Verify billing is enabled
gcloud beta billing projects describe $PROJECT_ID
```

### Step 4: Create Storage Bucket

```bash
# Set bucket name
export BUCKET_NAME="${PROJECT_ID}-dataflow-templates"

# Create regional bucket (recommended for performance)
gsutil mb -p $PROJECT_ID \
  -c STANDARD \
  -l us-central1 \
  gs://$BUCKET_NAME

# Or create multi-regional bucket (for global access)
gsutil mb -p $PROJECT_ID \
  -c STANDARD \
  -l US \
  gs://$BUCKET_NAME

# Set lifecycle policy (optional - auto-delete old temp files)
cat > lifecycle.json << EOF
{
  "lifecycle": {
    "rule": [
      {
        "action": {"type": "Delete"},
        "condition": {
          "age": 7,
          "matchesPrefix": ["temp/"]
        }
      }
    ]
  }
}
EOF

gsutil lifecycle set lifecycle.json gs://$BUCKET_NAME

# Verify bucket creation
gsutil ls -L gs://$BUCKET_NAME
```

## IAM and Permissions

### Service Account Creation

```bash
# Create Dataflow worker service account
export SA_NAME="deltaflow-worker"
export SA_EMAIL="${SA_NAME}@${PROJECT_ID}.iam.gserviceaccount.com"

gcloud iam service-accounts create $SA_NAME \
  --display-name="DeltaFlow Dataflow Worker" \
  --description="Service account for DeltaFlow Dataflow workers"

# Verify service account creation
gcloud iam service-accounts list --filter="email:${SA_EMAIL}"
```

### Grant Required Roles

```bash
# Dataflow Worker role
gcloud projects add-iam-policy-binding $PROJECT_ID \
  --member="serviceAccount:${SA_EMAIL}" \
  --role="roles/dataflow.worker"

# BigQuery Data Editor (read/write BigQuery tables)
gcloud projects add-iam-policy-binding $PROJECT_ID \
  --member="serviceAccount:${SA_EMAIL}" \
  --role="roles/bigquery.dataEditor"

# BigQuery Job User (run BigQuery queries)
gcloud projects add-iam-policy-binding $PROJECT_ID \
  --member="serviceAccount:${SA_EMAIL}" \
  --role="roles/bigquery.jobUser"

# Storage Object Admin (read/write GCS)
gcloud projects add-iam-policy-binding $PROJECT_ID \
  --member="serviceAccount:${SA_EMAIL}" \
  --role="roles/storage.objectAdmin"

# Logs Writer (write logs)
gcloud projects add-iam-policy-binding $PROJECT_ID \
  --member="serviceAccount:${SA_EMAIL}" \
  --role="roles/logging.logWriter"

# Monitoring Metric Writer (write metrics)
gcloud projects add-iam-policy-binding $PROJECT_ID \
  --member="serviceAccount:${SA_EMAIL}" \
  --role="roles/monitoring.metricWriter"

# Verify all roles
gcloud projects get-iam-policy $PROJECT_ID \
  --flatten="bindings[].members" \
  --filter="bindings.members:serviceAccount:${SA_EMAIL}"
```

### Create Service Account Key (for local development)

```bash
# Create key file
gcloud iam service-accounts keys create ~/deltaflow-key.json \
  --iam-account=$SA_EMAIL

# Set environment variable
export GOOGLE_APPLICATION_CREDENTIALS=~/deltaflow-key.json

# Verify authentication
gcloud auth application-default print-access-token
```

## Docker Build and Registry

### Authenticate Docker with GCR

```bash
# Configure Docker to use gcloud as credential helper
gcloud auth configure-docker

# Verify configuration
cat ~/.docker/config.json | grep gcr.io
```

### Build Docker Image

```bash
# Set image variables
export PROJECT_ID="your-project-id"
export VERSION="1.0.0"
export IMAGE_NAME="gcr.io/${PROJECT_ID}/dataflow/deltaflow:${VERSION}"
export LATEST_IMAGE="gcr.io/${PROJECT_ID}/dataflow/deltaflow:latest"

# Navigate to project root
cd /path/to/deltaflow

# Build image
docker build -t $IMAGE_NAME .

# Tag as latest
docker tag $IMAGE_NAME $LATEST_IMAGE

# Verify image was built
docker images | grep deltaflow
```

### Push to Google Container Registry

```bash
# Push versioned image
docker push $IMAGE_NAME

# Push latest tag
docker push $LATEST_IMAGE

# Verify images in GCR
gcloud container images list --repository=gcr.io/$PROJECT_ID/dataflow

# List all tags
gcloud container images list-tags gcr.io/$PROJECT_ID/dataflow/deltaflow
```

### Build Script (Optional)

Create `scripts/build_and_push.sh`:

```bash
#!/bin/bash
set -e

# Configuration
PROJECT_ID="${PROJECT_ID:-your-project-id}"
VERSION="${1:-latest}"
IMAGE_BASE="gcr.io/${PROJECT_ID}/dataflow/deltaflow"

echo "Building DeltaFlow image..."
echo "Project: $PROJECT_ID"
echo "Version: $VERSION"

# Build
docker build -t "${IMAGE_BASE}:${VERSION}" .

# Tag as latest if version is specified
if [ "$VERSION" != "latest" ]; then
  docker tag "${IMAGE_BASE}:${VERSION}" "${IMAGE_BASE}:latest"
fi

# Push
echo "Pushing to GCR..."
docker push "${IMAGE_BASE}:${VERSION}"

if [ "$VERSION" != "latest" ]; then
  docker push "${IMAGE_BASE}:latest"
fi

echo "✅ Build and push complete!"
echo "Image: ${IMAGE_BASE}:${VERSION}"
```

## Template Deployment

### Deploy Flex Template

```bash
# Set template variables
export PROJECT_ID="your-project-id"
export BUCKET_NAME="${PROJECT_ID}-dataflow-templates"
export TEMPLATE_NAME="deltaflow"
export IMAGE_NAME="gcr.io/${PROJECT_ID}/dataflow/deltaflow:1.0.0"

# Deploy template
gcloud dataflow flex-template build \
  gs://${BUCKET_NAME}/templates/${TEMPLATE_NAME} \
  --image $IMAGE_NAME \
  --sdk-language PYTHON \
  --metadata-file metadata.json

# Verify template was deployed
gsutil ls gs://${BUCKET_NAME}/templates/

# View template metadata
gsutil cat gs://${BUCKET_NAME}/templates/${TEMPLATE_NAME}
```

### Verify Template Deployment

```bash
# Check template exists
gsutil ls -l gs://${BUCKET_NAME}/templates/${TEMPLATE_NAME}

# Download and inspect metadata
gsutil cat gs://${BUCKET_NAME}/templates/${TEMPLATE_NAME} | jq .

# Test template with dry-run (if supported)
gcloud dataflow flex-template run test-dry-run \
  --template-file-gcs-location=gs://${BUCKET_NAME}/templates/${TEMPLATE_NAME} \
  --region=us-central1 \
  --parameters="data_source=bigquery" \
  --dry-run
```

## Running Jobs

### Run Job via gcloud CLI

#### Basic PostgreSQL to BigQuery

```bash
gcloud dataflow flex-template run deltaflow-job-$(date +%Y%m%d-%H%M%S) \
  --template-file-gcs-location=gs://${BUCKET_NAME}/templates/${TEMPLATE_NAME} \
  --region=us-central1 \
  --service-account-email=${SA_EMAIL} \
  --parameters="\
data_source=postgresql,\
postgresql_host=10.0.0.5,\
postgresql_database=production_db,\
postgresql_username=dataflow_user,\
postgresql_password=SECURE_PASSWORD,\
postgresql_query=SELECT * FROM orders,\
destination_bigquery_project=${PROJECT_ID},\
destination_bigquery_dataset=analytics,\
destination_bigquery_table=orders"
```

#### With Smart Sync + Auto-Schema

```bash
gcloud dataflow flex-template run deltaflow-smart-sync-$(date +%Y%m%d-%H%M%S) \
  --template-file-gcs-location=gs://${BUCKET_NAME}/templates/${TEMPLATE_NAME} \
  --region=us-central1 \
  --service-account-email=${SA_EMAIL} \
  --max-workers=10 \
  --parameters="\
data_source=postgresql,\
postgresql_host=10.0.0.5,\
postgresql_database=production_db,\
postgresql_username=dataflow_user,\
postgresql_password=SECURE_PASSWORD,\
enable_smart_sync=true,\
postgresql_base_query=SELECT * FROM orders WHERE updated_at > '{start_timestamp}' AND updated_at <= '{end_timestamp}' ORDER BY updated_at ASC,\
smart_sync_timestamp_column=updated_at,\
enable_auto_schema=true,\
source_table_for_schema=public.orders,\
partition_field=order_date,\
clustering_fields=customer_id,status,\
destination_bigquery_project=${PROJECT_ID},\
destination_bigquery_dataset=analytics,\
destination_bigquery_table=orders"
```

#### With VPC Network Configuration

```bash
gcloud dataflow flex-template run deltaflow-vpc-$(date +%Y%m%d-%H%M%S) \
  --template-file-gcs-location=gs://${BUCKET_NAME}/templates/${TEMPLATE_NAME} \
  --region=us-central1 \
  --network=projects/${PROJECT_ID}/global/networks/dataflow-vpc \
  --subnetwork=regions/us-central1/subnetworks/dataflow-subnet \
  --disable-public-ips \
  --service-account-email=${SA_EMAIL} \
  --max-workers=5 \
  --worker-machine-type=n1-standard-2 \
  --parameters="..."
```

### Run Job via GCP Console

1. Navigate to **Dataflow** in GCP Console
2. Click **Create Job from Template**
3. Select **Custom Template**
4. Enter template GCS path: `gs://${BUCKET_NAME}/templates/${TEMPLATE_NAME}`
5. Fill in required parameters
6. Click **Run Job**

### Monitor Job Execution

```bash
# List running jobs
gcloud dataflow jobs list --region=us-central1 --status=active

# Get job details
export JOB_ID="your-job-id"
gcloud dataflow jobs describe $JOB_ID --region=us-central1

# Stream job logs
gcloud dataflow jobs show $JOB_ID --region=us-central1

# Cancel a job
gcloud dataflow jobs cancel $JOB_ID --region=us-central1
```

## Environment Management

### Development Environment

```bash
# Development configuration
export ENV="dev"
export PROJECT_ID="deltaflow-dev"
export BUCKET_NAME="${PROJECT_ID}-dataflow-templates"
export IMAGE_TAG="dev"
export MAX_WORKERS=2
export WORKER_MACHINE_TYPE="n1-standard-1"

# Build and deploy dev
docker build -t gcr.io/${PROJECT_ID}/dataflow/deltaflow:${IMAGE_TAG} .
docker push gcr.io/${PROJECT_ID}/dataflow/deltaflow:${IMAGE_TAG}

gcloud dataflow flex-template build \
  gs://${BUCKET_NAME}/templates/deltaflow-dev \
  --image gcr.io/${PROJECT_ID}/dataflow/deltaflow:${IMAGE_TAG} \
  --sdk-language PYTHON \
  --metadata-file metadata.json
```

### Staging Environment

```bash
# Staging configuration
export ENV="staging"
export PROJECT_ID="deltaflow-staging"
export BUCKET_NAME="${PROJECT_ID}-dataflow-templates"
export IMAGE_TAG="staging"
export MAX_WORKERS=5
export WORKER_MACHINE_TYPE="n1-standard-2"

# Build and deploy staging
docker build -t gcr.io/${PROJECT_ID}/dataflow/deltaflow:${IMAGE_TAG} .
docker push gcr.io/${PROJECT_ID}/dataflow/deltaflow:${IMAGE_TAG}

gcloud dataflow flex-template build \
  gs://${BUCKET_NAME}/templates/deltaflow-staging \
  --image gcr.io/${PROJECT_ID}/dataflow/deltaflow:${IMAGE_TAG} \
  --sdk-language PYTHON \
  --metadata-file metadata.json
```

### Production Environment

```bash
# Production configuration
export ENV="prod"
export PROJECT_ID="deltaflow-prod"
export BUCKET_NAME="${PROJECT_ID}-dataflow-templates"
export VERSION="1.0.0"
export IMAGE_TAG="v${VERSION}"
export MAX_WORKERS=20
export WORKER_MACHINE_TYPE="n1-standard-4"

# Build production with version tag
docker build -t gcr.io/${PROJECT_ID}/dataflow/deltaflow:${IMAGE_TAG} .
docker tag gcr.io/${PROJECT_ID}/dataflow/deltaflow:${IMAGE_TAG} \
       gcr.io/${PROJECT_ID}/dataflow/deltaflow:latest
docker push gcr.io/${PROJECT_ID}/dataflow/deltaflow:${IMAGE_TAG}
docker push gcr.io/${PROJECT_ID}/dataflow/deltaflow:latest

# Deploy production template
gcloud dataflow flex-template build \
  gs://${BUCKET_NAME}/templates/deltaflow \
  --image gcr.io/${PROJECT_ID}/dataflow/deltaflow:${IMAGE_TAG} \
  --sdk-language PYTHON \
  --metadata-file metadata.json
```

### Environment Variables File

Create `.env.dev`, `.env.staging`, `.env.prod`:

```bash
# .env.prod
PROJECT_ID=deltaflow-prod
REGION=us-central1
BUCKET_NAME=deltaflow-prod-dataflow-templates
TEMPLATE_NAME=deltaflow
IMAGE_VERSION=1.0.0
MAX_WORKERS=20
WORKER_MACHINE_TYPE=n1-standard-4
SERVICE_ACCOUNT_EMAIL=deltaflow-worker@deltaflow-prod.iam.gserviceaccount.com
NETWORK=projects/deltaflow-prod/global/networks/dataflow-vpc
SUBNETWORK=regions/us-central1/subnetworks/dataflow-subnet
```

Load with:

```bash
source .env.prod
```

## Cloud Scheduler Integration

### Create Automated Schedule

#### Every 2 Hours

```bash
gcloud scheduler jobs create http deltaflow-hourly-sync \
  --location=us-central1 \
  --schedule="0 */2 * * *" \
  --time-zone="UTC" \
  --description="DeltaFlow sync every 2 hours" \
  --uri="https://dataflow.googleapis.com/v1b3/projects/${PROJECT_ID}/locations/us-central1/flexTemplates:launch" \
  --http-method=POST \
  --headers="Content-Type=application/json" \
  --oauth-service-account-email=${SA_EMAIL} \
  --message-body="{
    \"launch_parameter\": {
      \"jobName\": \"deltaflow-scheduled-$(date +%Y%m%d-%H%M%S)\",
      \"containerSpecGcsPath\": \"gs://${BUCKET_NAME}/templates/${TEMPLATE_NAME}\",
      \"environment\": {
        \"serviceAccountEmail\": \"${SA_EMAIL}\",
        \"maxWorkers\": 10,
        \"workerMachineType\": \"n1-standard-2\"
      },
      \"parameters\": {
        \"data_source\": \"postgresql\",
        \"postgresql_host\": \"YOUR_HOST\",
        \"postgresql_database\": \"YOUR_DB\",
        \"postgresql_username\": \"YOUR_USER\",
        \"postgresql_password\": \"YOUR_PASSWORD\",
        \"enable_smart_sync\": \"true\",
        \"postgresql_base_query\": \"SELECT * FROM orders WHERE updated_at > '{start_timestamp}' AND updated_at <= '{end_timestamp}' ORDER BY updated_at ASC\",
        \"destination_bigquery_project\": \"${PROJECT_ID}\",
        \"destination_bigquery_dataset\": \"analytics\",
        \"destination_bigquery_table\": \"orders\"
      }
    }
  }"
```

#### Daily at 2 AM

```bash
gcloud scheduler jobs create http deltaflow-daily-sync \
  --location=us-central1 \
  --schedule="0 2 * * *" \
  --time-zone="America/New_York" \
  --description="DeltaFlow daily sync at 2 AM EST" \
  --uri="https://dataflow.googleapis.com/v1b3/projects/${PROJECT_ID}/locations/us-central1/flexTemplates:launch" \
  --http-method=POST \
  --headers="Content-Type=application/json" \
  --oauth-service-account-email=${SA_EMAIL} \
  --message-body="{...}"  # Same format as above
```

### Manage Schedulers

```bash
# List all schedulers
gcloud scheduler jobs list --location=us-central1

# Pause scheduler
gcloud scheduler jobs pause deltaflow-hourly-sync --location=us-central1

# Resume scheduler
gcloud scheduler jobs resume deltaflow-hourly-sync --location=us-central1

# Test run immediately
gcloud scheduler jobs run deltaflow-hourly-sync --location=us-central1

# Update schedule
gcloud scheduler jobs update http deltaflow-hourly-sync \
  --location=us-central1 \
  --schedule="0 */4 * * *"  # Change to every 4 hours

# Delete scheduler
gcloud scheduler jobs delete deltaflow-hourly-sync \
  --location=us-central1 \
  --quiet
```

## Network Configuration

### VPC Network Setup

#### Create VPC Network

```bash
# Create custom VPC network
gcloud compute networks create dataflow-vpc \
  --subnet-mode=custom \
  --bgp-routing-mode=regional

# Create subnet with private Google access
gcloud compute networks subnets create dataflow-subnet \
  --network=dataflow-vpc \
  --region=us-central1 \
  --range=10.0.0.0/24 \
  --enable-private-ip-google-access

# Verify network creation
gcloud compute networks list
gcloud compute networks subnets list --network=dataflow-vpc
```

#### Configure Firewall Rules

```bash
# Allow internal communication
gcloud compute firewall-rules create dataflow-internal \
  --network=dataflow-vpc \
  --allow=tcp,udp,icmp \
  --source-ranges=10.0.0.0/24

# Allow SSH for debugging (optional)
gcloud compute firewall-rules create dataflow-ssh \
  --network=dataflow-vpc \
  --allow=tcp:22 \
  --source-ranges=0.0.0.0/0

# Verify firewall rules
gcloud compute firewall-rules list --filter="network:dataflow-vpc"
```

#### VPC Peering for Database Access

```bash
# Create VPC peering to database VPC
gcloud compute networks peerings create dataflow-to-db \
  --network=dataflow-vpc \
  --peer-project=database-project \
  --peer-network=database-vpc

# Verify peering
gcloud compute networks peerings list --network=dataflow-vpc
```

### Cloud NAT for Outbound Access

```bash
# Create Cloud Router
gcloud compute routers create dataflow-router \
  --network=dataflow-vpc \
  --region=us-central1

# Create Cloud NAT
gcloud compute routers nats create dataflow-nat \
  --router=dataflow-router \
  --region=us-central1 \
  --nat-all-subnet-ip-ranges \
  --auto-allocate-nat-external-ips

# Verify NAT configuration
gcloud compute routers nats list --router=dataflow-router --region=us-central1
```

## Monitoring and Logging

### View Job Metrics

```bash
# List recent jobs
gcloud dataflow jobs list --region=us-central1 --limit=10

# Get job details with metrics
gcloud dataflow jobs describe $JOB_ID \
  --region=us-central1 \
  --format="table(id,name,currentState,createTime,startTime,currentStateTime)"

# View job graph
gcloud dataflow jobs show $JOB_ID --region=us-central1
```

### Access Logs

```bash
# Stream logs for running job
gcloud logging read "resource.type=dataflow_job AND resource.labels.job_id=$JOB_ID" \
  --limit=50 \
  --format="table(timestamp,severity,textPayload)"

# Filter by severity
gcloud logging read "resource.type=dataflow_job AND resource.labels.job_id=$JOB_ID AND severity>=ERROR" \
  --limit=20

# Export logs to file
gcloud logging read "resource.type=dataflow_job AND resource.labels.job_id=$JOB_ID" \
  --limit=1000 \
  --format=json > job_logs.json
```

### Set Up Alerts

```bash
# Create log-based metric for errors
gcloud logging metrics create deltaflow_errors \
  --description="Count of DeltaFlow errors" \
  --log-filter='resource.type="dataflow_job" AND severity="ERROR"'

# Create alert policy
gcloud alpha monitoring policies create \
  --notification-channels=CHANNEL_ID \
  --display-name="DeltaFlow Error Alert" \
  --condition-display-name="Error rate exceeded" \
  --condition-threshold-value=10 \
  --condition-threshold-duration=300s
```

## Troubleshooting

### Common Issues and Solutions

#### 1. Template Not Found

**Error**: `Template not found: gs://...`

**Solutions**:

```bash
# Verify bucket exists
gsutil ls gs://${BUCKET_NAME}

# Verify template exists
gsutil ls gs://${BUCKET_NAME}/templates/

# Check permissions
gsutil iam get gs://${BUCKET_NAME}

# Redeploy template
gcloud dataflow flex-template build \
  gs://${BUCKET_NAME}/templates/${TEMPLATE_NAME} \
  --image $IMAGE_NAME \
  --sdk-language PYTHON \
  --metadata-file metadata.json
```

#### 2. Docker Push Fails

**Error**: `unauthorized: authentication required`

**Solutions**:

```bash
# Reconfigure Docker authentication
gcloud auth configure-docker

# Login to gcloud
gcloud auth login

# Verify project is set
gcloud config get-value project

# Try push again
docker push $IMAGE_NAME
```

#### 3. Permission Denied Errors

**Error**: `Permission denied on resource project`

**Solutions**:

```bash
# Check current authenticated user
gcloud auth list

# Verify service account has required roles
gcloud projects get-iam-policy $PROJECT_ID \
  --flatten="bindings[].members" \
  --filter="bindings.members:serviceAccount:${SA_EMAIL}"

# Grant missing role (example: Dataflow Worker)
gcloud projects add-iam-policy-binding $PROJECT_ID \
  --member="serviceAccount:${SA_EMAIL}" \
  --role="roles/dataflow.worker"
```

#### 4. Job Fails to Start

**Error**: `Job failed to start`

**Check logs**:

```bash
# View job creation logs
gcloud logging read "resource.type=dataflow_job AND resource.labels.job_id=$JOB_ID" \
  --limit=50 \
  --format="table(timestamp,severity,textPayload)"

# Common causes:
# - Invalid parameter values
# - Network connectivity issues
# - Insufficient quotas
# - Invalid Docker image
```

#### 5. Connection to Database Fails

**Error**: `Connection refused` or `Timeout`

**Solutions**:

```bash
# Check VPC configuration
gcloud compute networks list
gcloud compute firewall-rules list

# Test connectivity from Compute Engine VM in same VPC
gcloud compute ssh test-vm --zone=us-central1-a
nc -zv YOUR_DB_HOST 5432  # PostgreSQL
nc -zv YOUR_DB_HOST 27017 # MongoDB

# Check database allows connections from Dataflow workers
# - Update database firewall rules
# - Verify VPC peering is active
# - Check service account has database access
```

#### 6. BigQuery Write Fails

**Error**: `BigQuery insert failed`

**Solutions**:

```bash
# Check BigQuery dataset exists
bq ls --project_id=$PROJECT_ID

# Check service account has BigQuery permissions
gcloud projects get-iam-policy $PROJECT_ID \
  --flatten="bindings[].members" \
  --filter="bindings.members:serviceAccount:${SA_EMAIL}" \
  --format="table(bindings.role)"

# Grant BigQuery Data Editor role
gcloud projects add-iam-policy-binding $PROJECT_ID \
  --member="serviceAccount:${SA_EMAIL}" \
  --role="roles/bigquery.dataEditor"
```

### Debug Mode

Enable verbose logging:

```bash
# Run job with debug logging
gcloud dataflow flex-template run deltaflow-debug-$(date +%Y%m%d-%H%M%S) \
  --template-file-gcs-location=gs://${BUCKET_NAME}/templates/${TEMPLATE_NAME} \
  --region=us-central1 \
  --additional-experiments=enable_stackdriver_agent_metrics \
  --parameters="..." \
  --verbosity=debug
```

## Best Practices

### 1. Version Control

```bash
# Always use version tags
docker build -t gcr.io/${PROJECT_ID}/dataflow/deltaflow:1.0.0 .
docker tag gcr.io/${PROJECT_ID}/dataflow/deltaflow:1.0.0 \
       gcr.io/${PROJECT_ID}/dataflow/deltaflow:latest

# Keep multiple versions for rollback
docker push gcr.io/${PROJECT_ID}/dataflow/deltaflow:1.0.0
docker push gcr.io/${PROJECT_ID}/dataflow/deltaflow:latest
```

### 2. Resource Optimization

```bash
# Start with minimal resources
--max-workers=2
--worker-machine-type=n1-standard-1

# Scale up based on metrics
--max-workers=10
--worker-machine-type=n1-standard-2

# For large datasets
--max-workers=50
--worker-machine-type=n1-standard-4
--disk-size-gb=100
```

### 3. Cost Management

```bash
# Use preemptible workers (up to 80% cost savings)
--additional-experiments=use_runner_v2,enable_prime \
--num-workers=5 \
--max-workers=20

# Set up budget alerts
gcloud billing budgets create \
  --billing-account=BILLING_ACCOUNT_ID \
  --display-name="Dataflow Budget" \
  --budget-amount=1000 \
  --threshold-rule=percent=90
```

### 4. Security Best Practices

```bash
# Use Secret Manager for credentials
gcloud secrets create db-password --data-file=password.txt

# Reference in job parameters
--parameters="postgresql_password=$(gcloud secrets versions access latest --secret=db-password)"

# Rotate service account keys regularly
gcloud iam service-accounts keys list --iam-account=$SA_EMAIL
gcloud iam service-accounts keys delete KEY_ID --iam-account=$SA_EMAIL

# Use VPC Service Controls
gcloud access-context-manager policies create --title="DeltaFlow Policy"
```

### 5. Testing Strategy

```bash
# Test locally with DirectRunner
python main.py \
  --runner=DirectRunner \
  --data_source=postgresql \
  --postgresql_host=localhost \
  # ... other parameters

# Test in dev environment with small dataset
gcloud dataflow flex-template run test-job \
  --parameters="postgresql_query=SELECT * FROM orders LIMIT 1000,..."

# Gradually scale to production
```

### 6. Monitoring Setup

```bash
# Create dashboard for job metrics
gcloud monitoring dashboards create --config-from-file=dashboard.json

# Set up email notifications
gcloud alpha monitoring channels create \
  --display-name="DeltaFlow Alerts" \
  --type=email \
  --channel-labels=email_address=team@company.com
```

### 7. Disaster Recovery

```bash
# Backup template and configurations
gsutil cp gs://${BUCKET_NAME}/templates/${TEMPLATE_NAME} \
          gs://${BUCKET_NAME}/backups/templates/${TEMPLATE_NAME}-$(date +%Y%m%d)

# Export job configurations
gcloud dataflow jobs describe $JOB_ID --region=us-central1 > job_config_backup.json

# Document recovery procedures
# - Service account keys
# - Network configurations
# - Database credentials
# - Template versions
```

---

**Document Version**: 1.0.0
**Last Updated**: January 2025
**Maintainer**: [kavyasoni](https://github.com/kavyasoni/)

For architecture details, see [ARCHITECTURE.md](ARCHITECTURE.md).
For configuration reference, see [CONFIGURATION.md](CONFIGURATION.md).
