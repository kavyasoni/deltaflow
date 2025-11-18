#!/bin/bash
################################################################################
# DeltaFlow Production Deployment Script
#
# This script automates the complete deployment of DeltaFlow templates to GCP,
# including Docker image building, template creation, and Cloud Scheduler setup.
#
# Features:
# - Docker image build and push to GCR
# - Flex template deployment to GCS
# - Cloud Scheduler job creation
# - IAM permissions setup
# - Validation and health checks
#
# Prerequisites:
# - gcloud CLI authenticated and configured
# - Docker installed and running
# - Appropriate GCP permissions (Project Editor or specific roles)
#
# Author: DeltaFlow Team
# License: Apache 2.0
################################################################################

set -e  # Exit on error
set -u  # Exit on undefined variable

# =============================================================================
# CONFIGURATION
# =============================================================================

# GCP Configuration (MODIFY THESE)
export PROJECT_ID="${PROJECT_ID:-my-gcp-project}"
export REGION="${REGION:-us-central1}"
export ZONE="${ZONE:-us-central1-a}"

# Template Configuration
export TEMPLATE_NAME="${TEMPLATE_NAME:-kk-custom-data-sync-template}"
export TEMPLATE_VERSION="${TEMPLATE_VERSION:-v1.0.0}"
export BUCKET_NAME="${BUCKET_NAME:-${PROJECT_ID}-dataflow-templates}"

# Docker Configuration
export IMAGE_NAME="gcr.io/${PROJECT_ID}/dataflow/custom-data-sync-template:${TEMPLATE_VERSION}"
export DOCKERFILE_PATH="./Dockerfile"

# Service Account Configuration
export SERVICE_ACCOUNT_NAME="${SERVICE_ACCOUNT_NAME:-dataflow-runner}"
export SERVICE_ACCOUNT_EMAIL="${SERVICE_ACCOUNT_NAME}@${PROJECT_ID}.iam.gserviceaccount.com"

# Script directory
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

# =============================================================================
# UTILITY FUNCTIONS
# =============================================================================

log_info() {
    echo -e "${BLUE}[INFO]${NC} $1"
}

log_success() {
    echo -e "${GREEN}[SUCCESS]${NC} $1"
}

log_warning() {
    echo -e "${YELLOW}[WARNING]${NC} $1"
}

log_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

print_header() {
    echo ""
    echo "================================================================================"
    echo "$1"
    echo "================================================================================"
    echo ""
}

check_gcloud() {
    if ! command -v gcloud &> /dev/null; then
        log_error "gcloud CLI not found. Please install Google Cloud SDK."
        exit 1
    fi

    local current_project
    current_project=$(gcloud config get-value project 2>/dev/null)

    if [ "$current_project" != "$PROJECT_ID" ]; then
        log_warning "Current gcloud project: $current_project"
        log_info "Setting project to: $PROJECT_ID"
        gcloud config set project "$PROJECT_ID"
    fi

    log_success "gcloud configured for project: $PROJECT_ID"
}

check_docker() {
    if ! command -v docker &> /dev/null; then
        log_error "Docker not found. Please install Docker."
        exit 1
    fi

    if ! docker info &> /dev/null; then
        log_error "Docker daemon not running. Please start Docker."
        exit 1
    fi

    log_success "Docker is running"
}

# =============================================================================
# STEP 1: Enable Required APIs
# =============================================================================

enable_apis() {
    print_header "STEP 1: Enabling Required GCP APIs"

    local apis=(
        "dataflow.googleapis.com"
        "compute.googleapis.com"
        "storage.googleapis.com"
        "bigquery.googleapis.com"
        "cloudscheduler.googleapis.com"
        "containerregistry.googleapis.com"
        "cloudresourcemanager.googleapis.com"
    )

    for api in "${apis[@]}"; do
        log_info "Enabling $api..."
        gcloud services enable "$api" --project="$PROJECT_ID" 2>/dev/null || true
    done

    log_success "All required APIs enabled"
}

# =============================================================================
# STEP 2: Create GCS Bucket for Templates
# =============================================================================

create_bucket() {
    print_header "STEP 2: Creating GCS Bucket for Templates"

    log_info "Bucket name: gs://$BUCKET_NAME"

    if gsutil ls -b "gs://$BUCKET_NAME" &> /dev/null; then
        log_warning "Bucket already exists: gs://$BUCKET_NAME"
    else
        log_info "Creating bucket..."
        gsutil mb -p "$PROJECT_ID" -l "$REGION" "gs://$BUCKET_NAME"
        log_success "Bucket created: gs://$BUCKET_NAME"
    fi

    # Create required folders
    log_info "Creating folder structure..."
    gsutil -q ls "gs://$BUCKET_NAME/templates/" &> /dev/null || \
        echo "" | gsutil cp - "gs://$BUCKET_NAME/templates/.keep"
    gsutil -q ls "gs://$BUCKET_NAME/temp/" &> /dev/null || \
        echo "" | gsutil cp - "gs://$BUCKET_NAME/temp/.keep"
    gsutil -q ls "gs://$BUCKET_NAME/staging/" &> /dev/null || \
        echo "" | gsutil cp - "gs://$BUCKET_NAME/staging/.keep"

    log_success "Bucket structure ready"
}

# =============================================================================
# STEP 3: Create Service Account
# =============================================================================

create_service_account() {
    print_header "STEP 3: Creating Service Account"

    log_info "Service account: $SERVICE_ACCOUNT_EMAIL"

    # Check if service account exists
    if gcloud iam service-accounts describe "$SERVICE_ACCOUNT_EMAIL" &> /dev/null; then
        log_warning "Service account already exists"
    else
        log_info "Creating service account..."
        gcloud iam service-accounts create "$SERVICE_ACCOUNT_NAME" \
            --display-name="Dataflow Runner Service Account" \
            --project="$PROJECT_ID"
        log_success "Service account created"
    fi

    # Grant required roles
    log_info "Granting IAM roles..."

    local roles=(
        "roles/dataflow.worker"
        "roles/dataflow.admin"
        "roles/bigquery.dataEditor"
        "roles/bigquery.jobUser"
        "roles/storage.objectAdmin"
        "roles/compute.networkUser"
    )

    for role in "${roles[@]}"; do
        log_info "Granting $role..."
        gcloud projects add-iam-policy-binding "$PROJECT_ID" \
            --member="serviceAccount:$SERVICE_ACCOUNT_EMAIL" \
            --role="$role" \
            --quiet 2>/dev/null || true
    done

    log_success "Service account configured with necessary permissions"
}

# =============================================================================
# STEP 4: Build Docker Image
# =============================================================================

build_docker_image() {
    print_header "STEP 4: Building Docker Image"

    log_info "Image name: $IMAGE_NAME"
    log_info "Building from: $PROJECT_ROOT"

    cd "$PROJECT_ROOT"

    # Configure Docker for GCR
    log_info "Configuring Docker for Google Container Registry..."
    gcloud auth configure-docker --quiet

    # Build image
    log_info "Building Docker image (this may take several minutes)..."
    docker build \
        -t "$IMAGE_NAME" \
        -f "$DOCKERFILE_PATH" \
        --build-arg BEAM_VERSION=2.66.0 \
        .

    log_success "Docker image built successfully"

    # Show image details
    docker images "$IMAGE_NAME" --format "table {{.Repository}}\t{{.Tag}}\t{{.Size}}"
}

# =============================================================================
# STEP 5: Push Docker Image to GCR
# =============================================================================

push_docker_image() {
    print_header "STEP 5: Pushing Docker Image to GCR"

    log_info "Pushing to: $IMAGE_NAME"

    docker push "$IMAGE_NAME"

    log_success "Docker image pushed to GCR"
    log_info "Image URL: https://console.cloud.google.com/gcr/images/$PROJECT_ID"
}

# =============================================================================
# STEP 6: Deploy Flex Template
# =============================================================================

deploy_template() {
    print_header "STEP 6: Deploying Flex Template to GCS"

    local template_path="gs://${BUCKET_NAME}/templates/${TEMPLATE_NAME}"

    log_info "Template path: $template_path"
    log_info "Metadata file: ${PROJECT_ROOT}/metadata.json"

    # Validate metadata.json exists
    if [ ! -f "${PROJECT_ROOT}/metadata.json" ]; then
        log_error "metadata.json not found at ${PROJECT_ROOT}/metadata.json"
        exit 1
    fi

    # Deploy template
    gcloud dataflow flex-template build "$template_path" \
        --image "$IMAGE_NAME" \
        --sdk-language PYTHON \
        --metadata-file "${PROJECT_ROOT}/metadata.json" \
        --project "$PROJECT_ID"

    log_success "Flex template deployed successfully"
    log_info "Template location: $template_path"
}

# =============================================================================
# STEP 7: Test Template Deployment
# =============================================================================

test_template() {
    print_header "STEP 7: Testing Template Deployment"

    local template_path="gs://${BUCKET_NAME}/templates/${TEMPLATE_NAME}"
    local test_job_name="deltaflow-test-$(date +%Y%m%d-%H%M%S)"

    log_info "Running test job: $test_job_name"
    log_warning "This will create a small Dataflow job. Make sure test parameters are configured."

    read -p "Run test job? (y/N) " -n 1 -r
    echo
    if [[ ! $REPLY =~ ^[Yy]$ ]]; then
        log_info "Skipping test job"
        return
    fi

    # Example test parameters (MODIFY THESE FOR YOUR ENVIRONMENT)
    gcloud dataflow flex-template run "$test_job_name" \
        --template-file-gcs-location="$template_path" \
        --region="$REGION" \
        --parameters="data_source=postgresql" \
        --parameters="postgresql_host=localhost" \
        --parameters="postgresql_database=testdb" \
        --parameters="postgresql_username=postgres" \
        --parameters="postgresql_password=postgres" \
        --parameters="postgresql_query=SELECT 1 as test_column" \
        --parameters="destination_bigquery_project=${PROJECT_ID}" \
        --parameters="destination_bigquery_dataset=test_dataset" \
        --parameters="destination_bigquery_table=test_table" \
        --service-account-email="$SERVICE_ACCOUNT_EMAIL" \
        --max-workers=2

    log_success "Test job submitted: $test_job_name"
    log_info "Monitor at: https://console.cloud.google.com/dataflow/jobs/$REGION/$test_job_name?project=$PROJECT_ID"
}

# =============================================================================
# STEP 8: Create Cloud Scheduler Jobs
# =============================================================================

create_scheduler_jobs() {
    print_header "STEP 8: Creating Cloud Scheduler Jobs (Optional)"

    log_info "This step creates automated scheduling for DeltaFlow pipelines"

    read -p "Create Cloud Scheduler jobs? (y/N) " -n 1 -r
    echo
    if [[ ! $REPLY =~ ^[Yy]$ ]]; then
        log_info "Skipping Cloud Scheduler setup"
        return
    fi

    # Enable Cloud Scheduler API
    gcloud services enable cloudscheduler.googleapis.com --project="$PROJECT_ID"

    # Create App Engine app (required for Cloud Scheduler in some regions)
    log_info "Checking App Engine app..."
    if ! gcloud app describe --project="$PROJECT_ID" &> /dev/null; then
        log_warning "App Engine app not found. Cloud Scheduler requires it."
        log_info "Creating App Engine app in region: $REGION"
        gcloud app create --region="$REGION" --project="$PROJECT_ID" || true
    fi

    local template_path="gs://${BUCKET_NAME}/templates/${TEMPLATE_NAME}"
    local scheduler_name="deltaflow-hourly-sync"

    log_info "Creating scheduler job: $scheduler_name"

    # Create scheduler job (example: hourly PostgreSQL sync)
    gcloud scheduler jobs create http "$scheduler_name" \
        --location="$REGION" \
        --schedule="0 * * * *" \
        --time-zone="UTC" \
        --uri="https://dataflow.googleapis.com/v1b3/projects/${PROJECT_ID}/locations/${REGION}/flexTemplates:launch" \
        --http-method=POST \
        --oauth-service-account-email="$SERVICE_ACCOUNT_EMAIL" \
        --message-body="{
            \"launchParameter\": {
                \"jobName\": \"deltaflow-scheduled-\$(date +%Y%m%d-%H%M%S)\",
                \"containerSpecGcsPath\": \"${template_path}\",
                \"parameters\": {
                    \"data_source\": \"postgresql\",
                    \"postgresql_host\": \"YOUR_HOST\",
                    \"postgresql_database\": \"YOUR_DB\",
                    \"postgresql_username\": \"YOUR_USER\",
                    \"postgresql_password\": \"YOUR_PASSWORD\",
                    \"postgresql_query\": \"SELECT * FROM your_table\",
                    \"destination_bigquery_project\": \"${PROJECT_ID}\",
                    \"destination_bigquery_dataset\": \"your_dataset\",
                    \"destination_bigquery_table\": \"your_table\",
                    \"enable_smart_sync\": \"true\"
                },
                \"environment\": {
                    \"serviceAccountEmail\": \"${SERVICE_ACCOUNT_EMAIL}\",
                    \"tempLocation\": \"gs://${BUCKET_NAME}/temp\",
                    \"maxWorkers\": 5
                }
            }
        }" \
        --project="$PROJECT_ID" || log_warning "Scheduler job may already exist"

    log_success "Cloud Scheduler configured"
    log_info "View jobs at: https://console.cloud.google.com/cloudscheduler?project=$PROJECT_ID"
}

# =============================================================================
# STEP 9: Deployment Summary
# =============================================================================

print_summary() {
    print_header "DEPLOYMENT SUMMARY"

    cat <<EOF
${GREEN}✓ Deployment completed successfully!${NC}

${BLUE}Project Details:${NC}
  Project ID:        $PROJECT_ID
  Region:            $REGION
  Template Name:     $TEMPLATE_NAME
  Template Version:  $TEMPLATE_VERSION

${BLUE}Resources Created:${NC}
  Docker Image:      $IMAGE_NAME
  Template Location: gs://${BUCKET_NAME}/templates/${TEMPLATE_NAME}
  Bucket:            gs://${BUCKET_NAME}
  Service Account:   $SERVICE_ACCOUNT_EMAIL

${BLUE}Quick Start - Run a Job:${NC}
  gcloud dataflow flex-template run my-sync-job-\$(date +%Y%m%d-%H%M%S) \\
    --template-file-gcs-location=gs://${BUCKET_NAME}/templates/${TEMPLATE_NAME} \\
    --region=${REGION} \\
    --service-account-email=${SERVICE_ACCOUNT_EMAIL} \\
    --parameters="data_source=postgresql" \\
    --parameters="postgresql_host=YOUR_HOST" \\
    --parameters="postgresql_database=YOUR_DB" \\
    --parameters="postgresql_username=YOUR_USER" \\
    --parameters="postgresql_password=YOUR_PASSWORD" \\
    --parameters="postgresql_query=SELECT * FROM your_table" \\
    --parameters="destination_bigquery_project=${PROJECT_ID}" \\
    --parameters="destination_bigquery_dataset=your_dataset" \\
    --parameters="destination_bigquery_table=your_table"

${BLUE}Next Steps:${NC}
  1. Configure your data source credentials (use Secret Manager for production)
  2. Set up Cloud Scheduler for automated syncs
  3. Configure monitoring and alerting
  4. Review security and IAM permissions
  5. Test with production data

${BLUE}Useful Links:${NC}
  Dataflow Console: https://console.cloud.google.com/dataflow?project=${PROJECT_ID}
  GCS Bucket:       https://console.cloud.google.com/storage/browser/${BUCKET_NAME}?project=${PROJECT_ID}
  IAM & Admin:      https://console.cloud.google.com/iam-admin?project=${PROJECT_ID}
  Logs:             https://console.cloud.google.com/logs?project=${PROJECT_ID}

${YELLOW}Documentation:${NC}
  README.md - Full documentation
  CLAUDE.md - Development guidelines
  examples/  - Configuration examples and test scripts

EOF
}

# =============================================================================
# MAIN EXECUTION
# =============================================================================

main() {
    print_header "DeltaFlow Production Deployment"

    log_info "Starting deployment to GCP project: $PROJECT_ID"
    log_info "Region: $REGION"
    log_info "Template: $TEMPLATE_NAME ($TEMPLATE_VERSION)"

    # Confirm deployment
    echo ""
    log_warning "This script will:"
    echo "  1. Enable required GCP APIs"
    echo "  2. Create GCS bucket for templates"
    echo "  3. Create service account with necessary permissions"
    echo "  4. Build Docker image"
    echo "  5. Push image to Google Container Registry"
    echo "  6. Deploy Dataflow Flex template"
    echo "  7. Optionally test the deployment"
    echo "  8. Optionally create Cloud Scheduler jobs"
    echo ""

    read -p "Continue with deployment? (y/N) " -n 1 -r
    echo
    if [[ ! $REPLY =~ ^[Yy]$ ]]; then
        log_info "Deployment cancelled"
        exit 0
    fi

    # Pre-flight checks
    check_gcloud
    check_docker

    # Execute deployment steps
    enable_apis
    create_bucket
    create_service_account
    build_docker_image
    push_docker_image
    deploy_template
    test_template
    create_scheduler_jobs

    # Print summary
    print_summary
}

# Handle command-line arguments
case "${1:-deploy}" in
    deploy)
        main
        ;;
    build-only)
        print_header "Build Docker Image Only"
        check_docker
        build_docker_image
        log_success "Build complete"
        ;;
    template-only)
        print_header "Deploy Template Only (assumes image exists)"
        check_gcloud
        deploy_template
        log_success "Template deployed"
        ;;
    help|--help|-h)
        echo "DeltaFlow Production Deployment Script"
        echo ""
        echo "Usage: $0 [command]"
        echo ""
        echo "Commands:"
        echo "  deploy         - Full deployment (default)"
        echo "  build-only     - Build Docker image only"
        echo "  template-only  - Deploy template only (assumes image exists)"
        echo "  help           - Show this help message"
        echo ""
        echo "Environment Variables:"
        echo "  PROJECT_ID           - GCP project ID (required)"
        echo "  REGION               - GCP region (default: us-central1)"
        echo "  TEMPLATE_NAME        - Template name (default: kk-custom-data-sync-template)"
        echo "  TEMPLATE_VERSION     - Template version (default: v1.0.0)"
        echo "  BUCKET_NAME          - GCS bucket name (default: PROJECT_ID-dataflow-templates)"
        echo "  SERVICE_ACCOUNT_NAME - Service account name (default: dataflow-runner)"
        echo ""
        echo "Example:"
        echo "  PROJECT_ID=my-project REGION=us-east1 $0 deploy"
        ;;
    *)
        log_error "Unknown command: $1"
        echo "Run '$0 help' for usage information"
        exit 1
        ;;
esac
