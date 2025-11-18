#!/usr/bin/env python3
"""
DeltaFlow - Run Pipeline Programmatically

This script demonstrates how to run the DeltaFlow data synchronization pipeline
programmatically from Python code, rather than using command-line arguments.

Use cases:
- Integration with existing Python applications
- Custom orchestration logic
- Dynamic parameter generation
- Automated testing frameworks
- CI/CD pipeline integration

Author: DeltaFlow Team
License: Apache 2.0
"""

import sys
import os
import json
from datetime import datetime, timedelta
import logging
from typing import Dict, Any, Optional

# Add parent directory to path to import main.py
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class DeltaFlowPipeline:
    """
    Wrapper class for running DeltaFlow pipelines programmatically.

    This class provides a clean Python API for configuring and executing
    DeltaFlow data synchronization jobs.
    """

    def __init__(
        self,
        runner: str = "DirectRunner",
        project: Optional[str] = None,
        region: str = "us-central1",
        temp_location: Optional[str] = None
    ):
        """
        Initialize DeltaFlow pipeline runner.

        Args:
            runner: Beam runner ('DirectRunner' for local, 'DataflowRunner' for GCP)
            project: GCP project ID (required for DataflowRunner)
            region: GCP region for Dataflow jobs
            temp_location: GCS path for temporary files
        """
        self.runner = runner
        self.project = project
        self.region = region
        self.temp_location = temp_location

    def build_pipeline_options(self, config: Dict[str, Any]) -> PipelineOptions:
        """
        Build Apache Beam PipelineOptions from configuration dictionary.

        Args:
            config: Dictionary with pipeline parameters

        Returns:
            PipelineOptions instance
        """
        # Base options
        options_dict = {
            'runner': self.runner,
        }

        # Add Dataflow-specific options
        if self.runner == 'DataflowRunner':
            if not self.project:
                raise ValueError("project must be specified for DataflowRunner")

            options_dict.update({
                'project': self.project,
                'region': self.region,
                'temp_location': self.temp_location or f'gs://{self.project}-dataflow-temp/temp',
                'staging_location': f'gs://{self.project}-dataflow-temp/staging',
                'job_name': config.get('job_name', f'deltaflow-{datetime.now().strftime("%Y%m%d-%H%M%S")}'),
                'max_num_workers': config.get('max_num_workers', 10),
                'worker_machine_type': config.get('worker_machine_type', 'n1-standard-4'),
                'autoscaling_algorithm': config.get('autoscaling_algorithm', 'THROUGHPUT_BASED'),
            })

        # Add custom pipeline parameters
        options_dict.update(config)

        return PipelineOptions.from_dictionary(options_dict)

    def run_postgresql_sync(
        self,
        postgresql_host: str,
        postgresql_database: str,
        postgresql_username: str,
        postgresql_password: str,
        postgresql_query: str,
        destination_project: str,
        destination_dataset: str,
        destination_table: str,
        enable_smart_sync: bool = False,
        enable_auto_schema: bool = True,
        **kwargs
    ) -> beam.PipelineResult:
        """
        Run PostgreSQL to BigQuery sync.

        Args:
            postgresql_host: PostgreSQL host
            postgresql_database: Database name
            postgresql_username: Username
            postgresql_password: Password
            postgresql_query: SQL query
            destination_project: BigQuery project
            destination_dataset: BigQuery dataset
            destination_table: BigQuery table
            enable_smart_sync: Enable Smart Sync feature
            enable_auto_schema: Enable Auto-Schema detection
            **kwargs: Additional pipeline parameters

        Returns:
            Pipeline execution result
        """
        config = {
            'data_source': 'postgresql',
            'postgresql_host': postgresql_host,
            'postgresql_database': postgresql_database,
            'postgresql_username': postgresql_username,
            'postgresql_password': postgresql_password,
            'postgresql_query': postgresql_query,
            'destination_bigquery_project': destination_project,
            'destination_bigquery_dataset': destination_dataset,
            'destination_bigquery_table': destination_table,
            'enable_smart_sync': str(enable_smart_sync).lower(),
            'enable_auto_schema': str(enable_auto_schema).lower(),
        }
        config.update(kwargs)

        return self._run_pipeline(config)

    def run_mongodb_sync(
        self,
        mongodb_host: str,
        mongodb_database: str,
        mongodb_collection: str,
        destination_project: str,
        destination_dataset: str,
        destination_table: str,
        mongodb_username: Optional[str] = None,
        mongodb_password: Optional[str] = None,
        mongodb_query: str = "{}",
        enable_auto_schema: bool = True,
        **kwargs
    ) -> beam.PipelineResult:
        """
        Run MongoDB to BigQuery sync.

        Args:
            mongodb_host: MongoDB host
            mongodb_database: Database name
            mongodb_collection: Collection name
            destination_project: BigQuery project
            destination_dataset: BigQuery dataset
            destination_table: BigQuery table
            mongodb_username: Optional username
            mongodb_password: Optional password
            mongodb_query: MongoDB query filter (JSON string)
            enable_auto_schema: Enable Auto-Schema detection
            **kwargs: Additional pipeline parameters

        Returns:
            Pipeline execution result
        """
        config = {
            'data_source': 'mongodb',
            'mongodb_host': mongodb_host,
            'mongodb_database': mongodb_database,
            'mongodb_collection': mongodb_collection,
            'mongodb_query': mongodb_query,
            'destination_bigquery_project': destination_project,
            'destination_bigquery_dataset': destination_dataset,
            'destination_bigquery_table': destination_table,
            'enable_auto_schema': str(enable_auto_schema).lower(),
        }

        if mongodb_username:
            config['mongodb_username'] = mongodb_username
        if mongodb_password:
            config['mongodb_password'] = mongodb_password

        config.update(kwargs)

        return self._run_pipeline(config)

    def _run_pipeline(self, config: Dict[str, Any]) -> beam.PipelineResult:
        """
        Internal method to execute the pipeline.

        Args:
            config: Pipeline configuration

        Returns:
            Pipeline execution result
        """
        logger.info(f"Starting DeltaFlow pipeline with runner: {self.runner}")
        logger.info(f"Configuration: {json.dumps({k: v for k, v in config.items() if 'password' not in k.lower()}, indent=2)}")

        # Import and run the main pipeline
        # Note: This imports the run_pipeline function from main.py
        from main import run_pipeline

        # Build options
        options = self.build_pipeline_options(config)

        # Run pipeline
        result = run_pipeline(options)

        logger.info("Pipeline submitted successfully")

        return result


# =============================================================================
# EXAMPLE 1: Simple PostgreSQL Sync (Local Testing)
# =============================================================================

def example_simple_postgresql_sync():
    """
    Example: Run a simple PostgreSQL to BigQuery sync locally.
    """
    print("\n" + "="*80)
    print("EXAMPLE 1: Simple PostgreSQL Sync (DirectRunner)")
    print("="*80 + "\n")

    pipeline = DeltaFlowPipeline(runner="DirectRunner")

    result = pipeline.run_postgresql_sync(
        postgresql_host="localhost",
        postgresql_database="testdb",
        postgresql_username="postgres",
        postgresql_password="postgres",
        postgresql_query="SELECT * FROM users LIMIT 100",
        destination_project="my-gcp-project",
        destination_dataset="test_dataset",
        destination_table="users_sync",
        enable_auto_schema=True,
    )

    # For DirectRunner, wait for completion
    if pipeline.runner == "DirectRunner":
        result.wait_until_finish()
        print("\nPipeline completed successfully!")


# =============================================================================
# EXAMPLE 2: PostgreSQL with Smart Sync (Production)
# =============================================================================

def example_smart_sync_dataflow():
    """
    Example: Run PostgreSQL sync with Smart Sync on Dataflow.
    """
    print("\n" + "="*80)
    print("EXAMPLE 2: Smart Sync on Dataflow")
    print("="*80 + "\n")

    pipeline = DeltaFlowPipeline(
        runner="DataflowRunner",
        project="my-gcp-project",
        region="us-central1"
    )

    result = pipeline.run_postgresql_sync(
        postgresql_host="10.128.0.5",
        postgresql_database="production_db",
        postgresql_username="dataflow_reader",
        postgresql_password="secure_password",  # Use Secret Manager in production!
        postgresql_query="",  # Will be built by Smart Sync
        destination_project="my-gcp-project",
        destination_dataset="analytics",
        destination_table="user_events",
        enable_smart_sync=True,
        enable_auto_schema=True,
        smart_sync_timestamp_column="updated_at",
        postgresql_base_query="""
            SELECT * FROM user_events
            WHERE updated_at > '{start_timestamp}'
            AND updated_at <= '{end_timestamp}'
            ORDER BY updated_at ASC
        """,
        source_table_for_schema="public.user_events",
        partition_field="updated_at",
        clustering_fields="user_id,event_type",
        max_num_workers=5,
        worker_machine_type="n1-standard-2",
    )

    print(f"\nDataflow job submitted!")
    print(f"Job ID: {result.job_id()}")
    print(f"Monitor at: https://console.cloud.google.com/dataflow/jobs/{result.job_id()}")


# =============================================================================
# EXAMPLE 3: MongoDB Sync
# =============================================================================

def example_mongodb_sync():
    """
    Example: Sync MongoDB collection to BigQuery.
    """
    print("\n" + "="*80)
    print("EXAMPLE 3: MongoDB to BigQuery Sync")
    print("="*80 + "\n")

    pipeline = DeltaFlowPipeline(runner="DirectRunner")

    result = pipeline.run_mongodb_sync(
        mongodb_host="localhost",
        mongodb_database="app_db",
        mongodb_collection="events",
        mongodb_username="reader",
        mongodb_password="password",
        mongodb_query='{"status": "active", "created_at": {"$gte": {"$date": "2024-01-01T00:00:00Z"}}}',
        destination_project="my-gcp-project",
        destination_dataset="analytics",
        destination_table="mongodb_events",
        enable_auto_schema=True,
        source_table_for_schema="app_db.events",
        batch_size=2000,
    )

    if pipeline.runner == "DirectRunner":
        result.wait_until_finish()
        print("\nMongoDB sync completed!")


# =============================================================================
# EXAMPLE 4: Dynamic Configuration from Environment
# =============================================================================

def example_dynamic_configuration():
    """
    Example: Build configuration dynamically from environment variables.
    """
    print("\n" + "="*80)
    print("EXAMPLE 4: Dynamic Configuration")
    print("="*80 + "\n")

    # Read from environment variables
    config = {
        'postgresql_host': os.getenv('DB_HOST', 'localhost'),
        'postgresql_database': os.getenv('DB_NAME', 'testdb'),
        'postgresql_username': os.getenv('DB_USER', 'postgres'),
        'postgresql_password': os.getenv('DB_PASSWORD', 'postgres'),
        'destination_project': os.getenv('GCP_PROJECT', 'my-gcp-project'),
        'destination_dataset': os.getenv('BQ_DATASET', 'test'),
        'destination_table': os.getenv('BQ_TABLE', 'test_table'),
    }

    # Build query dynamically
    table_name = os.getenv('SOURCE_TABLE', 'users')
    days_back = int(os.getenv('DAYS_BACK', '7'))
    start_date = (datetime.now() - timedelta(days=days_back)).strftime('%Y-%m-%d')

    config['postgresql_query'] = f"""
        SELECT * FROM {table_name}
        WHERE created_at >= '{start_date}'
        ORDER BY created_at ASC
    """

    print(f"Configuration built from environment:")
    print(json.dumps({k: v for k, v in config.items() if 'password' not in k}, indent=2))

    pipeline = DeltaFlowPipeline(runner="DirectRunner")
    result = pipeline.run_postgresql_sync(**config)

    if pipeline.runner == "DirectRunner":
        result.wait_until_finish()
        print("\nDynamic pipeline completed!")


# =============================================================================
# EXAMPLE 5: Multi-Source Sync (Advanced)
# =============================================================================

def example_multi_source_sync():
    """
    Example: Run multiple pipelines in sequence for different data sources.
    """
    print("\n" + "="*80)
    print("EXAMPLE 5: Multi-Source Sequential Sync")
    print("="*80 + "\n")

    pipeline = DeltaFlowPipeline(runner="DirectRunner")

    # Sync PostgreSQL users
    print("Step 1: Syncing PostgreSQL users...")
    result1 = pipeline.run_postgresql_sync(
        postgresql_host="localhost",
        postgresql_database="app_db",
        postgresql_username="postgres",
        postgresql_password="postgres",
        postgresql_query="SELECT * FROM users",
        destination_project="my-gcp-project",
        destination_dataset="analytics",
        destination_table="users",
    )
    result1.wait_until_finish()
    print("PostgreSQL users synced!\n")

    # Sync MongoDB events
    print("Step 2: Syncing MongoDB events...")
    result2 = pipeline.run_mongodb_sync(
        mongodb_host="localhost",
        mongodb_database="app_db",
        mongodb_collection="events",
        mongodb_query='{}',
        destination_project="my-gcp-project",
        destination_dataset="analytics",
        destination_table="events",
    )
    result2.wait_until_finish()
    print("MongoDB events synced!\n")

    print("All sources synced successfully!")


# =============================================================================
# EXAMPLE 6: Scheduled Sync with Error Handling
# =============================================================================

def example_scheduled_sync_with_retry():
    """
    Example: Run sync with error handling and retry logic.
    Suitable for cron jobs or Cloud Scheduler.
    """
    print("\n" + "="*80)
    print("EXAMPLE 6: Scheduled Sync with Error Handling")
    print("="*80 + "\n")

    max_retries = 3
    retry_delay = 60  # seconds

    for attempt in range(max_retries):
        try:
            print(f"Attempt {attempt + 1} of {max_retries}")

            pipeline = DeltaFlowPipeline(runner="DirectRunner")

            result = pipeline.run_postgresql_sync(
                postgresql_host="localhost",
                postgresql_database="production_db",
                postgresql_username="reader",
                postgresql_password="password",
                postgresql_query="SELECT * FROM transactions WHERE DATE(created_at) = CURRENT_DATE",
                destination_project="my-gcp-project",
                destination_dataset="daily_reports",
                destination_table="transactions_today",
                enable_auto_schema=True,
                write_disposition="WRITE_TRUNCATE",  # Full refresh daily
            )

            result.wait_until_finish()

            print("✓ Sync completed successfully!")
            break

        except Exception as e:
            logger.error(f"Attempt {attempt + 1} failed: {str(e)}")

            if attempt < max_retries - 1:
                print(f"Retrying in {retry_delay} seconds...\n")
                import time
                time.sleep(retry_delay)
            else:
                print("✗ All retry attempts failed!")
                raise


# =============================================================================
# MAIN ENTRY POINT
# =============================================================================

def main():
    """
    Main entry point - run examples based on command-line argument.
    """
    import argparse

    parser = argparse.ArgumentParser(
        description="Run DeltaFlow pipeline examples programmatically"
    )
    parser.add_argument(
        '--example',
        type=int,
        choices=[1, 2, 3, 4, 5, 6],
        help='Example number to run (1-6)'
    )
    parser.add_argument(
        '--all',
        action='store_true',
        help='Run all examples (for DirectRunner only)'
    )

    args = parser.parse_args()

    if args.all:
        print("\n" + "="*80)
        print("RUNNING ALL EXAMPLES")
        print("="*80)

        # Only run DirectRunner examples
        example_simple_postgresql_sync()
        example_mongodb_sync()
        example_dynamic_configuration()
        example_multi_source_sync()
        example_scheduled_sync_with_retry()

        print("\n" + "="*80)
        print("ALL EXAMPLES COMPLETED")
        print("="*80 + "\n")

    elif args.example:
        examples = {
            1: example_simple_postgresql_sync,
            2: example_smart_sync_dataflow,
            3: example_mongodb_sync,
            4: example_dynamic_configuration,
            5: example_multi_source_sync,
            6: example_scheduled_sync_with_retry,
        }
        examples[args.example]()

    else:
        parser.print_help()
        print("\nAvailable examples:")
        print("  1. Simple PostgreSQL Sync (DirectRunner)")
        print("  2. Smart Sync on Dataflow (DataflowRunner)")
        print("  3. MongoDB to BigQuery Sync")
        print("  4. Dynamic Configuration from Environment")
        print("  5. Multi-Source Sequential Sync")
        print("  6. Scheduled Sync with Error Handling")
        print("\nUsage:")
        print("  python run_from_python.py --example 1")
        print("  python run_from_python.py --all")


if __name__ == '__main__':
    main()
