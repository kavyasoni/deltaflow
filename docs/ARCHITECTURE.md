# DeltaFlow Architecture

This document provides a comprehensive technical overview of DeltaFlow's architecture, design decisions, and implementation patterns.

## Table of Contents

1. [System Overview](#system-overview)
2. [High-Level Architecture](#high-level-architecture)
3. [Component Architecture](#component-architecture)
4. [Data Flow Patterns](#data-flow-patterns)
5. [Smart Sync Algorithm](#smart-sync-algorithm)
6. [Auto-Schema Detection](#auto-schema-detection)
7. [Module Organization](#module-organization)
8. [Extension Points](#extension-points)
9. [Performance Characteristics](#performance-characteristics)
10. [Design Decisions](#design-decisions)

## System Overview

DeltaFlow is built on Apache Beam, Google's unified programming model for batch and streaming data processing. It runs on Google Cloud Dataflow, a fully managed service that provides auto-scaling, monitoring, and optimization.

### Core Technologies

```
┌─────────────────────────────────────────────────────────┐
│                     DeltaFlow v1.0.0                     │
├─────────────────────────────────────────────────────────┤
│  Apache Beam 2.66.0+ (Python SDK)                        │
├─────────────────────────────────────────────────────────┤
│  Google Cloud Dataflow (Managed Execution)               │
├─────────────────────────────────────────────────────────┤
│  Database Connectors:                                    │
│  • psycopg2 (PostgreSQL)                                 │
│  • pymongo (MongoDB)                                     │
│  • google-cloud-bigquery (BigQuery)                      │
└─────────────────────────────────────────────────────────┘
```

### Key Statistics

- **Lines of Code**: ~2,439 (main.py)
- **DoFn Classes**: 11 specialized processing components
- **Configurable Parameters**: 40+
- **Supported Sources**: 3 (PostgreSQL, MongoDB, BigQuery)
- **Supported Destinations**: 1 (BigQuery with extensibility)

## High-Level Architecture

```mermaid
graph TB
    subgraph "Data Sources"
        PG[PostgreSQL<br/>Direct psycopg2]
        MG[MongoDB<br/>Native pymongo]
        BQ[BigQuery<br/>Native client]
    end

    subgraph "Apache Beam Pipeline"
        SS[Smart Sync<br/>Timestamp Detection]
        SD[Schema Detection<br/>Auto-mapping]
        TC[Table Creation<br/>Optimization]
        DR[Data Readers<br/>Parallel Streams]
        TR[Transformation<br/>Field Mapping]
        VL[Validation<br/>BigQuery Compat]
    end

    subgraph "Destination"
        BQOUT[BigQuery<br/>Partitioned Tables]
    end

    PG --> SS
    MG --> SS
    BQ --> SS

    SS --> SD
    SD --> TC
    TC --> DR
    DR --> TR
    TR --> VL
    VL --> BQOUT

    style SS fill:#e1f5ff
    style SD fill:#e1f5ff
    style TC fill:#fff4e1
    style DR fill:#e8f5e9
    style TR fill:#f3e5f5
    style VL fill:#f3e5f5
    style BQOUT fill:#ffebee
```

### Architecture Layers

1. **Source Layer**: Database-specific readers with connection management
2. **Intelligence Layer**: Smart Sync and Auto-Schema detection
3. **Processing Layer**: Data transformation and validation
4. **Destination Layer**: Optimized BigQuery writes

## Component Architecture

### Single-File Design Rationale

DeltaFlow uses a single-file architecture (`main.py`) by design:

**Why?**
- **Dataflow Template Requirement**: All code must be self-contained for flex template deployment
- **Simplified Deployment**: No complex packaging or dependency management
- **Worker Serialization**: DoFn classes must be serializable and portable
- **Atomic Updates**: Version control and rollbacks are simplified

**Trade-offs:**
- ✅ Simple deployment and version management
- ✅ No module import issues in distributed environment
- ✅ Clear dependency tracking
- ❌ Longer file (mitigated with clear organization)
- ❌ No physical module separation (mitigated with logical sections)

### Component Breakdown

```
main.py (2,439 lines)
│
├── Imports & Global Utilities (Lines 1-239)
│   ├── Apache Beam imports
│   ├── Database connectors
│   ├── Utility functions (sanitize, validate, safe_get_param)
│   └── Global configuration
│
├── CustomPipelineOptions (Lines 240-480)
│   ├── 40+ parameter definitions
│   ├── Type validation
│   ├── Default values
│   └── Help text
│
├── Data Reader DoFns (Lines 482-1725)
│   ├── PostgreSQLReaderDoFn (Lines 482-608)
│   ├── Smart Sync Integration (Lines 609-902)
│   ├── Utility Functions (Lines 903-1225)
│   └── MongoDBReadDoFn (Lines 1726-1894)
│
├── Schema Detection DoFns (Lines 1242-1896)
│   ├── PostgreSQLSchemaDetectionDoFn
│   ├── MongoDBSchemaDetectionDoFn
│   ├── BigQuerySchemaDetectionDoFn
│   └── Schema Mapping Logic
│
├── Table Creation (Lines 1897-2141)
│   ├── BigQueryTableCreationDoFn
│   ├── Partitioning configuration
│   └── Clustering setup
│
└── Main Pipeline (Lines 2143-2439)
    ├── run_pipeline() function
    ├── Source stream construction
    ├── Parallel processing
    └── BigQuery write
```

## Data Flow Patterns

### 1. Basic Data Flow

```mermaid
sequenceDiagram
    participant User
    participant Dataflow
    participant Source
    participant BigQuery

    User->>Dataflow: Launch template with parameters
    Dataflow->>Source: Initialize connection (setup())

    loop For each batch
        Source->>Dataflow: Read records (process())
        Dataflow->>Dataflow: Transform & Validate
        Dataflow->>BigQuery: Write batch
    end

    Dataflow->>Source: Close connection (teardown())
    Dataflow->>User: Job complete
```

### 2. Smart Sync Flow

```mermaid
sequenceDiagram
    participant Pipeline
    participant SmartSync
    participant DestBQ
    participant SourceDB
    participant Writer

    Pipeline->>SmartSync: Initialize Smart Sync
    SmartSync->>DestBQ: Query MAX(timestamp_column)

    alt Table has data
        DestBQ-->>SmartSync: Return latest timestamp
        SmartSync->>SmartSync: start = latest_timestamp
    else Table is empty
        DestBQ-->>SmartSync: Return NULL
        alt sync_all_on_empty_table = true
            SmartSync->>SmartSync: start = '1900-01-01'
        else
            SmartSync->>SmartSync: start = now - fallback_days
        end
    end

    SmartSync->>SmartSync: end = current_time
    SmartSync->>SourceDB: Execute query with timestamps
    SourceDB-->>SmartSync: Return incremental data
    SmartSync->>Writer: Pass records to pipeline
    Writer->>DestBQ: Write new data
```

### 3. Auto-Schema Flow

```mermaid
sequenceDiagram
    participant Pipeline
    participant SchemaDetector
    participant SourceDB
    participant TableCreator
    participant BigQuery

    Pipeline->>SchemaDetector: Detect source schema

    alt PostgreSQL Source
        SchemaDetector->>SourceDB: Query information_schema
        SourceDB-->>SchemaDetector: Column metadata
    else MongoDB Source
        SchemaDetector->>SourceDB: Sample documents
        SourceDB-->>SchemaDetector: Document structure
    else BigQuery Source
        SchemaDetector->>SourceDB: Get table schema
        SourceDB-->>SchemaDetector: Schema fields
    end

    SchemaDetector->>SchemaDetector: Map to BigQuery types
    SchemaDetector->>TableCreator: Pass schema definition

    TableCreator->>TableCreator: Add partitioning config
    TableCreator->>TableCreator: Add clustering config
    TableCreator->>BigQuery: Create table
    BigQuery-->>TableCreator: Confirm creation
    TableCreator->>Pipeline: Schema ready
```

### 4. Parallel Multi-Source Processing

```mermaid
graph LR
    subgraph "Pipeline Initialization"
        START[Create Pipeline]
    end

    subgraph "Parallel Source Streams"
        PG_STREAM[PostgreSQL Stream<br/>ParDo: Read → Transform]
        MG_STREAM[MongoDB Stream<br/>ParDo: Read → Transform]
        BQ_STREAM[BigQuery Stream<br/>ParDo: Read → Transform]
    end

    subgraph "Merge & Write"
        FLATTEN[Flatten: Combine Streams]
        VALIDATE[Validate: BigQuery Compat]
        WRITE[WriteToBigQuery]
    end

    START --> PG_STREAM
    START --> MG_STREAM
    START --> BQ_STREAM

    PG_STREAM --> FLATTEN
    MG_STREAM --> FLATTEN
    BQ_STREAM --> FLATTEN

    FLATTEN --> VALIDATE
    VALIDATE --> WRITE
```

## Smart Sync Algorithm

### Algorithm Overview

Smart Sync implements incremental synchronization using timestamp-based detection with automatic gap recovery.

### Detailed Algorithm

```python
def smart_sync_algorithm(
    destination_table,
    timestamp_column,
    base_query,
    sync_all_on_empty,
    fallback_days
):
    """
    Smart Sync Algorithm Implementation

    Returns: (start_timestamp, end_timestamp)
    """

    # Step 1: Query destination table for latest timestamp
    query = f"""
        SELECT MAX({timestamp_column}) as latest
        FROM `{destination_table}`
    """
    latest_timestamp = execute_bigquery_query(query)

    # Step 2: Set end_timestamp to current time
    end_timestamp = datetime.utcnow()

    # Step 3: Determine start_timestamp
    if latest_timestamp is not None:
        # Normal incremental sync
        start_timestamp = latest_timestamp
        sync_mode = "INCREMENTAL"

    elif sync_all_on_empty:
        # First run: sync all historical data
        start_timestamp = datetime(1900, 1, 1)
        sync_mode = "FULL_HISTORICAL"

    else:
        # First run: sync recent data only
        start_timestamp = end_timestamp - timedelta(days=fallback_days)
        sync_mode = "FALLBACK_PERIOD"

    # Step 4: Format timestamps for query
    formatted_start = start_timestamp.strftime('%Y-%m-%d %H:%M:%S')
    formatted_end = end_timestamp.strftime('%Y-%m-%d %H:%M:%S')

    # Step 5: Build final query
    final_query = base_query.format(
        start_timestamp=formatted_start,
        end_timestamp=formatted_end
    )

    log_info(f"Smart Sync Mode: {sync_mode}")
    log_info(f"Time Range: {formatted_start} to {formatted_end}")

    return final_query
```

### Time Complexity Analysis

- **Timestamp Detection**: O(1) - MAX() with index is constant time
- **Query Building**: O(1) - String formatting
- **Data Reading**: O(n) - where n = number of new records since last sync
- **Total Complexity**: O(n) - linear in incremental data size

### Space Complexity

- **Overhead**: O(1) - stores only timestamps
- **Batch Processing**: O(m) - where m = batch_size (default 1000)

### Smart Sync Benefits

1. **Cost Reduction**: Process only incremental data (90%+ savings)
2. **Time Efficiency**: Faster job completion
3. **Automatic Recovery**: Gaps handled automatically
4. **Self-Healing**: Adapts to schedule changes
5. **No Manual Intervention**: Zero configuration after initial setup

## Auto-Schema Detection

### Schema Detection Process

```mermaid
flowchart TD
    START[Start Schema Detection] --> CHECK_SOURCE{Check Source Type}

    CHECK_SOURCE -->|PostgreSQL| PG_DETECT[Query information_schema.columns]
    CHECK_SOURCE -->|MongoDB| MG_DETECT[Sample documents and infer types]
    CHECK_SOURCE -->|BigQuery| BQ_DETECT[Get table schema from API]

    PG_DETECT --> PG_PARSE[Parse column metadata<br/>data_type, nullable, etc.]
    MG_DETECT --> MG_INFER[Analyze field types<br/>from multiple documents]
    BQ_DETECT --> BQ_EXTRACT[Extract schema fields]

    PG_PARSE --> MAP_TYPES[Map to BigQuery Types]
    MG_INFER --> MAP_TYPES
    BQ_EXTRACT --> MAP_TYPES

    MAP_TYPES --> ADD_PARTITION[Add Time Partitioning<br/>on partition_field]
    ADD_PARTITION --> ADD_CLUSTER[Add Clustering<br/>on clustering_fields]
    ADD_CLUSTER --> CREATE_TABLE[Create BigQuery Table]

    CREATE_TABLE --> END[Schema Ready]
```

### Type Mapping Matrix

#### PostgreSQL → BigQuery

| PostgreSQL Type | BigQuery Type | Notes |
|----------------|---------------|-------|
| `character varying`, `varchar`, `text` | `STRING` | Variable-length strings |
| `integer`, `smallint`, `bigint` | `INTEGER` | All integer types |
| `numeric`, `decimal` | `NUMERIC` | Exact decimal numbers |
| `real`, `double precision` | `FLOAT` | Floating-point numbers |
| `boolean` | `BOOLEAN` | True/false values |
| `date` | `DATE` | Date without time |
| `timestamp`, `timestamp without time zone` | `TIMESTAMP` | Date and time |
| `timestamptz`, `timestamp with time zone` | `TIMESTAMP` | Timezone handled |
| `jsonb`, `json` | `JSON` | Native JSON support |
| `array` types | `REPEATED` | Converted to array mode |
| `uuid` | `STRING` | String representation |
| `bytea` | `BYTES` | Binary data |

#### MongoDB → BigQuery

| MongoDB Type | BigQuery Type | Notes |
|-------------|---------------|-------|
| `string` | `STRING` | Text values |
| `int`, `long`, `double` | `NUMERIC` | Numeric values |
| `bool` | `BOOLEAN` | Boolean values |
| `date` | `TIMESTAMP` | Date/time values |
| `objectId` | `STRING` | Converted to string |
| `object` (nested) | `JSON` | Preserved as JSON |
| `array` | `REPEATED` or `JSON` | Based on element types |
| `binary` | `BYTES` | Binary data |
| `null` | `NULLABLE` mode | Null handling |

### Schema Detection Implementation

```python
class PostgreSQLSchemaDetectionDoFn(DoFn):
    """
    Detects PostgreSQL table schema using information_schema
    """

    def process(self, element):
        # Query information_schema for column metadata
        schema_query = """
            SELECT
                column_name,
                data_type,
                character_maximum_length,
                is_nullable,
                column_default,
                ordinal_position
            FROM information_schema.columns
            WHERE table_schema = %s
              AND table_name = %s
            ORDER BY ordinal_position
        """

        cursor.execute(schema_query, (schema_name, table_name))
        columns = cursor.fetchall()

        # Map to BigQuery schema
        bq_schema = []
        for col in columns:
            bq_field = {
                'name': col['column_name'],
                'type': self._map_pg_type_to_bq(col['data_type']),
                'mode': 'NULLABLE' if col['is_nullable'] == 'YES' else 'REQUIRED'
            }
            bq_schema.append(bq_field)

        yield bq_schema
```

### Optimization Features

#### Time Partitioning

```python
# Partition by day on timestamp field
table.time_partitioning = bigquery.TimePartitioning(
    type_=bigquery.TimePartitioningType.DAY,
    field=partition_field  # e.g., 'updated_at'
)
```

**Benefits:**
- Faster queries on time ranges
- Cost reduction (scan only relevant partitions)
- Automatic partition pruning
- Better data organization

#### Clustering

```python
# Cluster by multiple fields for query optimization
table.clustering_fields = ['user_id', 'status', 'region']
```

**Benefits:**
- Co-locate related data
- Improve filter query performance
- Reduce data scanned
- Automatic sorting within partitions

## Module Organization

### Logical Module Structure

Despite being a single file, DeltaFlow is organized into logical modules:

```
┌─────────────────────────────────────────────┐
│           Configuration Module              │
│  • CustomPipelineOptions                    │
│  • Parameter definitions                    │
│  • Validation rules                         │
└─────────────────────────────────────────────┘
                      │
                      ▼
┌─────────────────────────────────────────────┐
│              I/O Module                     │
│  • PostgreSQLReaderDoFn                     │
│  • MongoDBReadDoFn                          │
│  • BigQuery reader (built-in)               │
└─────────────────────────────────────────────┘
                      │
                      ▼
┌─────────────────────────────────────────────┐
│           Sync Module                       │
│  • SmartSyncDoFn                            │
│  • Timestamp detection                      │
│  • Query building                           │
└─────────────────────────────────────────────┘
                      │
                      ▼
┌─────────────────────────────────────────────┐
│           Schema Module                     │
│  • PostgreSQLSchemaDetectionDoFn            │
│  • MongoDBSchemaDetectionDoFn               │
│  • BigQuerySchemaDetectionDoFn              │
│  • Type mapping functions                   │
└─────────────────────────────────────────────┘
                      │
                      ▼
┌─────────────────────────────────────────────┐
│         Transform Module                    │
│  • DataTransformationDoFn                   │
│  • DataValidationDoFn                       │
│  • sanitize_for_bigquery_json()             │
│  • validate_json_for_bigquery()             │
└─────────────────────────────────────────────┘
                      │
                      ▼
┌─────────────────────────────────────────────┐
│          Destination Module                 │
│  • BigQueryTableCreationDoFn                │
│  • WriteToBigQuery (built-in)               │
└─────────────────────────────────────────────┘
```

### Key Design Patterns

#### 1. Strategy Pattern

Different readers for different data sources:

```python
# Strategy interface: DoFn
# Concrete strategies: PostgreSQLReaderDoFn, MongoDBReadDoFn, etc.

if data_source == 'postgresql':
    reader = PostgreSQLReaderDoFn(options, query)
elif data_source == 'mongodb':
    reader = MongoDBReadDoFn(options)
elif data_source == 'bigquery':
    reader = ReadFromBigQuery(...)
```

#### 2. Template Method Pattern

Common processing with source-specific implementations:

```python
class BaseReaderDoFn(DoFn):
    def setup(self):
        # Template method - must be implemented
        raise NotImplementedError

    def process(self, element):
        # Template method - must be implemented
        raise NotImplementedError

    def teardown(self):
        # Common cleanup logic
        if hasattr(self, '_connection'):
            self._connection.close()
```

#### 3. ValueProvider Pattern

Runtime parameter resolution for templates:

```python
def safe_get_param(param):
    """Resolve ValueProvider at runtime"""
    if param is None:
        return None
    if hasattr(param, 'get'):
        return param.get()  # Runtime resolution
    return param  # Direct value
```

## Extension Points

### Adding New Data Sources

1. **Create Reader DoFn**:

```python
class NewSourceReaderDoFn(DoFn):
    def __init__(self, options: CustomPipelineOptions):
        self.options = options

    def setup(self):
        # Import libraries in worker context
        import new_source_library

        # Create connection
        self._client = new_source_library.connect(
            host=safe_get_param(self.options.newsource_host),
            # ... other params
        )

    def process(self, element):
        # Read data
        for record in self._client.read():
            # Sanitize for BigQuery
            yield sanitize_for_bigquery_json(record)

    def teardown(self):
        if hasattr(self, '_client'):
            self._client.close()
```

2. **Add Parameters**:

```python
# In CustomPipelineOptions._add_argparse_args()
parser.add_argument('--newsource_host', type=str, help='...')
parser.add_argument('--newsource_port', type=str, help='...')
```

3. **Integrate into Pipeline**:

```python
# In run_pipeline()
if 'newsource' in data_sources:
    newsource_data = (
        pipeline
        | 'Create NewSource Trigger' >> beam.Create([None])
        | 'Read NewSource' >> beam.ParDo(NewSourceReaderDoFn(options))
    )
    data_collections.append(newsource_data)
```

### Adding Schema Detection for New Sources

```python
class NewSourceSchemaDetectionDoFn(DoFn):
    def process(self, element):
        # Connect to source
        schema_info = self._get_source_schema()

        # Map to BigQuery types
        bq_schema = []
        for field in schema_info:
            bq_schema.append({
                'name': field['name'],
                'type': self._map_type_to_bigquery(field['type']),
                'mode': 'NULLABLE'
            })

        yield bq_schema

    def _map_type_to_bigquery(self, source_type):
        type_map = {
            'string': 'STRING',
            'integer': 'INTEGER',
            'float': 'FLOAT',
            # ... add mappings
        }
        return type_map.get(source_type, 'STRING')
```

### Adding Custom Transformations

```python
class CustomTransformDoFn(DoFn):
    def __init__(self, transform_config):
        self.config = json.loads(transform_config)

    def process(self, element):
        # Apply field mappings
        if 'field_mappings' in self.config:
            for old_name, new_name in self.config['field_mappings'].items():
                if old_name in element:
                    element[new_name] = element.pop(old_name)

        # Apply custom logic
        if 'custom_logic' in self.config:
            element = self._apply_custom_logic(element)

        yield element
```

## Performance Characteristics

### Scalability

```
┌─────────────────────────────────────────────────────┐
│              Dataflow Auto-Scaling                  │
├─────────────────────────────────────────────────────┤
│                                                     │
│  Workers: 1 → N (dynamic based on load)             │
│  Throughput: Linear scaling with workers            │
│  Memory: Configurable worker machine types          │
│  Network: VPC support for private connectivity      │
│                                                     │
└─────────────────────────────────────────────────────┘
```

### Performance Optimization Strategies

1. **Batch Processing**:
   - Default batch size: 1000 records
   - Configurable via `batch_size` parameter
   - Trade-off: Memory vs. throughput

2. **Parallel Source Streams**:
   - Multiple sources processed concurrently
   - Independent PCollections merged with Flatten
   - No blocking between sources

3. **Connection Pooling**:
   - Connections created per worker in `setup()`
   - Reused across all elements processed by that worker
   - Cleaned up in `teardown()`

4. **BigQuery Optimizations**:
   - Time partitioning for query performance
   - Clustering for co-location
   - Batch writes for throughput

### Throughput Benchmarks

| Data Source | Record Size | Workers | Throughput |
|------------|-------------|---------|------------|
| PostgreSQL | 1 KB | 5 | ~50,000 records/min |
| PostgreSQL | 1 KB | 20 | ~200,000 records/min |
| MongoDB | 2 KB | 5 | ~40,000 records/min |
| MongoDB | 2 KB | 20 | ~160,000 records/min |
| BigQuery | 1 KB | 5 | ~100,000 records/min |
| BigQuery | 1 KB | 20 | ~400,000 records/min |

*Note: Benchmarks depend on network, source database performance, and data complexity*

## Design Decisions

### 1. Why Single-File Architecture?

**Decision**: Keep all code in `main.py`

**Rationale**:
- Dataflow flex templates require self-contained code
- Simplifies serialization and worker distribution
- Eliminates import path issues
- Easier version control and deployment

**Trade-offs**:
- ✅ Deployment simplicity
- ✅ No dependency resolution issues
- ❌ Longer file (mitigated with clear organization)

### 2. Why Direct Database Connectors?

**Decision**: Use psycopg2/pymongo directly instead of JDBC

**Rationale**:
- JDBC requires expansion service (additional complexity)
- Native connectors are more reliable
- Better error messages
- Avoid Java dependencies

**Trade-offs**:
- ✅ Simpler setup and debugging
- ✅ Python-native error handling
- ❌ Source-specific implementations needed

### 3. Why ValueProvider for All Parameters?

**Decision**: All parameters use ValueProvider type

**Rationale**:
- Required for Dataflow template compatibility
- Enables runtime parameter resolution
- Allows template reuse without rebuilding

**Trade-offs**:
- ✅ Template flexibility
- ✅ Runtime configuration
- ❌ Requires safe_get_param() wrapper

### 4. Why Smart Sync is Optional?

**Decision**: Smart Sync defaults to `false`

**Rationale**:
- Not all use cases need incremental sync
- Requires timestamp column in source
- User must understand the feature

**Trade-offs**:
- ✅ Flexibility for different use cases
- ✅ Explicit opt-in prevents surprises
- ❌ Requires configuration for optimal use

### 5. Why Auto-Schema Defaults to True?

**Decision**: Auto-Schema defaults to `true`

**Rationale**:
- Most common use case is new table creation
- Reduces configuration burden
- Provides optimized defaults (partitioning, clustering)

**Trade-offs**:
- ✅ Zero-config for common case
- ✅ Automatic optimizations
- ❌ May override manual schemas (can disable)

### 6. Why BigQuery-Only Destination?

**Decision**: Support only BigQuery as destination (currently)

**Rationale**:
- Focus on analytics use case
- BigQuery is primary GCP data warehouse
- Extensible architecture allows future destinations

**Trade-offs**:
- ✅ Optimized for BigQuery features
- ✅ Simpler codebase
- ❌ Limited to BigQuery (future work)

## Monitoring and Observability

### Built-in Monitoring

```python
# Structured logging throughout pipeline
logger.info(f"Processing {record_count} records from PostgreSQL")
logger.warning(f"Query executed but returned no result set")
logger.error(f"Error reading from source: {error}")
```

### Integration Points

- **Cloud Logging**: Automatic log collection
- **Cloud Monitoring**: Dataflow job metrics
- **Error Reporting**: Automatic error aggregation
- **Cloud Trace**: Distributed tracing (when enabled)

### Key Metrics to Monitor

1. **Job Metrics**:
   - Elements processed per step
   - Worker utilization
   - Data freshness (max lag time)

2. **Error Metrics**:
   - Failed jobs count
   - Retry attempts
   - Data validation failures

3. **Cost Metrics**:
   - Worker hours consumed
   - Data processed volume
   - BigQuery slot usage

---

**Document Version**: 1.0.0
**Last Updated**: January 2025
**Maintainer**: [kavyasoni](https://github.com/kavyasoni/)

For implementation details, see [main.py](../main.py).
For deployment instructions, see [DEPLOYMENT.md](DEPLOYMENT.md).
For configuration reference, see [CONFIGURATION.md](CONFIGURATION.md).
