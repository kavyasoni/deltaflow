#!/usr/bin/env python3
"""
Custom Dataflow Pipeline Template for Cross-Platform Data Synchronization

A production-ready Google Cloud Dataflow template for automated data synchronization
across PostgreSQL, MongoDB, and BigQuery with intelligent schema detection.

Key Features:
- Smart Sync: Dynamic timestamp-based synchronization
- Auto-Schema: Automatic table creation with source schema detection
- Multi-Source: PostgreSQL, MongoDB, and BigQuery support
- Cross-Project: Secure VPC network configuration
- Production-Ready: Comprehensive error handling and monitoring

Author: kavyasoni (https://github.com/kavyasoni/)
Version: 1.0.0
License: Apache 2.0
"""

import base64
import json
import logging
import re
from datetime import date, datetime
from typing import Any, Dict, List, Optional

import apache_beam as beam
from apache_beam.io import ReadFromBigQuery, WriteToBigQuery
from apache_beam.io.gcp.bigquery import BigQueryDisposition
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.value_provider import ValueProvider
from apache_beam.transforms import DoFn, ParDo

# Import Google Cloud libraries for schema management
from google.cloud import bigquery
import psycopg2

# Configure logging first
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


_PG_ARRAY_RE = re.compile(r"^\{(.*)\}$")

def safe_get_param(param):
    """
    Safely get parameter value, handling both ValueProvider and direct string cases.
    
    Args:
        param: Parameter that might be a ValueProvider or a direct value
        
    Returns:
        The parameter value as a string or None
    """
    if param is None:
        return None
    if hasattr(param, 'get'):
        return param.get()
    return param


def sanitize_for_bigquery_json(value):
    """
    Sanitize value for BigQuery JSON compatibility.
    
    Args:
        value: Value to sanitize
        
    Returns:
        Sanitized value safe for BigQuery JSON loading
    """
    # None → null
    if value is None:
        return None

    # Preserve objects for JSON/RECORD (sanitize children)
    if isinstance(value, dict):
        return {str(k): sanitize_for_bigquery_json(v) for k, v in value.items()}

    # Preserve arrays for REPEATED/JSON (sanitize elements)
    if isinstance(value, list):
        return [sanitize_for_bigquery_json(v) for v in value]

    # BYTES → base64 (ASCII)
    if isinstance(value, (bytes, bytearray)):
        return base64.b64encode(value).decode('ascii')

    # datetime/date → ISO-8601 (BigQuery will parse)
    if isinstance(value, (datetime, date)):
        try:
            return value.isoformat()
        except Exception:
            return str(value)

    # Clean strings (strip null bytes that break loaders)
    if isinstance(value, str):
        return value.replace('\x00', '')

    # Keep scalar primitives as-is
    if isinstance(value, (int, float, bool)):
        return value

    # Fallback: stringify unknown types
    try:
        return str(value)
    except Exception:
        return None


def _recursive_sanitize(obj):
    """
    Recursively sanitize nested objects for JSON serialization.
    
    Args:
        obj: Object to sanitize
        
    Returns:
        Sanitized object
    """
    if isinstance(obj, dict):
        return {key: _recursive_sanitize(value) for key, value in obj.items()}
    elif isinstance(obj, list):
        return [_recursive_sanitize(item) for item in obj]
    elif isinstance(obj, str):
        # Remove null bytes and properly escape strings
        return obj.replace('\x00', '').replace('\r', '\\r').replace('\n', '\\n')
    elif obj is None:
        return None
    else:
        # Handle ObjectId and other non-serializable types
        try:
            # Try to convert to string first
            return str(obj).replace('\x00', '')
        except:
            return None


def parse_postgres_array_string(s: str):
    """
    Convert a PostgreSQL array literal like:
      {a,b,c} or {"a","b,c","d"} or {}
    into a Python list[str].
    """
    if not isinstance(s, str):
        return s
    m = _PG_ARRAY_RE.match(s.strip())
    if not m:
        return s
    inner = m.group(1)
    if inner == "":
        return []
    items = []
    cur = ""
    in_quotes = False
    i = 0
    while i < len(inner):
        ch = inner[i]
        if ch == '"':
            # handle "" -> escaped quote inside quoted value
            if in_quotes and i + 1 < len(inner) and inner[i + 1] == '"':
                cur += '"'
                i += 1
            else:
                in_quotes = not in_quotes
        elif ch == ',' and not in_quotes:
            items.append(cur)
            cur = ""
        else:
            cur += ch
        i += 1
    items.append(cur)

    out = []
    for it in items:
        it = it.strip()
        if not it or it.upper() == "NULL":
            continue
        # strip surrounding quotes if present
        if len(it) >= 2 and it[0] == '"' and it[-1] == '"':
            it = it[1:-1].replace('""', '"')
        out.append(it)
    return out


def fetch_bq_schema_fields(project: str, dataset: str, table: str):
    """
    Return a dict: field_name -> {"type": "STRING|JSON|...","mode":"NULLABLE|REQUIRED|REPEATED"}
    """
    client = bigquery.Client(project=project)
    table_ref = client.dataset(dataset).table(table)
    tbl = client.get_table(table_ref)
    return {f.name: {"type": f.field_type, "mode": f.mode} for f in tbl.schema}

def validate_json_for_bigquery(record):
    """
    Validate that a record can be safely loaded into BigQuery as JSON.
    
    Args:
        record: Dictionary record to validate
        
    Returns:
        tuple: (is_valid, error_message)
    """
    try:
        import json
        
        # Try to serialize the entire record
        json_str = json.dumps(record, ensure_ascii=False, separators=(',', ':'), default=str)
        
        # Check for common problematic patterns
        if '\x00' in json_str:
            return False, "Record contains null bytes"
        
        # Validate that it can be parsed back
        json.loads(json_str)
        
        # Check record size (BigQuery has limits)
        if len(json_str) > 100 * 1024 * 1024:  # 100MB limit
            return False, "Record exceeds BigQuery size limit"
        
        return True, None
        
    except (TypeError, ValueError, UnicodeError) as e:
        return False, f"JSON serialization error: {str(e)}"
    except Exception as e:
        return False, f"Validation error: {str(e)}"


# Import MongoDB libraries
try:
    import pymongo
    from pymongo import MongoClient
    from bson import ObjectId
    MONGODB_AVAILABLE = True
except ImportError:
    MONGODB_AVAILABLE = False
    logger.warning("pymongo not available. MongoDB functionality will be disabled.")


class CustomPipelineOptions(PipelineOptions):
    """
    Custom pipeline options for the data synchronization template.
    
    All parameters use ValueProvider to enable runtime parameter resolution,
    making this template suitable for deployment as a Dataflow template.
    """
    
    @classmethod
    def _add_argparse_args(cls, parser):
        # PostgreSQL Connection Parameters
        parser.add_argument(
            '--postgresql_host',
            type=str,
            help='PostgreSQL host IP or hostname'
        )
        parser.add_argument(
            '--postgresql_port',
            type=str,
            default='5432',
            help='PostgreSQL port (default: 5432)'
        )
        parser.add_argument(
            '--postgresql_database',
            type=str,
            help='PostgreSQL database name'
        )
        parser.add_argument(
            '--postgresql_username',
            type=str,
            help='PostgreSQL username'
        )
        parser.add_argument(
            '--postgresql_password',
            type=str,
            help='PostgreSQL password'
        )
        parser.add_argument(
            '--postgresql_table',
            type=str,
            help='PostgreSQL table name (optional, can be auto-detected from query or schema)'
        )
        parser.add_argument(
            '--postgresql_query',
            type=str,
            help='SQL query to read data from PostgreSQL'
        )
        
        # MongoDB Connection Parameters
        parser.add_argument(
            '--mongodb_host',
            type=str,
            help='MongoDB host IP or hostname'
        )
        parser.add_argument(
            '--mongodb_port',
            type=str,
            default='27017',
            help='MongoDB port (default: 27017)'
        )
        parser.add_argument(
            '--mongodb_database',
            type=str,
            help='MongoDB database name'
        )
        parser.add_argument(
            '--mongodb_collection',
            type=str,
            help='MongoDB collection name'
        )
        parser.add_argument(
            '--mongodb_username',
            type=str,
            help='MongoDB username (optional)'
        )
        parser.add_argument(
            '--mongodb_password',
            type=str,
            help='MongoDB password (optional)'
        )
        parser.add_argument(
            '--mongodb_connection_string',
            type=str,
            help='MongoDB connection string (overrides individual connection parameters)'
        )
        parser.add_argument(
            '--mongodb_query',
            type=str,
            help='MongoDB query as JSON string (e.g., {"status": "active"})'
        )
        parser.add_argument(
            '--mongodb_projection',
            type=str,
            help='MongoDB projection as JSON string to limit fields (e.g., {"name": 1, "email": 1})'
        )
        
        # BigQuery Source Parameters (for reading)
        parser.add_argument(
            '--source_bigquery_project',
            type=str,
            help='Source BigQuery project ID (can be different from pipeline project)'
        )
        parser.add_argument(
            '--source_bigquery_dataset',
            type=str,
            help='Source BigQuery dataset name'
        )
        parser.add_argument(
            '--source_bigquery_table',
            type=str,
            help='Source BigQuery table name'
        )
        parser.add_argument(
            '--source_bigquery_query',
            type=str,
            help='BigQuery SQL query for reading data (alternative to table)'
        )
        
        # BigQuery Destination Parameters (for writing)
        parser.add_argument(
            '--destination_bigquery_project',
            type=str,
            help='Destination BigQuery project ID'
        )
        parser.add_argument(
            '--destination_bigquery_dataset',
            type=str,
            help='Destination BigQuery dataset name'
        )
        parser.add_argument(
            '--destination_bigquery_table',
            type=str,
            help='Destination BigQuery table name'
        )
        
        # Pipeline Control Parameters
        parser.add_argument(
            '--data_source',
            type=str,
            default='postgresql',
            help='Data source type: postgresql, mongodb, bigquery, or multiple (comma-separated, default: postgresql)'
        )
        parser.add_argument(
            '--transformation_config',
            type=str,
            help='JSON string with transformation configuration'
        )
        parser.add_argument(
            '--write_disposition',
            type=str,
            default='WRITE_APPEND',
            help='BigQuery write disposition: WRITE_APPEND, WRITE_TRUNCATE, or WRITE_EMPTY'
        )
        parser.add_argument(
            '--create_disposition',
            type=str,
            default='CREATE_IF_NEEDED',
            help='BigQuery create disposition: CREATE_IF_NEEDED or CREATE_NEVER'
        )
        
        # Smart Sync Parameters
        parser.add_argument(
            '--enable_smart_sync',
            type=str,
            default='false',
            help='Enable smart sync: query destination BigQuery table for latest timestamp (true/false)'
        )
        parser.add_argument(
            '--smart_sync_timestamp_column',
            type=str,
            default='updated_at',
            help='Column name to use for smart sync timestamp detection (default: updated_at)'
        )
        parser.add_argument(
            '--smart_sync_fallback_hours',
            type=str,
            default='24',
            help='Hours to look back if destination table is empty (default: 24)'
        )
        parser.add_argument(
            '--postgresql_base_query',
            type=str,
            help='Base PostgreSQL query template with {start_timestamp} and {end_timestamp} placeholders for smart sync'
        )
        parser.add_argument(
            '--sync_all_on_empty_table',
            type=str,
            default='true',
            help='Sync all historical data when destination table is empty (true/false, default: true)'
        )
        parser.add_argument(
            '--fallback_days',
            type=str,
            default='1',
            help='Days to look back when sync_all_on_empty_table=false (default: 1 day, equivalent to 24 hours)'
        )
        
        # Advanced Configuration
        parser.add_argument(
            '--batch_size',
            type=int,
            default=1000,
            help='Batch size for data processing (default: 1000)'
        )
        parser.add_argument(
            '--max_retries',
            type=int,
            default=3,
            help='Maximum number of retries for failed operations (default: 3)'
        )
        
        # Schema Detection and Table Creation
        parser.add_argument(
            '--enable_auto_schema',
            type=str,
            default='true',
            help='Enable automatic schema detection and BigQuery table creation (true/false, default: true)'
        )
        parser.add_argument(
            '--source_table_for_schema',
            type=str,
            help='Source table name for schema detection (PostgreSQL: schema.table, BigQuery: project.dataset.table)'
        )
        parser.add_argument(
            '--partition_field',
            type=str,
            default='updated_at',
            help='Field to use for BigQuery table partitioning (default: updated_at)'
        )
        parser.add_argument(
            '--clustering_fields',
            type=str,
            help='Comma-separated list of fields for BigQuery table clustering (e.g., id,status)'
        )
        parser.add_argument(
            '--delete_existing_table',
            type=str,
            default='false',
            help='Delete existing BigQuery table before creating new one (true/false, default: false)'
        )


class PostgreSQLReaderDoFn(DoFn):
    """
    DoFn for reading data directly from PostgreSQL using psycopg2.
    
    This DoFn provides a more reliable alternative to ReadFromJdbc
    by using psycopg2 directly, avoiding expansion service issues.
    """
    
    def __init__(self, options: CustomPipelineOptions, query: str):
        """
        Initialize PostgreSQL reader.
        
        Args:
            options: Pipeline options containing connection parameters
            query: SQL query to execute
        """
        if not query or query.strip() == '':
            raise ValueError("PostgreSQL query cannot be empty")
        
        self.options = options
        self.query = query
        self._connection = None
    
    def setup(self):
        """Setup PostgreSQL connection."""
        import logging
        import psycopg2
        logger = logging.getLogger(__name__)
        
        # Define safe_get_param locally for worker context
        def safe_get_param(param):
            if param is None:
                return None
            if hasattr(param, 'get'):
                return param.get()
            return param
        
        try:
            # Get connection parameters
            host = safe_get_param(self.options.postgresql_host)
            port = safe_get_param(self.options.postgresql_port)
            database = safe_get_param(self.options.postgresql_database)
            username = safe_get_param(self.options.postgresql_username)
            password = safe_get_param(self.options.postgresql_password)
            
            # Create connection
            self._connection = psycopg2.connect(
                host=host,
                port=port,
                database=database,
                user=username,
                password=password
            )
            
            logger.info(f"Connected to PostgreSQL: {host}:{port}/{database}")
            
        except Exception as e:
            logger.error(f"Failed to connect to PostgreSQL: {e}")
            raise
    
    def process(self, element):
        """
        Execute PostgreSQL query and yield results.
        
        Args:
            element: Input element (not used, can be None)
            
        Yields:
            Database records as dictionaries
        """
        import logging
        logger = logging.getLogger(__name__)
        
        try:
            cursor = self._connection.cursor()
            
            # Validate query before execution
            if not self.query or self.query.strip() == '':
                logger.error("PostgreSQL query is empty or None")
                raise ValueError("PostgreSQL query cannot be empty")
            
            logger.info(f"Executing PostgreSQL query: {self.query[:100]}...")
            logger.info(f"Full query length: {len(self.query)} characters")
            cursor.execute(self.query)
            
            # Check if query returned results
            if cursor.description is None:
                logger.warning("Query executed but returned no result set (possibly a non-SELECT query)")
                cursor.close()
                return
            
            # Get column names
            column_names = [desc[0] for desc in cursor.description]
            logger.info(f"Query returned {len(column_names)} columns: {column_names}")
            
            # Process results
            row_count = 0
            for row in cursor.fetchall():
                # Convert row to dictionary
                record = dict(zip(column_names, row))
                
                # Convert data types for BigQuery compatibility
                for key, value in record.items():
                    if hasattr(value, 'isoformat'):  # datetime objects
                        # Use BigQuery-compatible timestamp format
                        record[key] = value.isoformat()
                    else:
                        # Use the new sanitization function for all other values
                        record[key] = sanitize_for_bigquery_json(value)
                
                # Keep only source data - no metadata fields to avoid BigQuery JSON schema issues
                
                yield record
                row_count += 1
            
            logger.info(f"Successfully processed {row_count} records from PostgreSQL")
            cursor.close()
            
        except Exception as e:
            logger.error(f"Error reading from PostgreSQL: {e}")
            raise
    
    def teardown(self):
        """Close PostgreSQL connection."""
        if self._connection:
            self._connection.close()


class DataTransformationDoFn(DoFn):
    """
    Custom DoFn for applying data transformations.
    
    This DoFn can apply various transformations based on configuration:
    - Field mapping and renaming
    - Data type conversions
    - Value transformations
    - Field calculations
    - Data validation and cleaning
    """
    
    def __init__(self, transformation_config: ValueProvider):
        """
        Initialize transformation function.
        
        Args:
            transformation_config: ValueProvider containing JSON transformation config
        """
        self.transformation_config = transformation_config
        self._parsed_config = None
    
    def setup(self):
        """Setup transformation configuration."""
        import logging
        logger = logging.getLogger(__name__)
        
        try:
            config_value = safe_get_param(self.transformation_config)
            if config_value:
                self._parsed_config = json.loads(config_value)
                logger.info(f"Loaded transformation config: {self._parsed_config}")
            else:
                self._parsed_config = {}
                logger.info("No transformation config provided, using passthrough")
        except Exception as e:
            logger.error(f"Error parsing transformation config: {e}")
            self._parsed_config = {}
    
    def process(self, element):
        """
        Process each data element with configured transformations.
        
        Args:
            element: Input data element (dict)
            
        Yields:
            Transformed data element
        """
        try:
            if not self._parsed_config:
                # No transformations - pass through
                yield element
                return
            
            transformed_element = dict(element)
            
            # Apply field mappings
            if 'field_mappings' in self._parsed_config:
                for old_field, new_field in self._parsed_config['field_mappings'].items():
                    if old_field in transformed_element:
                        transformed_element[new_field] = transformed_element.pop(old_field)
            
            # Apply data type conversions
            if 'type_conversions' in self._parsed_config:
                for field, target_type in self._parsed_config['type_conversions'].items():
                    if field in transformed_element and transformed_element[field] is not None:
                        transformed_element[field] = self._convert_type(
                            transformed_element[field], target_type
                        )
            
            # Apply value transformations
            if 'value_transformations' in self._parsed_config:
                for field, transform in self._parsed_config['value_transformations'].items():
                    if field in transformed_element:
                        transformed_element[field] = self._apply_value_transform(
                            transformed_element[field], transform
                        )
            
            # Apply calculated fields
            if 'calculated_fields' in self._parsed_config:
                for field, expression in self._parsed_config['calculated_fields'].items():
                    transformed_element[field] = self._calculate_field(
                        transformed_element, expression
                    )
            
            # Keep only source data - no metadata to avoid BigQuery schema issues
            
            yield transformed_element
            
        except Exception as e:
            import logging
            logger = logging.getLogger(__name__)
            logger.error(f"Error transforming element {element}: {e}")
            # Yield original element with error flag
            element['_transformation_error'] = str(e)
            yield element
    
    def _convert_type(self, value, target_type: str):
        """Convert value to target type."""
        if target_type == 'string':
            return str(value)
        elif target_type == 'integer':
            return int(float(value))
        elif target_type == 'float':
            return float(value)
        elif target_type == 'boolean':
            return bool(value)
        elif target_type == 'timestamp':
            if isinstance(value, str):
                return datetime.fromisoformat(value.replace('Z', '+00:00')).isoformat()
            return value
        else:
            return value
    
    def _apply_value_transform(self, value, transform: Dict):
        """Apply value transformation based on transform config."""
        if transform['type'] == 'regex_replace':
            return re.sub(transform['pattern'], transform['replacement'], str(value))
        elif transform['type'] == 'upper':
            return str(value).upper()
        elif transform['type'] == 'lower':
            return str(value).lower()
        elif transform['type'] == 'trim':
            return str(value).strip()
        else:
            return value
    
    def _calculate_field(self, element: Dict, expression: str) -> Any:
        """Calculate field value based on expression."""
        try:
            # Simple expression evaluation (extend as needed)
            # Example: "field1 + field2" or "len(field1)"
            if '+' in expression:
                fields = [f.strip() for f in expression.split('+')]
                return sum(element.get(f, 0) for f in fields)
            elif 'len(' in expression:
                field = expression.replace('len(', '').replace(')', '')
                return len(str(element.get(field, '')))
            else:
                return element.get(expression, None)
        except Exception as e:
            import logging
            logger = logging.getLogger(__name__)
            logger.error(f"Error calculating field with expression '{expression}': {e}")
            return None


class DataValidationDoFn(DoFn):
    """
    DoFn for data validation and quality checks.
    
    Validates data quality and adds validation metadata to records.
    """
    def __init__(self, options: CustomPipelineOptions):
        self.options = options
        self._schema = {}
        self._json_fields = set()
        self._repeated_fields = set()

    def setup(self):
        project = safe_get_param(self.options.destination_bigquery_project)
        dataset = safe_get_param(self.options.destination_bigquery_dataset)
        table   = safe_get_param(self.options.destination_bigquery_table)
        try:
            schema = fetch_bq_schema_fields(project, dataset, table)
            self._schema = schema or {}
            for name, spec in self._schema.items():
                if spec["type"] == "JSON":
                    self._json_fields.add(name)
                if spec["mode"] == "REPEATED":
                    self._repeated_fields.add(name)
            logger.info(f"Loaded BQ schema for coercion: JSON={self._json_fields}, REPEATED={self._repeated_fields}")
        except Exception as e:
            logger.warning(f"Could not fetch destination schema; proceeding without it: {e}")
            self._schema = {}

    def process(self, element):
        """
        Validates a single input element.

        Checks required fields, data types, and basic rules.
        Returns the element if valid, or raises an error if invalid.
        """
        import json

        def SAFE_PARSE(s: str):
            try:
                return json.loads(s)
            except Exception:
                return None

        def _looks_like_json_text(x: any) -> bool:
            return isinstance(x, str) and x.strip().startswith(('{', '['))

        try:
            sanitized_element = {}

            for key, value in element.items():
                clean_key = str(key)
                v = value

                # Track if the source value started as a string (used for unknown schema fallback)
                source_was_string = isinstance(v, str)

                # --- Parsing phase ---
                if isinstance(v, str):
                    s = v.strip()

                    # 1) PostgreSQL array literal first -> list[str]
                    if s.startswith('{') and s.endswith('}') and ':' not in s:
                        try:
                            v = parse_postgres_array_string(s)
                        except Exception:
                            # fall back to original string
                            v = s
                    else:
                        # 2) JSON-looking payload -> keep structure if valid
                        if (s.startswith('[') and s.endswith(']')) or (s.startswith('{') and s.endswith('}')):
                            try:
                                v = json.loads(s)
                            except Exception:
                                # leave as original string if not valid JSON
                                pass

                # --- Schema-aware coercion ---
                spec = self._schema.get(clean_key)

                if spec:
                    is_repeated = (spec.get("mode") == "REPEATED")
                    ftype = spec.get("type")

                    # Ensure list shape for repeated fields
                    if is_repeated and v is not None and not isinstance(v, list):
                        v = [v]

                    if ftype == "RECORD":
                        # keep dicts/(list of dicts) as-is
                        pass

                    elif ftype == "JSON":
                        # For BigQuery JSON type: pass **native JSON values**, not strings.
                        def to_json_value(x):
                            # If it's JSON-looking text, parse once
                            if isinstance(x, str) and _looks_like_json_text(x):
                                parsed = SAFE_PARSE(x)  # helper below
                                return parsed if parsed is not None else x  # fall back to original
                            # dict/list/number/bool/null are already valid JSON values
                            return x

                        if is_repeated:
                            if v is None:
                                v = []
                            v = [to_json_value(item) for item in v]
                        else:
                            v = to_json_value(v)

                    else:
                        # Non-JSON, non-RECORD types
                        if is_repeated:
                            if isinstance(v, list):
                                v = [
                                    (json.dumps(it, ensure_ascii=False, separators=(',', ':'))
                                    if isinstance(it, (dict, list)) else it)
                                    for it in v
                                ]
                        else:
                            if isinstance(v, (dict, list)):
                                v = json.dumps(v, ensure_ascii=False, separators=(',', ':'))
                else:
                    # Unknown schema: if source was a string and we parsed JSON to dict/list,
                    # serialize back to JSON string to avoid accidental RECORDs.
                    if source_was_string and isinstance(v, (dict, list)):
                        v = json.dumps(v, ensure_ascii=False, separators=(',', ':'))

                # Final sanitize for BigQuery JSON compatibility (bytes, dates, null bytes, etc.)
                sanitized_element[clean_key] = sanitize_for_bigquery_json(v)

            # Final validation to catch serialization/size issues early
            is_valid, error_msg = validate_json_for_bigquery(sanitized_element)
            if not is_valid:
                logger.error(f"Record failed BigQuery JSON validation: {error_msg}")
                logger.error(f"Problem record: {sanitized_element}")
                return

            yield sanitized_element

        except Exception as e:
            logger = logging.getLogger(__name__)
            logger.error(f"Error processing element {element}: {e}")
            return


class SmartSyncDoFn(DoFn):
    """
    DoFn for implementing smart sync functionality.
    
    This DoFn queries the BigQuery destination table to find the latest
    timestamp and constructs PostgreSQL queries dynamically to sync only
    new data since that timestamp.
    
    Benefits:
    - No fixed time intervals
    - Eliminates duplicate data processing  
    - Automatically handles gaps of any size
    - Self-healing pipeline behavior
    """
    
    def __init__(self, options: CustomPipelineOptions):
        """
        Initialize smart sync functionality.
        
        Args:
            options: Pipeline options containing configuration
        """
        self.options = options
        self._latest_timestamp = None
        self._sync_query = None
    
    def setup(self):
        """Setup smart sync by querying BigQuery for latest timestamp."""
        import logging
        logger = logging.getLogger(__name__)
        
        # Define safe_get_param locally for worker context
        def safe_get_param(param):
            if param is None:
                return None
            if hasattr(param, 'get'):
                return param.get()
            return param
        
        try:
            enable_smart_sync = safe_get_param(self.options.enable_smart_sync) or 'false'
            
            if enable_smart_sync.lower() == 'true':
                logger.info("Smart sync enabled - querying BigQuery for latest timestamp")
                self._latest_timestamp = self._get_latest_timestamp_from_bigquery()
                self._sync_query = self._build_smart_sync_query()
            else:
                logger.info("Smart sync disabled - using provided postgresql_query")
                self._sync_query = safe_get_param(self.options.postgresql_query)
        except Exception as e:
            logger.error(f"Error setting up smart sync: {e}")
            # Fallback to regular query if smart sync fails
            self._latest_timestamp = None
            fallback_query = safe_get_param(self.options.postgresql_query)
            self._sync_query = fallback_query
    
    def _get_latest_timestamp_from_bigquery(self) -> Optional[str]:
        """
        Query BigQuery destination table for the latest timestamp.
        
        Returns:
            Latest timestamp as string, or None if table is empty
        """
        import subprocess
        import json
        
        try:
            import logging
            logger = logging.getLogger(__name__)
            
            # Define safe_get_param locally for worker context
            def safe_get_param(param):
                if param is None:
                    return None
                if hasattr(param, 'get'):
                    return param.get()
                return param
            
            # Build BigQuery table specification
            project = safe_get_param(self.options.destination_bigquery_project)
            dataset = safe_get_param(self.options.destination_bigquery_dataset)
            table = safe_get_param(self.options.destination_bigquery_table)
            timestamp_column = safe_get_param(self.options.smart_sync_timestamp_column)
            
            # Query for maximum timestamp
            query = f"""
            SELECT MAX({timestamp_column}) as latest_timestamp
            FROM `{project}.{dataset}.{table}`
            """
            
            logger.info(f"Querying BigQuery for latest timestamp: {query}")
            
            # Execute BigQuery query using bq command line tool
            cmd = [
                "bq", "query",
                "--use_legacy_sql=false",
                "--format=json",
                "--max_rows=1",
                query
            ]
            
            result = subprocess.run(cmd, capture_output=True, text=True, check=True)
            query_result = json.loads(result.stdout)
            
            if query_result and len(query_result) > 0:
                latest_timestamp_str = query_result[0].get("latest_timestamp")
                
                if latest_timestamp_str:
                    logger.info(f"Found latest timestamp in BigQuery: {latest_timestamp_str}")
                    return latest_timestamp_str
                else:
                    logger.warning(f"Destination table {project}.{dataset}.{table} is empty")
                    return None
            else:
                logger.warning("No results from BigQuery query")
                return None
                
        except subprocess.CalledProcessError as e:
            logger.error(f"BigQuery query failed: {e}")
            logger.error(f"Error output: {e.stderr}")
            # Check if table doesn't exist
            if "Not found" in str(e.stderr) or "does not exist" in str(e.stderr):
                logger.info(f"Destination table {project}.{dataset}.{table} does not exist - normal for first run")
                return None
            logger.warning("BigQuery query failed for other reasons, treating as empty table")
            return None
        except Exception as e:
            logger.error(f"Error querying BigQuery for latest timestamp: {e}")
            return None
    
    def _build_smart_sync_query(self) -> str:
        """
        Build PostgreSQL query based on latest BigQuery timestamp.
        
        Returns:
            Dynamic PostgreSQL query string
        """
        from datetime import datetime, timedelta
        import logging
        logger = logging.getLogger(__name__)
        
        try:
            # Define safe_get_param locally for worker context
            def safe_get_param(param):
                if param is None:
                    return None
                if hasattr(param, 'get'):
                    return param.get()
                return param
                
            base_query = safe_get_param(self.options.postgresql_base_query)
            
            if not base_query:
                # Fallback to regular query if no base query provided
                import logging
                logger = logging.getLogger(__name__)
                logger.warning("No postgresql_base_query provided for smart sync. Using postgresql_query.")
                return safe_get_param(self.options.postgresql_query)
            
            # Determine start timestamp
            if self._latest_timestamp:
                # Use latest timestamp from BigQuery
                start_timestamp = self._latest_timestamp
                
                # End timestamp is current time
                end_timestamp = datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')
                
                # Build dynamic query by replacing placeholders
                smart_query = base_query.format(
                    start_timestamp=start_timestamp,
                    end_timestamp=end_timestamp
                )
                
            else:
                # BigQuery table is empty - check sync_all_on_empty_table setting
                sync_all_setting = (safe_get_param(self.options.sync_all_on_empty_table) or 'true').lower()
                
                if sync_all_setting == 'true':
                    # Fresh deployment: sync ALL historical data
                    logger.info("Empty destination table detected - syncing all historical data")
                    
                    # Remove time constraints to sync all data
                    # Create a query without start/end timestamp filters
                    base_query_without_placeholders = base_query.replace("WHERE updated_at > '{start_timestamp}' AND updated_at <= '{end_timestamp}'", "")
                    base_query_without_placeholders = base_query_without_placeholders.replace("WHERE updated_at > '{start_timestamp}' AND updated_at <= '{end_timestamp}' ORDER BY updated_at ASC", "ORDER BY updated_at ASC")
                    
                    # If the base query doesn't have the expected WHERE clause, fall back to building a simple query
                    if '{start_timestamp}' in base_query_without_placeholders or '{end_timestamp}' in base_query_without_placeholders:
                        # Base query still has placeholders, so we need to provide values
                        # Use a very old start date to capture all historical data
                        start_timestamp = '1900-01-01 00:00:00'  # Very old date to capture all data
                        end_timestamp = datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')
                        
                        smart_query = base_query.format(
                            start_timestamp=start_timestamp,
                            end_timestamp=end_timestamp
                        )
                    else:
                        smart_query = base_query_without_placeholders
                else:
                    # Use fallback logic with configurable time window (days)
                    fallback_days = int(safe_get_param(self.options.fallback_days) or '1')
                    fallback_time = datetime.utcnow() - timedelta(days=fallback_days)
                    start_timestamp = fallback_time.strftime('%Y-%m-%d %H:%M:%S')
                    end_timestamp = datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')
                    
                    smart_query = base_query.format(
                        start_timestamp=start_timestamp,
                        end_timestamp=end_timestamp
                    )
            
            return smart_query
            
        except Exception as e:
            logger.error(f"Error building smart sync query: {e}")
            # Fallback to regular query
            def safe_get_param(param):
                if param is None:
                    return None
                if hasattr(param, 'get'):
                    return param.get()
                return param
            
            fallback_query = safe_get_param(self.options.postgresql_query)
            
            if not fallback_query or not fallback_query.strip():
                base_query = safe_get_param(self.options.postgresql_base_query)
                if base_query and base_query.strip():
                    return base_query
                else:
                    raise ValueError("No valid PostgreSQL query available after Smart Sync failure")
            
            return fallback_query
    
    def get_sync_query(self) -> str:
        """
        Get the constructed sync query.
        
        Returns:
            PostgreSQL query string for data synchronization
        """
        if self._sync_query and self._sync_query.strip():
            return self._sync_query
        else:
            # Fallback to regular query
            fallback_query = safe_get_param(self.options.postgresql_query)
            
            if not fallback_query or not fallback_query.strip():
                # Try base query as final resort
                base_query = safe_get_param(self.options.postgresql_base_query)
                if base_query and base_query.strip():
                    return base_query
                else:
                    raise ValueError("No valid query available in SmartSyncDoFn.get_sync_query()")
            
            return fallback_query


def build_smart_postgresql_query(options: CustomPipelineOptions) -> str:
    """
    Build PostgreSQL query using smart sync logic.
    
    This function implements the smart sync functionality by:
    1. Querying BigQuery destination table for latest timestamp
    2. Using that timestamp as the start point for PostgreSQL query
    3. Syncing only new data since that timestamp
    
    Args:
        options: Pipeline options containing configuration
        
    Returns:
        PostgreSQL query string for smart synchronization
    """
    enable_smart_sync = safe_get_param(options.enable_smart_sync) or 'false'
    
    if enable_smart_sync.lower() != 'true':
        # Smart sync disabled, use regular query
        regular_query = safe_get_param(options.postgresql_query) or ''
        if not regular_query.strip():
            raise ValueError("PostgreSQL query cannot be empty")
        return regular_query
    
    try:
        # Create SmartSyncDoFn to handle the logic
        smart_sync = SmartSyncDoFn(options)
        smart_sync.setup()
        sync_query = smart_sync.get_sync_query()
        
        if not sync_query or sync_query.strip() == '':
            fallback_query = safe_get_param(options.postgresql_query) or ''
            
            if not fallback_query.strip():
                # Last resort: check if we have base query
                base_query = safe_get_param(options.postgresql_base_query) or ''
                if base_query.strip():
                    # Create a simple query from base query without timestamp filtering
                    simple_query = base_query.replace("WHERE updated_at > '{start_timestamp}' AND updated_at <= '{end_timestamp}'", "")
                    simple_query = simple_query.replace("WHERE updated_at > '{start_timestamp}' AND updated_at <= '{end_timestamp}' ORDER BY updated_at ASC", "ORDER BY updated_at ASC")
                    return simple_query.strip()
                else:
                    raise ValueError("No valid PostgreSQL query available - all query sources are empty")
            return fallback_query
        
        return sync_query
        
    except Exception as e:
        logger.error(f"Error in smart sync, falling back to regular query: {e}")
        
        fallback_query = safe_get_param(options.postgresql_query) or ''
        
        if not fallback_query.strip():
            # Try base query as last resort
            base_query = safe_get_param(options.postgresql_base_query) or ''
            if base_query.strip():
                # Remove timestamp placeholders for emergency fallback
                emergency_query = base_query.replace("WHERE updated_at > '{start_timestamp}' AND updated_at <= '{end_timestamp}'", "")
                emergency_query = emergency_query.replace("WHERE updated_at > '{start_timestamp}' AND updated_at <= '{end_timestamp}' ORDER BY updated_at ASC", "ORDER BY updated_at ASC")
                return emergency_query.strip()
            else:
                raise ValueError("No valid PostgreSQL query available - all query sources are empty")
        return fallback_query



def create_bigquery_table_spec(project: str, dataset: str, table: str) -> str:
    """
    Create BigQuery table specification string.
    
    Args:
        project: GCP project ID
        dataset: BigQuery dataset name
        table: BigQuery table name
        
    Returns:
        BigQuery table specification
    """
    return f"{project}.{dataset}.{table}"


class PostgreSQLSchemaDetectionDoFn(DoFn):
    """
    DoFn for detecting PostgreSQL table schema.
    
    This DoFn connects to PostgreSQL and extracts table schema information
    including column names, data types, and constraints.
    """
    
    def __init__(self, options: CustomPipelineOptions):
        """
        Initialize PostgreSQL schema detection.
        
        Args:
            options: Pipeline options containing connection parameters
        """
        self.options = options
        self._schema_info = None
    
    def setup(self):
        """Setup and detect PostgreSQL table schema."""
        import logging
        logger = logging.getLogger(__name__)
        
        # Define safe_get_param locally for worker context
        def safe_get_param(param):
            if param is None:
                return None
            if hasattr(param, 'get'):
                return param.get()
            return param
        
        try:
            enable_auto_schema = safe_get_param(self.options.enable_auto_schema) or 'false'
            if enable_auto_schema.lower() == 'true':
                self._schema_info = self._get_postgresql_schema()
                logger.info(f"Detected PostgreSQL schema: {self._schema_info}")
            else:
                logger.info("Auto schema detection disabled")
        except Exception as e:
            logger.error(f"Error detecting PostgreSQL schema: {e}")
            self._schema_info = None
    
    def _get_postgresql_schema(self) -> Optional[List[Dict[str, Any]]]:
        """
        Get schema information from PostgreSQL table.
        
        Returns:
            List of dictionaries containing column information
        """
        try:
            import logging
            logger = logging.getLogger(__name__)
            
            # Define safe_get_param locally for worker context
            def safe_get_param(param):
                if param is None:
                    return None
                if hasattr(param, 'get'):
                    return param.get()
                return param
            
            # Get connection parameters
            host = safe_get_param(self.options.postgresql_host)
            port = safe_get_param(self.options.postgresql_port)
            database = safe_get_param(self.options.postgresql_database)
            username = safe_get_param(self.options.postgresql_username)
            password = safe_get_param(self.options.postgresql_password)
            
            # Get table name for schema detection
            source_table = safe_get_param(self.options.source_table_for_schema)
            if not source_table:
                logger.warning("No source_table_for_schema provided for schema detection")
                return None
            
            # Extract table name (handle schema.table format)
            table_name = source_table.split('.')[-1]
            schema_name = source_table.split('.')[0] if '.' in source_table else 'public'
            
            # Connect to PostgreSQL
            conn = psycopg2.connect(
                host=host,
                port=port,
                database=database,
                user=username,
                password=password
            )
            cursor = conn.cursor()
            
            # Query for table schema information
            schema_query = """
                SELECT 
                    column_name,
                    data_type,
                    character_maximum_length,
                    is_nullable,
                    column_default,
                    ordinal_position
                FROM information_schema.columns 
                WHERE table_schema = %s AND table_name = %s
                ORDER BY ordinal_position
            """
            
            cursor.execute(schema_query, (schema_name, table_name))
            columns = cursor.fetchall()
            
            if not columns:
                logger.error(f"No schema information found for table {source_table}")
                return None
            
            # Format schema information
            schema_info = []
            for col in columns:
                schema_info.append({
                    'name': col[0],
                    'type': col[1],
                    'length': col[2],
                    'nullable': col[3] == 'YES',
                    'default': col[4],
                    'position': col[5]
                })
            
            logger.info(f"Detected {len(schema_info)} columns in PostgreSQL table {source_table}")
            return schema_info
            
        except Exception as e:
            logger.error(f"Error getting PostgreSQL schema: {e}")
            return None
        finally:
            if 'conn' in locals():
                conn.close()
    
    def get_schema_info(self) -> Optional[List[Dict[str, Any]]]:
        """Get the detected schema information."""
        return self._schema_info


class BigQuerySchemaDetectionDoFn(DoFn):
    """
    DoFn for detecting BigQuery table schema.
    
    This DoFn connects to BigQuery and extracts table schema information.
    """
    
    def __init__(self, options: CustomPipelineOptions):
        """
        Initialize BigQuery schema detection.
        
        Args:
            options: Pipeline options containing connection parameters
        """
        self.options = options
        self._schema_info = None
    
    def setup(self):
        """Setup and detect BigQuery table schema."""
        import logging
        logger = logging.getLogger(__name__)
        
        # Define safe_get_param locally for worker context
        def safe_get_param(param):
            if param is None:
                return None
            if hasattr(param, 'get'):
                return param.get()
            return param
        
        try:
            enable_auto_schema = safe_get_param(self.options.enable_auto_schema) or 'false'
            if enable_auto_schema.lower() == 'true':
                self._schema_info = self._get_bigquery_schema()
                logger.info(f"Detected BigQuery schema: {self._schema_info}")
            else:
                logger.info("Auto schema detection disabled")
        except Exception as e:
            logger.error(f"Error detecting BigQuery schema: {e}")
            self._schema_info = None
    
    def _get_bigquery_schema(self) -> Optional[List[Dict[str, Any]]]:
        """
        Get schema information from BigQuery table.
        
        Returns:
            List of dictionaries containing column information
        """
        try:
            import logging
            from google.cloud import bigquery
            logger = logging.getLogger(__name__)
            
            # Define safe_get_param locally for worker context
            def safe_get_param(param):
                if param is None:
                    return None
                if hasattr(param, 'get'):
                    return param.get()
                return param
            
            source_table = safe_get_param(self.options.source_table_for_schema)
            if not source_table:
                logger.warning("No source_table_for_schema provided for BigQuery schema detection")
                return None
            
            # Parse table specification (project.dataset.table)
            parts = source_table.split('.')
            if len(parts) != 3:
                logger.error(f"Invalid BigQuery table specification: {source_table}. Expected format: project.dataset.table")
                return None
            
            project_id, dataset_id, table_id = parts
            
            # Initialize BigQuery client
            client = bigquery.Client(project=project_id)
            
            # Get table reference
            table_ref = client.dataset(dataset_id).table(table_id)
            table = client.get_table(table_ref)
            
            # Extract schema information
            schema_info = []
            for field in table.schema:
                schema_info.append({
                    'name': field.name,
                    'type': field.field_type,
                    'mode': field.mode,
                    'description': field.description,
                    'nullable': field.mode == 'NULLABLE'
                })
            
            logger.info(f"Detected {len(schema_info)} columns in BigQuery table {source_table}")
            return schema_info
            
        except Exception as e:
            logger.error(f"Error getting BigQuery schema: {e}")
            return None
    
    def get_schema_info(self) -> Optional[List[Dict[str, Any]]]:
        """Get the detected schema information."""
        return self._schema_info


class MongoDBSchemaDetectionDoFn(DoFn):
    """
    DoFn for detecting MongoDB collection schema.
    
    This DoFn connects to MongoDB and samples documents to infer
    collection schema including field names, types, and structure.
    """
    
    def __init__(self, options: CustomPipelineOptions):
        """
        Initialize MongoDB schema detection.
        
        Args:
            options: Pipeline options containing connection parameters
        """
        self.options = options
        self._schema_info = None
    
    def setup(self):
        """Setup and detect MongoDB collection schema."""
        import logging
        logger = logging.getLogger(__name__)
        
        # Import MongoDB libraries for worker context
        try:
            from pymongo import MongoClient
            from bson import ObjectId
            mongodb_available = True
        except ImportError:
            mongodb_available = False
        
        # Define safe_get_param locally for worker context
        def safe_get_param(param):
            if param is None:
                return None
            if hasattr(param, 'get'):
                return param.get()
            return param
        
        try:
            if not mongodb_available:
                logger.error("MongoDB support not available. Install pymongo: pip install pymongo")
                self._schema_info = None
                return
                
            enable_auto_schema = safe_get_param(self.options.enable_auto_schema) or 'false'
            if enable_auto_schema.lower() == 'true':
                self._schema_info = self._get_mongodb_schema()
                logger.info(f"Detected MongoDB schema: {self._schema_info}")
            else:
                logger.info("Auto schema detection disabled")
        except Exception as e:
            logger.error(f"Error detecting MongoDB schema: {e}")
            self._schema_info = None
    
    def _get_mongodb_schema(self) -> Optional[List[Dict[str, Any]]]:
        """
        Get schema information from MongoDB collection by sampling documents.
        
        Returns:
            List of dictionaries containing field information
        """
        try:
            import logging
            logger = logging.getLogger(__name__)
            
            # Import MongoDB libraries for worker context
            try:
                from pymongo import MongoClient
                from bson import ObjectId
            except ImportError:
                logger.error("MongoDB support not available in worker context")
                return None
            
            # Define safe_get_param locally for worker context
            def safe_get_param(param):
                if param is None:
                    return None
                if hasattr(param, 'get'):
                    return param.get()
                return param
            
            # Get connection parameters
            connection_string = safe_get_param(self.options.mongodb_connection_string)
            
            if connection_string:
                # Use connection string
                client = MongoClient(connection_string)
                database_name = safe_get_param(self.options.mongodb_database)
                collection_name = safe_get_param(self.options.mongodb_collection)
            else:
                # Build connection from individual parameters
                host = safe_get_param(self.options.mongodb_host)
                port = int(safe_get_param(self.options.mongodb_port) or '27017')
                database_name = safe_get_param(self.options.mongodb_database)
                collection_name = safe_get_param(self.options.mongodb_collection)
                username = safe_get_param(self.options.mongodb_username)
                password = safe_get_param(self.options.mongodb_password)
                
                if username and password:
                    client = MongoClient(host, port, username=username, password=password)
                else:
                    client = MongoClient(host, port)
            
            # Get database and collection
            db = client[database_name]
            collection = db[collection_name]
            
            # Sample documents to infer schema (limit to 1000 for performance)
            sample_size = min(1000, collection.count_documents({}))
            if sample_size == 0:
                logger.warning(f"Collection {database_name}.{collection_name} is empty")
                return None
            
            logger.info(f"Sampling {sample_size} documents from {database_name}.{collection_name}")
            
            # Get sample documents
            documents = list(collection.find().limit(sample_size))
            
            # Analyze schema
            schema_info = self._analyze_documents_schema(documents)
            
            logger.info(f"Detected {len(schema_info)} fields in MongoDB collection {database_name}.{collection_name}")
            return schema_info
            
        except Exception as e:
            logger.error(f"Error getting MongoDB schema: {e}")
            return None
        finally:
            if 'client' in locals():
                client.close()
    
    def _analyze_documents_schema(self, documents: List[Dict]) -> List[Dict[str, Any]]:
        """
        Analyze documents to infer schema structure.
        
        Args:
            documents: List of MongoDB documents
            
        Returns:
            List of field definitions with type information
        """
        field_types = {}
        field_counts = {}
        
        # Analyze each document
        for doc in documents:
            self._analyze_document(doc, field_types, field_counts, prefix="")
        
        # Build schema info
        schema_info = []
        total_docs = len(documents)
        
        for field_path, type_counts in field_types.items():
            # Determine the most common type
            most_common_type = max(type_counts.items(), key=lambda x: x[1])
            field_type = most_common_type[0]
            type_frequency = most_common_type[1]
            
            # Calculate nullable percentage
            field_count = field_counts.get(field_path, 0)
            nullable = field_count < total_docs
            
            schema_info.append({
                'name': field_path,
                'type': field_type,
                'nullable': nullable,
                'frequency': field_count / total_docs,
                'type_frequency': type_frequency / field_count if field_count > 0 else 0,
                'sample_count': field_count
            })
        
        # Sort by field name for consistency
        schema_info.sort(key=lambda x: x['name'])
        
        return schema_info
    
    def _analyze_document(self, doc: Dict, field_types: Dict, field_counts: Dict, prefix: str):
        """
        Recursively analyze document structure.
        
        Args:
            doc: Document to analyze
            field_types: Dictionary to store field type information
            field_counts: Dictionary to store field occurrence counts
            prefix: Field path prefix for nested fields
        """
        for key, value in doc.items():
            field_path = f"{prefix}{key}" if prefix else key
            
            # Count field occurrence
            field_counts[field_path] = field_counts.get(field_path, 0) + 1
            
            # Determine field type
            value_type = self._get_mongodb_type(value)
            
            # Track type frequency
            if field_path not in field_types:
                field_types[field_path] = {}
            field_types[field_path][value_type] = field_types[field_path].get(value_type, 0) + 1
            
            # Handle nested objects (limit depth to avoid infinite recursion)
            if isinstance(value, dict) and len(prefix.split('.')) < 3:
                self._analyze_document(value, field_types, field_counts, f"{field_path}.")
            elif isinstance(value, list) and len(value) > 0 and isinstance(value[0], dict) and len(prefix.split('.')) < 3:
                # Analyze first element of array for schema
                self._analyze_document(value[0], field_types, field_counts, f"{field_path}.")
    
    def _get_mongodb_type(self, value) -> str:
        """
        Get MongoDB BSON type name for a value.
        
        Args:
            value: Value to analyze
            
        Returns:
            BSON type name
        """
        if value is None:
            return 'null'
        elif isinstance(value, bool):
            return 'boolean'
        elif isinstance(value, int):
            return 'int'
        elif isinstance(value, float):
            return 'double'
        elif isinstance(value, str):
            return 'string'
        elif isinstance(value, ObjectId):
            return 'objectId'
        elif isinstance(value, dict):
            return 'object'
        elif isinstance(value, list):
            return 'array'
        elif hasattr(value, '__class__') and 'datetime' in str(value.__class__):
            return 'date'
        else:
            return 'mixed'
    
    def get_schema_info(self) -> Optional[List[Dict[str, Any]]]:
        """Get the detected schema information."""
        return self._schema_info


class MongoDBReadDoFn(DoFn):
    """
    DoFn for reading data from MongoDB collections.
    
    This DoFn connects to MongoDB and reads documents based on
    query and projection parameters.
    """
    
    def __init__(self, options: CustomPipelineOptions):
        """
        Initialize MongoDB read operation.
        
        Args:
            options: Pipeline options containing connection and query parameters
        """
        self.options = options
        self._client = None
        self._collection = None
    
    def setup(self):
        """Setup MongoDB connection."""
        import logging
        logger = logging.getLogger(__name__)
        
        # Import MongoDB libraries for worker context
        try:
            from pymongo import MongoClient
            from bson import ObjectId
            mongodb_available = True
        except ImportError:
            mongodb_available = False
        
        # Define safe_get_param locally for worker context
        def safe_get_param(param):
            if param is None:
                return None
            if hasattr(param, 'get'):
                return param.get()
            return param
        
        try:
            if not mongodb_available:
                raise RuntimeError("MongoDB support not available. Install pymongo: pip install pymongo")
            
            # Get connection parameters
            connection_string = safe_get_param(self.options.mongodb_connection_string)
            
            if connection_string:
                # Use connection string
                self._client = MongoClient(connection_string)
                database_name = safe_get_param(self.options.mongodb_database)
                collection_name = safe_get_param(self.options.mongodb_collection)
            else:
                # Build connection from individual parameters
                host = safe_get_param(self.options.mongodb_host)
                port = int(safe_get_param(self.options.mongodb_port) or '27017')
                database_name = safe_get_param(self.options.mongodb_database)
                collection_name = safe_get_param(self.options.mongodb_collection)
                username = safe_get_param(self.options.mongodb_username)
                password = safe_get_param(self.options.mongodb_password)
                
                if username and password:
                    self._client = MongoClient(host, port, username=username, password=password)
                else:
                    self._client = MongoClient(host, port)
            
            # Get collection
            db = self._client[database_name]
            self._collection = db[collection_name]
            
            logger.info(f"Connected to MongoDB collection: {database_name}.{collection_name}")
            
        except Exception as e:
            logger.error(f"Error setting up MongoDB connection: {e}")
            raise
    
    def start_bundle(self):
        """Start processing bundle - called for each worker."""
        pass
    
    def process(self, element):
        """
        Process element to read MongoDB documents.
        
        This method reads all documents matching the query and yields them.
        Note: In a real implementation, you might want to implement pagination
        or streaming for large collections.
        
        Args:
            element: Input element (not used, can be None)
            
        Yields:
            MongoDB documents as dictionaries
        """
        try:
            import logging
            logger = logging.getLogger(__name__)
            
            # Define safe_get_param locally for worker context
            def safe_get_param(param):
                if param is None:
                    return None
                if hasattr(param, 'get'):
                    return param.get()
                return param
            
            # Parse query and projection
            query = {}
            projection = None
            
            mongodb_query = safe_get_param(self.options.mongodb_query)
            if mongodb_query:
                query = json.loads(mongodb_query)
            
            mongodb_projection = safe_get_param(self.options.mongodb_projection)
            if mongodb_projection:
                projection = json.loads(mongodb_projection)
            
            logger.info(f"Executing MongoDB query: {query}")
            logger.info(f"Using projection: {projection}")
            
            # Execute query
            cursor = self._collection.find(query, projection)
            
            # Process documents
            doc_count = 0
            for document in cursor:
                # Convert ObjectId to string for JSON serialization
                document = self._convert_objectids_to_strings(document)
                
                # Keep only source data - no metadata fields
                
                yield document
                doc_count += 1
            
            logger.info(f"Processed {doc_count} documents from MongoDB")
            
        except Exception as e:
            logger.error(f"Error reading from MongoDB: {e}")
            raise
    
    def _convert_objectids_to_strings(self, doc):
        """
        Recursively convert ObjectId fields to strings and sanitize for BigQuery JSON.
        
        Args:
            doc: Document to process
            
        Returns:
            Document with ObjectIds converted to strings and values sanitized
        """
        if isinstance(doc, dict):
            return {key: self._convert_objectids_to_strings(value) for key, value in doc.items()}
        elif isinstance(doc, list):
            return [self._convert_objectids_to_strings(item) for item in doc]
        elif isinstance(doc, ObjectId):
            return str(doc)
        else:
            # Use sanitization for all other values
            return sanitize_for_bigquery_json(doc)
    
    def finish_bundle(self):
        """Finish processing bundle."""
        pass
    
    def teardown(self):
        """Cleanup MongoDB connection."""
        if self._client:
            self._client.close()


class BigQueryTableCreationDoFn(DoFn):
    """
    DoFn for creating BigQuery tables with proper schema mapping.
    
    This DoFn creates BigQuery tables based on detected source schema,
    handling data type mapping and table configuration.
    """
    
    def __init__(self, options: CustomPipelineOptions, source_schema: Optional[List[Dict[str, Any]]] = None):
        """
        Initialize BigQuery table creation.
        
        Args:
            options: Pipeline options containing table configuration
            source_schema: Detected source schema information
        """
        self.options = options
        self.source_schema = source_schema
        self._table_created = False
    
    def setup(self):
        """Setup and create BigQuery table if enabled."""
        import logging
        logger = logging.getLogger(__name__)
        
        # Define safe_get_param locally for worker context
        def safe_get_param(param):
            if param is None:
                return None
            if hasattr(param, 'get'):
                return param.get()
            return param
        
        try:
            enable_auto_schema = safe_get_param(self.options.enable_auto_schema) or 'false'
            if enable_auto_schema.lower() == 'true' and self.source_schema:
                self._create_bigquery_table()
                logger.info("BigQuery table creation completed")
            else:
                logger.info("Auto schema disabled or no source schema available")
        except Exception as e:
            logger.error(f"Error creating BigQuery table: {e}")
            # Don't fail the pipeline, let it proceed with existing table
    
    def _create_bigquery_table(self):
        """Create BigQuery table with proper schema mapping."""
        try:
            import logging
            from google.cloud import bigquery
            logger = logging.getLogger(__name__)
            
            # Define safe_get_param locally for worker context
            def safe_get_param(param):
                if param is None:
                    return None
                if hasattr(param, 'get'):
                    return param.get()
                return param
            
            # Get destination table information
            project = safe_get_param(self.options.destination_bigquery_project)
            dataset = safe_get_param(self.options.destination_bigquery_dataset)
            table = safe_get_param(self.options.destination_bigquery_table)
            
            # Initialize BigQuery client
            client = bigquery.Client(project=project)
            
            # Check if we should delete existing table
            delete_existing = (safe_get_param(self.options.delete_existing_table) or 'false').lower()
            if delete_existing == 'true':
                try:
                    table_id = f"{project}.{dataset}.{table}"
                    client.delete_table(table_id)
                    logger.info(f"Deleted existing table: {table_id}")
                except Exception as e:
                    logger.info(f"Table {table_id} does not exist or could not be deleted: {e}")
            
            # Create BigQuery schema from source schema
            bq_schema = self._map_schema_to_bigquery(self.source_schema)
            
            # Create table object
            table_id = f"{project}.{dataset}.{table}"
            bq_table = bigquery.Table(table_id, schema=bq_schema)
            
            # Configure partitioning - always enable with default or specified field
            partition_field = safe_get_param(self.options.partition_field) or 'updated_at'
            
            # Check if partition field exists in source schema
            partition_field_exists = False
            if self.source_schema:
                partition_field_exists = any(col['name'] == partition_field for col in self.source_schema)
            
            if partition_field_exists:
                bq_table.time_partitioning = bigquery.TimePartitioning(
                    type_=bigquery.TimePartitioningType.DAY,
                    field=partition_field
                )
                logger.info(f"✅ Configured table partitioning on field: {partition_field}")
            else:
                logger.warning(f"⚠️ Partition field '{partition_field}' not found in source schema. Table created without partitioning.")
            
            # Configure clustering if specified
            clustering_fields = safe_get_param(self.options.clustering_fields)
            if clustering_fields:
                cluster_fields = [field.strip() for field in clustering_fields.split(',')]
                bq_table.clustering_fields = cluster_fields
                logger.info(f"Configured table clustering on fields: {cluster_fields}")
            
            # Create table
            table = client.create_table(bq_table, exists_ok=True)
            self._table_created = True
            
            logger.info(f"Created BigQuery table: {table_id}")
            logger.info(f"Table schema: {len(bq_schema)} columns")
            
        except Exception as e:
            logger.error(f"Error creating BigQuery table: {e}")
            raise
    
    def _map_schema_to_bigquery(self, source_schema: List[Dict[str, Any]]) -> List[bigquery.SchemaField]:
        """
        Map source schema to BigQuery schema fields.
        
        Args:
            source_schema: Source table schema information
            
        Returns:
            List of BigQuery schema fields
        """
        # Multi-source to BigQuery type mapping
        type_mapping = {
            # PostgreSQL types
            'character varying': 'STRING',
            'varchar': 'STRING',
            'text': 'STRING',
            'char': 'STRING',
            'character': 'STRING',
            'integer': 'INTEGER',
            'int': 'INTEGER',
            'int4': 'INTEGER',
            'bigint': 'INTEGER',
            'int8': 'INTEGER',
            'smallint': 'INTEGER',
            'int2': 'INTEGER',
            'serial': 'INTEGER',
            'bigserial': 'INTEGER',
            'numeric': 'NUMERIC',
            'decimal': 'NUMERIC',
            'real': 'FLOAT',
            'float4': 'FLOAT',
            'double precision': 'FLOAT',
            'float8': 'FLOAT',
            'money': 'NUMERIC',
            'timestamp without time zone': 'TIMESTAMP',
            'timestamp with time zone': 'TIMESTAMP',
            'timestamp': 'TIMESTAMP',
            'timestamptz': 'TIMESTAMP',
            'date': 'DATE',
            'time': 'TIME',
            'timetz': 'TIME',
            'interval': 'STRING',
            'boolean': 'BOOLEAN',
            'bool': 'BOOLEAN',
            'json': 'JSON',
            'jsonb': 'JSON',
            'uuid': 'STRING',
            'inet': 'STRING',
            'cidr': 'STRING',
            'macaddr': 'STRING',
            'ARRAY': 'STRING',
            'USER-DEFINED': 'STRING',
            
            # MongoDB BSON types
            'string': 'STRING',
            'int': 'INTEGER',
            'long': 'INTEGER',
            'double': 'FLOAT',
            'decimal128': 'NUMERIC',
            'boolean': 'BOOLEAN',
            'date': 'TIMESTAMP',
            'timestamp': 'TIMESTAMP',
            'objectId': 'STRING',
            'object': 'JSON',
            'array': 'JSON',
            'null': 'STRING',
            'regex': 'STRING',
            'javascript': 'STRING',
            'binary': 'BYTES',
            'mixed': 'STRING',
            
            # BigQuery specific types (for BigQuery to BigQuery)
            'STRING': 'STRING',
            'INTEGER': 'INTEGER',
            'FLOAT': 'FLOAT',
            'NUMERIC': 'NUMERIC',
            'BOOLEAN': 'BOOLEAN',
            'TIMESTAMP': 'TIMESTAMP',
            'DATE': 'DATE',
            'TIME': 'TIME',
            'DATETIME': 'DATETIME',
            'JSON': 'JSON',
            'GEOGRAPHY': 'GEOGRAPHY',
            'BYTES': 'BYTES'
        }
        
        bq_fields = []
        
        for column in source_schema:
            column_name = column['name']
            column_type = column.get('type', 'STRING').lower()
            is_nullable = column.get('nullable', True)
            
            # Map source type to BigQuery type
            bq_type = type_mapping.get(column_type, 'STRING')
            
            # Determine field mode
            mode = 'NULLABLE' if is_nullable else 'REQUIRED'
            
            # Handle array types
            if 'array' in column_type.lower() or column.get('mode') == 'REPEATED':
                mode = 'REPEATED'
                # Extract base type for arrays
                if '[' in column_type:
                    base_type = column_type.split('[')[0]
                    bq_type = type_mapping.get(base_type, 'STRING')
            
            # Create BigQuery schema field
            bq_field = bigquery.SchemaField(
                name=column_name,
                field_type=bq_type,
                mode=mode,
                description=f"Auto-generated from source column: {column_name} ({column_type})"
            )
            
            bq_fields.append(bq_field)
        
        # Keep only source table fields - no metadata to avoid schema complexity
        # This ensures auto-schema creates destination table based ONLY on source table query
        
        return bq_fields
    
    def is_table_created(self) -> bool:
        """Check if table was successfully created."""
        return self._table_created


def run_pipeline(argv=None):
    """
    Main pipeline execution function.
    
    This function:
    1. Parses pipeline options and parameters
    2. Detects source table schema if auto-schema is enabled
    3. Creates BigQuery destination table with proper schema
    4. Creates the Apache Beam pipeline
    5. Reads data from specified sources
    6. Applies transformations
    7. Writes data to BigQuery destination
    8. Handles errors and monitoring
    
    Args:
        argv: Command line arguments
    """
    # Parse pipeline options
    pipeline_options = PipelineOptions(argv)
    custom_options = pipeline_options.view_as(CustomPipelineOptions)
    
    logger.info("🚀 Starting Custom Dataflow Pipeline Template v1.0.0")
    logger.info("🔧 Features: Smart Sync + Auto-Schema + Cross-Platform Data Synchronization")
    
    # Log partition configuration
    partition_field = safe_get_param(custom_options.partition_field) or 'updated_at'
    logger.info(f"🗂️ Partition field configured: {partition_field}")
    
    # Safely get parameter values (handle both ValueProvider and string cases)
    data_source_value = safe_get_param(custom_options.data_source)
    enable_auto_schema_value = safe_get_param(custom_options.enable_auto_schema)
    
    logger.info(f"Data source: {data_source_value}")
    logger.info(f"Auto schema enabled: {enable_auto_schema_value}")
    
    # Initialize schema detection
    source_schema = None
    
    # Safely get enable_auto_schema value
    enable_auto_schema = safe_get_param(custom_options.enable_auto_schema) or 'false'
    
    if enable_auto_schema.lower() == 'true':
        logger.info("Detecting source table schema...")
        
        # Detect schema based on data sources (prioritize first source for schema)
        data_sources = [src.strip() for src in data_source_value.split(',')]
        primary_source = data_sources[0] if data_sources else 'postgresql'
        
        if primary_source == 'postgresql':
            # Detect PostgreSQL schema
            logger.info("Detecting PostgreSQL table schema")
            pg_schema_detector = PostgreSQLSchemaDetectionDoFn(custom_options)
            pg_schema_detector.setup()
            source_schema = pg_schema_detector.get_schema_info()
            
            if source_schema:
                logger.info(f"Successfully detected PostgreSQL schema with {len(source_schema)} columns")
            else:
                logger.warning("Failed to detect PostgreSQL schema")
        
        elif primary_source == 'mongodb':
            # Detect MongoDB schema
            logger.info("Detecting MongoDB collection schema")
            mongo_schema_detector = MongoDBSchemaDetectionDoFn(custom_options)
            mongo_schema_detector.setup()
            source_schema = mongo_schema_detector.get_schema_info()
            
            if source_schema:
                logger.info(f"Successfully detected MongoDB schema with {len(source_schema)} fields")
            else:
                logger.warning("Failed to detect MongoDB schema")
        
        elif primary_source == 'bigquery':
            # Detect BigQuery schema
            logger.info("Detecting BigQuery table schema")
            bq_schema_detector = BigQuerySchemaDetectionDoFn(custom_options)
            bq_schema_detector.setup()
            source_schema = bq_schema_detector.get_schema_info()
            
            if source_schema:
                logger.info(f"Successfully detected BigQuery schema with {len(source_schema)} columns")
            else:
                logger.warning("Failed to detect BigQuery schema")
        
        # Create BigQuery destination table if schema was detected
        if source_schema:
            logger.info("✅ Creating BigQuery destination table with SOURCE TABLE SCHEMA ONLY")
            logger.info(f"📊 Schema detected: {len(source_schema)} columns from source table")
            logger.info("🎯 Note: Destination table will contain ONLY source table attributes (no metadata)")
            
            table_creator = BigQueryTableCreationDoFn(custom_options, source_schema)
            table_creator.setup()
            
            if table_creator.is_table_created():
                logger.info("✅ BigQuery destination table created successfully with source schema")
            else:
                logger.info("ℹ️ BigQuery destination table already exists or creation was skipped")
        else:
            logger.warning("⚠️ No source schema detected, proceeding without auto table creation")
    
    with beam.Pipeline(options=pipeline_options) as pipeline:
        
        # Initialize data collections
        data_collections = []
        
        # Parse data sources (support comma-separated values)
        data_source_value = safe_get_param(custom_options.data_source) or 'bigquery'
        data_sources = [src.strip() for src in data_source_value.split(',')]
        logger.info(f"Data sources: {data_sources}")
        
        # Read from PostgreSQL if specified
        if 'postgresql' in data_sources:
            logger.info("Reading data from PostgreSQL")
            
            # Use smart sync to build dynamic query
            enable_smart_sync = safe_get_param(custom_options.enable_smart_sync) or 'false'
            
            if enable_smart_sync.lower() == 'true':
                logger.info("Smart sync enabled - building dynamic PostgreSQL query")
                postgresql_query = build_smart_postgresql_query(custom_options)
            else:
                logger.info("Smart sync disabled - using provided PostgreSQL query")
                postgresql_query = safe_get_param(custom_options.postgresql_query)
            
            # Extract table name for ReadFromJdbc - prioritize explicit parameter, then auto-detect
            table_name = None
            
            # Option 1: Use explicit postgresql_table parameter if provided
            explicit_table = safe_get_param(custom_options.postgresql_table)
            if explicit_table:
                table_name = explicit_table
                logger.info(f"Using explicit postgresql_table parameter: {table_name}")
            
            # Option 2: Use source_table_for_schema if available (e.g., "public.organizations" -> "organizations")
            elif hasattr(custom_options, 'source_table_for_schema'):
                source_table = safe_get_param(custom_options.source_table_for_schema)
                if source_table:
                    table_name = source_table.split('.')[-1]  # Extract table name from schema.table format
                    logger.info(f"Using table name from source_table_for_schema: {table_name}")
            
            # Option 3: Extract from query if no explicit table specified
            if not table_name and postgresql_query:
                # Simple extraction from "FROM table_name" pattern
                import re
                match = re.search(r'FROM\s+(?:[\w\.]*\.)?(\w+)', postgresql_query, re.IGNORECASE)
                if match:
                    table_name = match.group(1)
                    logger.info(f"Extracted table name from query: {table_name}")
                else:
                    table_name = "data_source"  # Generic fallback
                    logger.warning(f"Could not extract table name from query, using fallback: {table_name}")
            
            # Option 4: Fallback for edge cases
            if not table_name:
                table_name = "data_source"
                logger.warning(f"No table name available, using generic fallback: {table_name}")
            
            # Use a custom DoFn for PostgreSQL reading to avoid JDBC expansion service issues
            logger.info("Using custom PostgreSQL reader for better reliability")
            
            # Create a trigger element to start the PostgreSQL read process
            postgresql_trigger = pipeline | 'Create PostgreSQL Trigger' >> beam.Create([None])
            
            postgresql_data = (
                postgresql_trigger
                | 'Read from PostgreSQL' >> ParDo(PostgreSQLReaderDoFn(custom_options, postgresql_query))
            )
            
            # Keep only source data - no metadata tags
            data_collections.append(postgresql_data)
        
        # Read from MongoDB if specified
        if 'mongodb' in data_sources:
            logger.info("Reading data from MongoDB")
            
            if not MONGODB_AVAILABLE:
                raise RuntimeError("MongoDB support not available. Install pymongo: pip install pymongo")
            
            # Create a dummy element to trigger MongoDB read
            mongodb_trigger = pipeline | 'Create MongoDB Trigger' >> beam.Create([None])
            
            mongodb_data = (
                mongodb_trigger
                | 'Read from MongoDB' >> ParDo(MongoDBReadDoFn(custom_options))
                # Keep only source data - no metadata tags
            )
            data_collections.append(mongodb_data)
        
        # Read from BigQuery if specified
        if 'bigquery' in data_sources:
            logger.info("Reading data from BigQuery")
            
            # Determine read method (table or query)
            source_bigquery_query = safe_get_param(custom_options.source_bigquery_query)
            if source_bigquery_query:
                # Read using SQL query
                bigquery_data = (
                    pipeline
                    | 'Read from BigQuery Query' >> ReadFromBigQuery(
                        query=source_bigquery_query,
                        project=safe_get_param(custom_options.source_bigquery_project),
                        use_standard_sql=True
                    )
                )
            else:
                # Read from specific table
                table_spec = create_bigquery_table_spec(
                    safe_get_param(custom_options.source_bigquery_project),
                    safe_get_param(custom_options.source_bigquery_dataset),
                    safe_get_param(custom_options.source_bigquery_table)
                )
                bigquery_data = (
                    pipeline
                    | 'Read from BigQuery Table' >> ReadFromBigQuery(
                        table=table_spec,
                        project=safe_get_param(custom_options.source_bigquery_project)
                    )
                )
            
            # Keep only source data - no metadata tags
            data_collections.append(bigquery_data)
        
        # Combine data from multiple sources if needed
        if len(data_collections) > 1:
            combined_data = (
                data_collections
                | 'Combine Data Sources' >> beam.Flatten()
            )
        else:
            combined_data = data_collections[0]
        
        # Apply data transformations
        transformed_data = (
            combined_data
            | 'Data Validation' >> ParDo(DataValidationDoFn(custom_options))
            | 'Apply Transformations' >> ParDo(DataTransformationDoFn(custom_options.transformation_config))
        )
        
        # Create destination table specification
        destination_table = create_bigquery_table_spec(
            safe_get_param(custom_options.destination_bigquery_project),
            safe_get_param(custom_options.destination_bigquery_dataset),
            safe_get_param(custom_options.destination_bigquery_table)
        )
        
        # Write to BigQuery destination
        # Prepare BigQuery write parameters
        write_params = {
            'table': destination_table,
            'project': safe_get_param(custom_options.destination_bigquery_project),
            'write_disposition': getattr(
                BigQueryDisposition, 
                safe_get_param(custom_options.write_disposition) or 'WRITE_APPEND'
            ),
            'create_disposition': getattr(
                BigQueryDisposition,
                safe_get_param(custom_options.create_disposition) or 'CREATE_IF_NEEDED'
            ),
            'custom_gcs_temp_location': pipeline_options.get_all_options().get('temp_location'),
        }
        
        # No complex schema - let BigQuery auto-detect from source data
        # This avoids JSON schema compatibility issues
        logger.info("Using BigQuery auto-detection - no predefined schema needed")
        
        write_result = (
            transformed_data
            | 'Write to BigQuery' >> WriteToBigQuery(**write_params)
        )
        
        # Add pipeline monitoring and metrics
        (
            transformed_data
            | 'Count Records' >> beam.combiners.Count.Globally()
            | 'Log Record Count' >> beam.Map(
                lambda count: __import__('logging').getLogger(__name__).info(f"Processed {count} records")
            )
        )
        
        # Log schema information if available
        if source_schema:
            logger.info(f"Pipeline completed with auto-detected schema: {len(source_schema)} columns")
        else:
            logger.info("Pipeline completed without schema detection")
    
    logger.info("Pipeline execution completed")


if __name__ == '__main__':
    """
    Entry point for the Dataflow pipeline template.
    
    This script can be executed directly for testing or deployed as a
    Dataflow template for production use.
    """
    logging.getLogger().setLevel(logging.INFO)
    run_pipeline() 