# flake8: noqa
import humanize
from typing import Any, Union, List
import os
import json
import threading
import time
import random
import uuid
from dotenv import load_dotenv
from datetime import datetime

import dlt
from dlt.common import pendulum
from dlt.sources.sql_database import sql_database
import sqlalchemy as sa

from http.server import SimpleHTTPRequestHandler, HTTPServer


# Load environment variables from .env file
load_dotenv()

# Get database credentials from environment variables
TARGET_DB_USER = os.getenv("TARGET_DB_USER", "symuser")
TARGET_DB_PASS = os.getenv("TARGET_DB_PASS", "sympass")
TARGET_DB_HOST = os.getenv("TARGET_DB_HOST", "127.0.0.1")
TARGET_DB_PORT = os.getenv("TARGET_DB_PORT", "3307")
TARGET_DB_NAME = os.getenv("TARGET_DB_NAME", "dbzains")

SOURCE_DB_USER = os.getenv("SOURCE_DB_USER", "symuser")
SOURCE_DB_PASS = os.getenv("SOURCE_DB_PASS", "sympass")
SOURCE_DB_HOST = os.getenv("SOURCE_DB_HOST", "127.0.0.1")
SOURCE_DB_PORT = os.getenv("SOURCE_DB_PORT", "3306")
SOURCE_DB_NAME = os.getenv("SOURCE_DB_NAME", "dbzains")

FETCH_LIMIT = os.getenv("FETCH_LIMIT", 1)
INTERVAL = int(os.getenv("INTERVAL", 60))  # Interval in seconds

# Batch processing configuration
BATCH_SIZE = int(os.getenv("BATCH_SIZE", 8))  # Process tables in batches of 8
BATCH_DELAY = int(os.getenv("BATCH_DELAY", 2))  # Delay between batches in seconds

# Debug mode for detailed logging
DEBUG_MODE = os.getenv("DEBUG_MODE", "true").lower() == "true"
DEEP_DEBUG_JSON = os.getenv("DEEP_DEBUG_JSON", "true").lower() == "true"
AUTO_SANITIZE_DATA = os.getenv("AUTO_SANITIZE_DATA", "true").lower() == "true"

# Column name preservation configuration
PRESERVE_COLUMN_NAMES = os.getenv("PRESERVE_COLUMN_NAMES", "true").lower() == "true"

def generate_unique_staging_prefix(base_name="dlt_staging"):
    """Generate a truly unique staging schema prefix to prevent collisions between services.
    
    Args:
        base_name: Base name for the staging prefix
    
    Returns:
        str: Unique prefix with timestamp, process ID, and random suffix
    """
    # Get current timestamp
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    
    # Get process ID for additional uniqueness
    process_id = os.getpid()
    
    # Generate random suffix
    random_suffix = uuid.uuid4().hex[:4]
    
    # Create unique prefix
    unique_prefix = f"{base_name}_{timestamp}_{process_id}_{random_suffix}"
    
    return unique_prefix

# Lock timeout and retry configuration
LOCK_TIMEOUT_RETRIES = int(os.getenv("LOCK_TIMEOUT_RETRIES", "5"))  # Max retries for lock timeouts
LOCK_TIMEOUT_BASE_DELAY = int(os.getenv("LOCK_TIMEOUT_BASE_DELAY", "10"))  # Base delay in seconds
LOCK_TIMEOUT_MAX_DELAY = int(os.getenv("LOCK_TIMEOUT_MAX_DELAY", "300"))  # Max delay in seconds
LOCK_TIMEOUT_JITTER = float(os.getenv("LOCK_TIMEOUT_JITTER", "0.1"))  # Jitter factor (0.1 = 10%)

# Transaction management configuration
TRANSACTION_TIMEOUT = int(os.getenv("TRANSACTION_TIMEOUT", "300"))  # Transaction timeout in seconds
MAX_CONCURRENT_TRANSACTIONS = int(os.getenv("MAX_CONCURRENT_TRANSACTIONS", "3"))  # Max concurrent transactions

# Incremental merge optimization configuration
MERGE_BATCH_SIZE = int(os.getenv("MERGE_BATCH_SIZE", "1000"))  # Batch size for merge operations
MERGE_MAX_BATCH_SIZE = int(os.getenv("MERGE_MAX_BATCH_SIZE", "5000"))  # Max batch size for merge operations
MERGE_OPTIMIZATION_ENABLED = os.getenv("MERGE_OPTIMIZATION_ENABLED", "true").lower() == "true"  # Enable merge optimizations
CONNECTION_LOSS_RETRIES = int(os.getenv("CONNECTION_LOSS_RETRIES", "3"))  # Max retries for connection loss

# Staging table isolation configuration
STAGING_ISOLATION_ENABLED = os.getenv("STAGING_ISOLATION_ENABLED", "true").lower() == "true"  # Enable staging table isolation
STAGING_SCHEMA_PREFIX = os.getenv("STAGING_SCHEMA_PREFIX", generate_unique_staging_prefix())  # Dynamic base prefix for staging schemas
STAGING_SCHEMA_RETENTION_HOURS = int(os.getenv("STAGING_SCHEMA_RETENTION_HOURS", "24"))  # How long to keep old staging schemas

DB_SOURCE_URL = f"mysql://{SOURCE_DB_USER}:{SOURCE_DB_PASS}@{SOURCE_DB_HOST}:{SOURCE_DB_PORT}/{SOURCE_DB_NAME}"
DB_TARGET_URL = f"mysql://{TARGET_DB_USER}:{TARGET_DB_PASS}@{TARGET_DB_HOST}:{TARGET_DB_PORT}/{TARGET_DB_NAME}"

# Load table configurations from tables.json
TABLES_FILE = "tables.json"
with open(TABLES_FILE, "r") as f:
    tables_data = json.load(f)

table_configs = {t["table"]: t for t in tables_data}

# Global transaction semaphore to limit concurrent transactions
transaction_semaphore = threading.Semaphore(MAX_CONCURRENT_TRANSACTIONS)

# Global engine variables
ENGINE_SOURCE = None
ENGINE_TARGET = None

def log(message):
    """Log a message with timestamp."""
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"{timestamp} - INFO - {message}")

def validate_table_configurations():
    """Validate all table configurations for proper primary key setup."""
    log("🔍 Validating table configurations...")
    
    for table_name, config in table_configs.items():
        # Check if primary_key exists
        if "primary_key" not in config:
            log(f"❌ Table '{table_name}' missing primary_key configuration")
            continue
            
        primary_key = config["primary_key"]
        
        # Validate primary key configuration
        if not validate_primary_key_config(primary_key):
            log(f"❌ Table '{table_name}' has invalid primary_key configuration: {primary_key}")
            continue
            
        # Log primary key information
        log_primary_key_info(table_name, primary_key)
        
        # Check if modifier exists for incremental sync
        if "modifier" in config:
            log(f"📅 Table '{table_name}' configured for incremental sync using column: {config['modifier']}")
        else:
            log(f"🔄 Table '{table_name}' configured for full refresh sync")
    
    log(f"✅ Table configuration validation completed for {len(table_configs)} tables")

def format_primary_key(primary_key):
    """Format primary key for DLT hints, handling both string and list formats."""
    if isinstance(primary_key, str):
        # If it's a comma-separated string, split it
        if ',' in primary_key:
            return primary_key.split(',')
        else:
            return primary_key
    elif isinstance(primary_key, list):
        return primary_key
    else:
        # Fallback to string representation
        return str(primary_key)

def validate_primary_key_config(primary_key: Union[str, List[str]]) -> bool:
    """
    Validate primary key configuration.
    
    Args:
        primary_key: Primary key configuration to validate
    
    Returns:
        True if valid, False otherwise
    """
    if isinstance(primary_key, str):
        # Single key should be non-empty string
        return bool(primary_key.strip())
    elif isinstance(primary_key, list):
        # Composite key should be non-empty list with non-empty strings
        return (len(primary_key) > 0 and 
                all(isinstance(key, str) and bool(key.strip()) for key in primary_key))
    else:
        return False

def log_primary_key_info(table_name: str, primary_key: Union[str, List[str]]):
    """
    Log information about primary key configuration.
    
    Args:
        table_name: Name of the table
        primary_key: Primary key configuration
    """
    if isinstance(primary_key, list):
        log(f"📋 Table '{table_name}' configured with composite primary key: {primary_key}")
    else:
        log(f"🔑 Table '{table_name}' configured with single primary key: {primary_key}")

# Create engines once with proper connection pool configuration
def create_engines():
    """Create SQLAlchemy engines with optimized connection pool settings and MySQL-specific parameters."""
    pool_settings = {
        'pool_size': 20,           # Configurable base pool size
        'max_overflow': 30,     # Configurable overflow limit  
        'pool_timeout': 60,     # Configurable timeout
        'pool_recycle': 3600,     # Configurable connection recycle time
        'pool_pre_ping': True,            # Validate connections before use
    }
    
    # MySQL-specific connection arguments to handle timeouts and connection stability
    mysql_connect_args = {
        'connect_timeout': 60,
        'read_timeout': 300,
        'write_timeout': 300,
        'autocommit': False,  # Changed to False for better transaction control
        'charset': 'utf8mb4',
        # Permissive SQL mode that allows zero dates and works with both strict/unrestricted modes
        # 'init_command': "SET SESSION sql_mode='ERROR_FOR_DIVISION_BY_ZERO'",
        'init_command': "SET sql_mode=(SELECT REPLACE(@@sql_mode,'ONLY_FULL_GROUP_BY','')); SET SESSION innodb_lock_wait_timeout=120; SET SESSION lock_wait_timeout=120",
        'use_unicode': True,
    }
    
    # Use print() instead of log() since this runs during module initialization
    print(f"Creating engines with pool settings: {pool_settings}")
    print(f"MySQL connection args: {mysql_connect_args}")
    
    engine_source = sa.create_engine(
        DB_SOURCE_URL, 
        connect_args=mysql_connect_args,
        **pool_settings
    )
    engine_target = sa.create_engine(
        DB_TARGET_URL, 
        connect_args=mysql_connect_args,
        **pool_settings
    )
    
    return engine_source, engine_target

# Initialize global engines
ENGINE_SOURCE, ENGINE_TARGET = create_engines()

def retry_on_connection_error(func, db_type="unknown", *args, **kwargs):
    """Enhanced retry function with lock timeout handling, deadlock detection, and exponential backoff.
    
    Args:
        func: Function to retry
        db_type: Type of database ('source' or 'target') for better error logging
        *args, **kwargs: Arguments to pass to the function
    """
    global ENGINE_SOURCE, ENGINE_TARGET  # Declare global at the top
    
    for attempt in range(3): # Simplified retry logic for connection errors
        try:
            return func(*args, **kwargs)
        except sa.exc.OperationalError as e:
            if "MySQL server has gone away" in str(e) or "2006" in str(e):
                log(f"MySQL connection lost on {db_type} database (attempt {attempt + 1}/3). Error: {e}")
                if attempt < 2:
                    log(f"Retrying {db_type} database connection in 30 seconds...")
                    time.sleep(30)
                    # Dispose and recreate engines to reset connection pool
                    ENGINE_SOURCE.dispose()
                    ENGINE_TARGET.dispose()
                    ENGINE_SOURCE, ENGINE_TARGET = create_engines()
                    continue
                else:
                    log(f"Max retries reached for {db_type} database connection. Failing.")
                    raise
            else:
                # Re-raise non-connection errors immediately
                log(f"Non-connection error occurred on {db_type} database: {e}")
                raise
        except sa.exc.ProgrammingError as e:
            if "Commands out of sync" in str(e) or "2014" in str(e):
                log(f"MySQL 'Commands out of sync' error on {db_type} database (attempt {attempt + 1}/3). Error: {e}")
                if attempt < 2:
                    log(f"Retrying {db_type} database connection in 30 seconds...")
                    time.sleep(30)
                    # Dispose and recreate engines to reset connection pool
                    ENGINE_SOURCE.dispose()
                    ENGINE_TARGET.dispose()
                    ENGINE_SOURCE, ENGINE_TARGET = create_engines()
                    continue
                else:
                    log(f"Max retries reached for {db_type} database connection. Failing.")
                    raise
            else:
                # Re-raise non-programming errors immediately
                log(f"Programming error occurred on {db_type} database: {e}")
                raise
        except Exception as e:
            log(f"Non-connection error occurred on {db_type} database: {e}")
            raise

def retry_on_lock_timeout(func, db_type="unknown", operation_name="operation", *args, **kwargs):
    """Enhanced retry function specifically for lock timeout and deadlock errors with exponential backoff.
    
    Args:
        func: Function to retry
        db_type: Type of database ('source' or 'target') for better error logging
        operation_name: Name of the operation being retried for better logging
        *args, **kwargs: Arguments to pass to the function
    """
    global ENGINE_SOURCE, ENGINE_TARGET
    
    for attempt in range(LOCK_TIMEOUT_RETRIES + 1):
        try:
            return func(*args, **kwargs)
        except sa.exc.OperationalError as e:
            error_str = str(e).lower()
            
            # Check for lock timeout errors
            if any(keyword in error_str for keyword in [
                'lock wait timeout exceeded',
                '1205',
                'deadlock found',
                '1213',
                'lock wait timeout',
                'try restarting transaction'
            ]):
                if attempt < LOCK_TIMEOUT_RETRIES:
                    # Calculate delay with exponential backoff and jitter
                    delay = min(
                        LOCK_TIMEOUT_BASE_DELAY * (2 ** attempt),
                        LOCK_TIMEOUT_MAX_DELAY
                    )
                    
                    # Add jitter to prevent thundering herd
                    jitter = delay * LOCK_TIMEOUT_JITTER * random.uniform(-1, 1)
                    final_delay = max(1, delay + jitter)
                    
                    log(f"🔒 Lock timeout/deadlock detected on {db_type} during {operation_name} (attempt {attempt + 1}/{LOCK_TIMEOUT_RETRIES + 1})")
                    log(f"   Error: {e}")
                    log(f"   Waiting {final_delay:.1f}s before retry...")
                    
                    time.sleep(final_delay)
                    
                    # For lock timeouts, try to reset the connection to clear any stuck transactions
                    try:
                        if db_type == "source" and ENGINE_SOURCE:
                            ENGINE_SOURCE.dispose()
                            ENGINE_SOURCE, _ = create_engines()
                        elif db_type == "target" and ENGINE_TARGET:
                            _, ENGINE_TARGET = create_engines()
                        elif db_type == "source+target":
                            ENGINE_SOURCE.dispose()
                            ENGINE_TARGET.dispose()
                            ENGINE_SOURCE, ENGINE_TARGET = create_engines()
                    except Exception as reset_error:
                        log(f"Warning: Could not reset {db_type} connection: {reset_error}")
                    
                    continue
                else:
                    log(f"❌ Max lock timeout retries reached for {operation_name} on {db_type} database")
                    log(f"   Final error: {e}")
                    raise
            else:
                # Re-raise non-lock timeout errors immediately
                log(f"Non-lock timeout error occurred on {db_type} database: {e}")
                raise
        except Exception as e:
            log(f"Non-lock timeout error occurred on {db_type} database: {e}")
            raise

def execute_with_transaction_management(engine, operation_name, operation_func, *args, **kwargs):
    """Execute database operations with proper transaction management and lock timeout handling.
    
    Args:
        engine: SQLAlchemy engine to use
        operation_name: Name of the operation for logging
        operation_func: Function to execute within transaction
        *args, **kwargs: Arguments to pass to the operation function
    """
    # Acquire transaction semaphore to limit concurrent transactions
    with transaction_semaphore:
        log(f"🔒 Acquired transaction semaphore for {operation_name}")
        
        try:
            # Use retry logic specifically for lock timeouts
            return retry_on_lock_timeout(
                _execute_transaction,
                "target" if engine == ENGINE_TARGET else "source",
                operation_name,
                engine,
                operation_func,
                *args,
                **kwargs
            )
        finally:
            log(f"🔓 Released transaction semaphore for {operation_name}")

def _execute_transaction(engine, operation_func, *args, **kwargs):
    """Internal function to execute operations within a transaction context."""
    with engine.begin() as connection:
        # Set transaction timeout
        connection.execute(sa.text(f"SET SESSION innodb_lock_wait_timeout={TRANSACTION_TIMEOUT}"))
        connection.execute(sa.text(f"SET SESSION lock_wait_timeout={TRANSACTION_TIMEOUT}"))
        
        # Execute the operation
        return operation_func(connection, *args, **kwargs)

def ensure_dlt_columns(engine_target, table_name):
    """Check if _dlt_load_id and _dlt_id exist in the target table, add them if not."""
    def _ensure_columns(connection):
        inspector = sa.inspect(engine_target)
        columns = [col["name"] for col in inspector.get_columns(table_name)]
        alter_statements = []
        
        if "_dlt_load_id" not in columns:
            alter_statements.append("ADD COLUMN `_dlt_load_id` TEXT NOT NULL")
        if "_dlt_id" not in columns:
            alter_statements.append("ADD COLUMN `_dlt_id` VARCHAR(128) NOT NULL")
        
        if alter_statements:
            alter_query = f"ALTER TABLE {table_name} {', '.join(alter_statements)};"
            log(f"Altering table {table_name}: {alter_query}")
            connection.execute(sa.text(alter_query))
            return True
        return False
    
    return execute_with_transaction_management(
        engine_target, 
        f"ensure_dlt_columns for {table_name}", 
        _ensure_columns
    )
            
def sync_table_schema(engine_source, engine_target, table_name):
    """Sync schema from source to target, handling new, changed, and deleted columns."""
    def _sync_schema(connection):
        inspector_source = sa.inspect(engine_source)
        inspector_target = sa.inspect(engine_target)
        
        source_columns = {col["name"]: col for col in inspector_source.get_columns(table_name)}
        target_columns = {col["name"] for col in inspector_target.get_columns(table_name)}
        
        alter_statements = []
        for column_name, column_info in source_columns.items():
            if column_name not in target_columns:
                column_type = column_info["type"]
                alter_statements.append(f"ADD COLUMN `{column_name}` {column_type}")
        
        # TODO: Handle _dlt_version column if not exits in target
        # for column_name in target_columns:
        #     if column_name not in source_columns and column_name not in ["_dlt_load_id", "_dlt_id"]:
        #         alter_statements.append(f"DROP COLUMN `{column_name}`")
        
        if alter_statements:
            alter_query = f"ALTER TABLE {table_name} {', '.join(alter_statements)};"
            log(f"Syncing schema for {table_name}: {alter_query}")
            connection.execute(sa.text(alter_query))
            return True
        return False
    
    return execute_with_transaction_management(
        engine_target, 
        f"sync_table_schema for {table_name}", 
        _sync_schema
    )

def validate_table_data(engine_source, table_name):
    """Check table for potential data issues that could cause JSON serialization problems."""
    try:
        with engine_source.connect() as connection:
            # Check if table exists and has data
            count_query = f"SELECT COUNT(*) FROM {table_name}"
            row_count = connection.execute(sa.text(count_query)).scalar()
            
            if row_count == 0:
                if DEBUG_MODE:
                    log(f"Table {table_name} is empty")
                return True
            
            # Check for basic data issues
            sample_query = f"SELECT * FROM {table_name} LIMIT 5"
            result = connection.execute(sa.text(sample_query))
            sample_rows = result.fetchall()
            
            if DEBUG_MODE:
                log(f"Table {table_name} validation - Rows: {row_count}, Sample fetched: {len(sample_rows)}")
            
            # Basic validation passed
            return True
            
    except Exception as validation_error:
        log(f"Table validation failed for {table_name}: {validation_error}")
        return False

def sanitize_data_value(value, column_name="unknown"):
    """Sanitize data values that might cause JSON serialization issues."""
    try:
        if value is None:
            return None
            
        # Handle Decimal objects (common with MySQL DECIMAL columns)
        from decimal import Decimal
        if isinstance(value, Decimal):
            log(f"DEBUG: Converting Decimal to float in column {column_name}: {value}")
            try:
                return float(value)
            except (ValueError, OverflowError) as decimal_error:
                log(f"DEBUG: Failed to convert Decimal to float in column {column_name}: {decimal_error}, converting to string")
                return str(value)
        
        # Handle string values
        if isinstance(value, str):
            # Handle empty strings that cause JSON parse errors
            if value == "":
                log(f"DEBUG: Converting empty string to NULL in column {column_name}")
                return None
                
            # Handle whitespace-only strings that can cause issues
            if value.strip() == "":
                log(f"DEBUG: Converting whitespace-only string to NULL in column {column_name}")
                return None
            
            # Remove NULL bytes that cause JSON errors
            if '\x00' in value:
                log(f"DEBUG: Removing NULL bytes from column {column_name}")
                value = value.replace('\x00', '')
            
            # Handle invalid dates/datetimes
            if column_name.lower().endswith(('_at', '_date', 'date', 'time')) or 'date' in column_name.lower():
                if value in ('0000-00-00', '0000-00-00 00:00:00', '0000-00-00 00:00:00.000000'):
                    log(f"DEBUG: Converting invalid date '{value}' to NULL in column {column_name}")
                    return None
                if value.startswith('0000-00-00'):
                    log(f"DEBUG: Converting invalid date '{value}' to NULL in column {column_name}")
                    return None
            
            # Handle very large strings (>1MB) that might cause memory issues
            if len(value) > 1048576:  # 1MB
                log(f"DEBUG: Truncating very large string in column {column_name}: {len(value)} chars")
                return value[:1048576] + "...[TRUNCATED]"
                
        # Handle datetime objects with invalid values
        elif hasattr(value, 'year') and value.year == 0:
            log(f"DEBUG: Converting datetime with year 0 to NULL in column {column_name}")
            return None
            
        # Handle decimal/numeric edge cases
        elif isinstance(value, (int, float)):
            import math
            if math.isnan(value) or math.isinf(value):
                log(f"DEBUG: Converting NaN/Inf value to NULL in column {column_name}")
                return None
                
        # Test if value can be JSON serialized
        import orjson
        orjson.dumps(value)
        return value
        
    except Exception as sanitize_error:
        log(f"DEBUG: Failed to sanitize value in column {column_name}: {sanitize_error}, converting to string")
        try:
            return str(value) if value is not None else None
        except:
            log(f"DEBUG: Could not convert value to string in column {column_name}, setting to NULL")
            return None

def debug_problematic_rows(engine_source, table_name, limit=10):
    """Debug specific rows that cause JSON serialization issues."""
    try:
        with engine_source.connect() as connection:
            # Get table schema
            inspector = sa.inspect(engine_source)
            columns = inspector.get_columns(table_name)
            column_names = [col['name'] for col in columns]
            
            log(f"DEBUG: Testing rows individually for table {table_name}")
            
            # Get sample data row by row
            for row_num in range(min(limit, 50)):  # Test up to 50 rows
                try:
                    sample_query = f"SELECT * FROM {table_name} LIMIT 1 OFFSET {row_num}"
                    result = connection.execute(sa.text(sample_query))
                    row = result.fetchone()
                    
                    if not row:
                        break
                        
                    # Convert row to dict
                    row_dict = dict(zip(column_names, row))
                    
                    # Test each column individually
                    problematic_columns = []
                    for col_name, value in row_dict.items():
                        try:
                            import orjson
                            orjson.dumps(value)
                        except Exception as col_error:
                            problematic_columns.append({
                                'column': col_name,
                                'value': repr(value),
                                'error': str(col_error),
                                'type': type(value).__name__
                            })
                    
                    if problematic_columns:
                        # Try sanitizing the problematic row first
                        sanitized_row = {}
                        for col_name, value in row_dict.items():
                            sanitized_row[col_name] = sanitize_data_value(value, col_name)
                        
                        # Test if sanitized row can be JSON encoded
                        try:
                            import orjson
                            orjson.dumps(sanitized_row)
                            # Sanitization worked - don't show diagnostic messages
                            continue
                        except Exception as sanitize_test_error:
                            # Only show diagnostic messages if sanitization failed
                            log(f"DEBUG: *** UNSOLVED PROBLEMATIC ROW {row_num + 1} in {table_name} ***")
                            for prob_col in problematic_columns:
                                log(f"DEBUG: Problem column '{prob_col['column']}' = {prob_col['value']}")
                                log(f"DEBUG: Value type: {prob_col['type']}, Error: {prob_col['error']}")
                            log(f"DEBUG: Row {row_num + 1} still problematic after sanitization: {sanitize_test_error}")
                            return problematic_columns  # Return details of first truly problematic row
                        
                except Exception as row_error:
                    log(f"DEBUG: Error testing row {row_num + 1}: {row_error}")
                    continue
            
            log(f"DEBUG: No obviously problematic rows found in first {limit} rows of {table_name}")
            return None
            
    except Exception as debug_error:
        log(f"DEBUG: Failed to debug problematic rows for {table_name}: {debug_error}")
        return None


def debug_json_parse_error(engine_source, table_name, limit=10):
    """Enhanced debugging specifically for 'line 1 column 1 (char 0)' JSON parse errors."""
    log(f"=== ENHANCED JSON PARSE ERROR DEBUGGING FOR {table_name} ===")
    
    try:
        with engine_source.connect() as connection:
            # Get table schema
            inspector = sa.inspect(engine_source)
            columns = inspector.get_columns(table_name)
            column_names = [col['name'] for col in columns]
            
            log(f"PARSE DEBUG: Analyzing {len(column_names)} columns in {table_name}")
            
            # Test sample rows for JSON parse issues
            for row_num in range(min(limit, 20)):
                try:
                    sample_query = f"SELECT * FROM {table_name} LIMIT 1 OFFSET {row_num}"
                    result = connection.execute(sa.text(sample_query))
                    row = result.fetchone()
                    
                    if not row:
                        break
                        
                    # Convert row to dict  
                    row_dict = dict(zip(column_names, row))
                    
                    log(f"PARSE DEBUG: --- Testing Row {row_num + 1} ---")
                    
                    # Check each value for potential JSON parse issues
                    for col_name, value in row_dict.items():
                        # Test the raw value for common JSON parse error causes
                        debug_value_for_json_parse_error(col_name, value, row_num + 1)
                        
                        # If value is a string, test if it looks like JSON that might be parsed
                        if isinstance(value, str) and value.strip():
                            if value.strip().startswith(('{', '[', '"')) or value.strip() in ('null', 'true', 'false'):
                                log(f"PARSE DEBUG: Column '{col_name}' contains JSON-like string: {repr(value[:100])}")
                                # Test if this string can be JSON parsed
                                try:
                                    import json
                                    json.loads(value)
                                    log(f"PARSE DEBUG: ✓ JSON-like string in '{col_name}' is valid JSON")
                                except json.JSONDecodeError as json_err:
                                    log(f"PARSE DEBUG: ✗ JSON-like string in '{col_name}' is INVALID JSON: {json_err}")
                                    debug_value_hex_representation(col_name, value, "Invalid JSON string")
                                    
                except Exception as row_error:
                    log(f"PARSE DEBUG: Error testing row {row_num + 1}: {row_error}")
                    continue
                    
    except Exception as parse_debug_error:
        log(f"PARSE DEBUG: Failed to run enhanced JSON parse debugging: {parse_debug_error}")


def debug_value_for_json_parse_error(col_name, value, row_num):
    """Debug a specific value for JSON parse error potential."""
    try:
        # Check for None/NULL
        if value is None:
            log(f"PARSE DEBUG: Row {row_num}, Column '{col_name}' = None (NULL)")
            return
            
        # Check for empty strings
        if isinstance(value, str):
            if value == "":
                log(f"PARSE DEBUG: ⚠️  Row {row_num}, Column '{col_name}' = EMPTY STRING")
                log(f"PARSE DEBUG: ⚠️  Empty strings cause 'line 1 column 1 (char 0)' when JSON parsed!")
                return
                
            if value.isspace():
                log(f"PARSE DEBUG: ⚠️  Row {row_num}, Column '{col_name}' = WHITESPACE ONLY")
                debug_value_hex_representation(col_name, value, "Whitespace-only string")
                return
                
            # Check for non-printable characters at start
            if value and ord(value[0]) < 32:
                log(f"PARSE DEBUG: ⚠️  Row {row_num}, Column '{col_name}' starts with non-printable character!")
                debug_value_hex_representation(col_name, value, "Non-printable start character")
                return
                
            # Check for NULL bytes
            if '\x00' in value:
                log(f"PARSE DEBUG: ⚠️  Row {row_num}, Column '{col_name}' contains NULL bytes!")
                debug_value_hex_representation(col_name, value, "Contains NULL bytes")
                return
                
        # Check for binary data
        if isinstance(value, bytes):
            log(f"PARSE DEBUG: ⚠️  Row {row_num}, Column '{col_name}' is binary data ({len(value)} bytes)")
            if len(value) == 0:
                log(f"PARSE DEBUG: ⚠️  Binary data is EMPTY - would cause JSON parse error!")
            debug_value_hex_representation(col_name, value, "Binary data")
            return
            
        # Test if value can be JSON serialized (encoding test)
        try:
            import orjson
            orjson.dumps(value)
        except Exception as encode_error:
            log(f"PARSE DEBUG: ⚠️  Row {row_num}, Column '{col_name}' cannot be JSON encoded: {encode_error}")
            debug_value_hex_representation(col_name, value, f"JSON encode error: {encode_error}")
            
    except Exception as debug_error:
        log(f"PARSE DEBUG: Error debugging value in column '{col_name}': {debug_error}")


def debug_value_hex_representation(col_name, value, reason):
    """Show hex representation of problematic values."""
    try:
        if value is None:
            log(f"PARSE DEBUG: HEX for '{col_name}': None")
            return
            
        if isinstance(value, str):
            hex_repr = ' '.join(f'{ord(c):02x}' for c in value[:50])  # First 50 chars
            log(f"PARSE DEBUG: HEX for '{col_name}' ({reason}): {hex_repr}")
            log(f"PARSE DEBUG: RAW for '{col_name}': {repr(value[:100])}")
            log(f"PARSE DEBUG: LENGTH: {len(value)} characters")
            
        elif isinstance(value, bytes):
            hex_repr = ' '.join(f'{b:02x}' for b in value[:50])  # First 50 bytes
            log(f"PARSE DEBUG: HEX for '{col_name}' ({reason}): {hex_repr}")
            log(f"PARSE DEBUG: RAW for '{col_name}': {repr(value[:100])}")
            log(f"PARSE DEBUG: LENGTH: {len(value)} bytes")
            
        else:
            log(f"PARSE DEBUG: VALUE for '{col_name}' ({reason}): {repr(value)} (type: {type(value).__name__})")
            
    except Exception as hex_error:
        log(f"PARSE DEBUG: Failed to show hex representation for '{col_name}': {hex_error}")


def debug_json_operation(operation_name, value, context=""):
    """Debug JSON operations that might cause 'line 1 column 1 (char 0)' errors."""
    try:
        log(f"JSON DEBUG: About to perform {operation_name}")
        log(f"JSON DEBUG: Context: {context}")
        log(f"JSON DEBUG: Value type: {type(value).__name__}")
        
        if value is None:
            log(f"JSON DEBUG: ⚠️  Value is None - this will cause JSON parse errors!")
            return
            
        if isinstance(value, str):
            log(f"JSON DEBUG: String length: {len(value)}")
            if len(value) == 0:
                log(f"JSON DEBUG: ⚠️  EMPTY STRING - this causes 'line 1 column 1 (char 0)' error!")
                debug_value_hex_representation("JSON_INPUT", value, "Empty string for JSON operation")
                return
                
            if len(value) <= 100:
                log(f"JSON DEBUG: String value: {repr(value)}")
            else:
                log(f"JSON DEBUG: String value (first 100 chars): {repr(value[:100])}")
                
            # Check for whitespace-only strings
            if value.isspace():
                log(f"JSON DEBUG: ⚠️  WHITESPACE-ONLY STRING - this causes JSON parse errors!")
                debug_value_hex_representation("JSON_INPUT", value, "Whitespace-only string for JSON operation")
                return
                
        elif isinstance(value, bytes):
            log(f"JSON DEBUG: Bytes length: {len(value)}")
            if len(value) == 0:
                log(f"JSON DEBUG: ⚠️  EMPTY BYTES - this causes JSON parse errors!")
                return
            log(f"JSON DEBUG: Bytes value (first 50): {repr(value[:50])}")
            
        else:
            log(f"JSON DEBUG: Non-string value: {repr(value)}")
            
    except Exception as debug_error:
        log(f"JSON DEBUG: Error during JSON operation debugging: {debug_error}")


def safe_json_loads(json_string, context="", default=None):
    """Safely load JSON with enhanced debugging for 'line 1 column 1 (char 0)' errors."""
    try:
        # Pre-operation debugging
        debug_json_operation("json.loads()", json_string, context)
        
        # Perform the actual JSON loading
        import json
        result = json.loads(json_string)
        log(f"JSON DEBUG: ✓ Successfully parsed JSON for context: {context}")
        return result
        
    except json.JSONDecodeError as json_err:
        log(f"JSON DEBUG: ✗ JSONDecodeError in context '{context}': {json_err}")
        
        # Special handling for the specific error we're debugging
        if "line 1 column 1" in str(json_err) and "char 0" in str(json_err):
            log(f"JSON DEBUG: *** CAUGHT THE 'line 1 column 1 (char 0)' ERROR! ***")
            log(f"JSON DEBUG: This error occurred while trying to parse JSON in context: {context}")
            debug_value_hex_representation("FAILED_JSON_INPUT", json_string, "Caused line 1 column 1 (char 0) error")
        
        if default is not None:
            log(f"JSON DEBUG: Returning default value: {default}")
            return default
        else:
            raise
            
    except Exception as other_error:
        log(f"JSON DEBUG: ✗ Unexpected error during JSON parsing in context '{context}': {other_error}")
        debug_value_hex_representation("ERROR_JSON_INPUT", json_string, f"Caused unexpected error: {other_error}")
        raise


def debug_table_data(engine_source, table_name, sample_size=3):
    """Debug function to inspect table data that might cause JSON issues."""
    try:
        # First try the detailed row-by-row debugging
        problematic_data = debug_problematic_rows(engine_source, table_name, limit=20)
        
        if problematic_data:
            log(f"DEBUG: Found specific problematic data in {table_name}")
            return False  # Indicate problems found
        
        # If no obvious problems, do basic validation
        with engine_source.connect() as connection:
            # Check if table exists and has data
            count_query = f"SELECT COUNT(*) FROM {table_name}"
            row_count = connection.execute(sa.text(count_query)).scalar()
            
            if row_count == 0:
                if DEBUG_MODE:
                    log(f"Table {table_name} is empty")
                return True
            
            # Check for basic data issues
            sample_query = f"SELECT * FROM {table_name} LIMIT {sample_size}"
            result = connection.execute(sa.text(sample_query))
            sample_rows = result.fetchall()
            
            if DEBUG_MODE:
                log(f"Table {table_name} validation - Rows: {row_count}, Sample fetched: {len(sample_rows)}")
            
            # Basic validation passed
            return True
            
    except Exception as validation_error:
        log(f"Table validation failed for {table_name}: {validation_error}")
        return False

@dlt.transformer
def sanitize_table_data(items):
    """DLT transformer to sanitize data before JSON serialization."""
    for item in items:
        if isinstance(item, dict):
            sanitized_item = {}
            for key, value in item.items():
                sanitized_item[key] = sanitize_data_value(value, key)
            yield sanitized_item
        else:
            yield item

def force_table_clear(engine_target, table_name):
    """Force clear a table using aggressive methods to prevent lock timeouts."""
    def _clear_table(connection):
        log(f"🧹 Force clearing table {table_name} to prevent lock timeouts...")
        
        try:
            # First try TRUNCATE (fastest, minimal locking)
            log(f"Attempting TRUNCATE for {table_name}")
            connection.execute(sa.text(f"TRUNCATE TABLE {table_name}"))
            log(f"✅ TRUNCATE successful for {table_name}")
            return True
        except Exception as truncate_error:
            log(f"⚠️  TRUNCATE failed for {table_name}: {truncate_error}")
            log(f"Falling back to aggressive DELETE...")
            
            # Fallback to DELETE with very small batches to minimize lock time
            try:
                # Get table row count
                count_result = connection.execute(sa.text(f"SELECT COUNT(*) FROM {table_name}"))
                total_rows = count_result.scalar()
                
                if total_rows == 0:
                    log(f"Table {table_name} is already empty")
                    return True
                
                log(f"Table {table_name} has {total_rows} rows, deleting in small batches...")
                
                # Use very small batches to minimize lock duration
                batch_size = 1000  # Smaller than the default 10000
                deleted_rows = 0
                
                while deleted_rows < total_rows:
                    delete_query = f"DELETE FROM {table_name} LIMIT {batch_size}"
                    result = connection.execute(sa.text(delete_query))
                    batch_deleted = result.rowcount
                    
                    if batch_deleted == 0:
                        break
                    
                    deleted_rows += batch_deleted
                    log(f"Deleted {deleted_rows}/{total_rows} rows from {table_name}")
                    
                    # Very short delay between batches
                    time.sleep(0.05)  # 50ms delay
                
                log(f"✅ Force DELETE completed for {table_name}: {deleted_rows} rows deleted")
                return True
                
            except Exception as delete_error:
                log(f"❌ Force DELETE also failed for {table_name}: {delete_error}")
                raise
                
    return execute_with_transaction_management(
        engine_target,
        f"force_table_clear for {table_name}",
        _clear_table
    )

def safe_table_cleanup(engine_target, table_name, write_disposition="replace"):
    """Safely clean up table data based on write disposition with lock timeout handling.
    
    Args:
        engine_target: Target database engine
        table_name: Name of the table to clean up
        write_disposition: DLT write disposition ('replace' or 'merge')
    
    Returns:
        bool: True if cleanup was successful
    """
    def _cleanup_table(connection):
        if write_disposition == "replace":
            # For replace operations, we need to clear the table
            log(f"🧹 Cleaning up table {table_name} for replace operation")
            
            # First try TRUNCATE (faster, less locking)
            try:
                log(f"Attempting TRUNCATE for {table_name}")
                connection.execute(sa.text(f"TRUNCATE TABLE {table_name}"))
                log(f"✅ TRUNCATE successful for {table_name}")
                return True
            except Exception as truncate_error:
                log(f"⚠️  TRUNCATE failed for {table_name}: {truncate_error}")
                log(f"Falling back to DELETE with batching...")
                
                # Fallback to DELETE with batching to avoid long locks
                try:
                    # Get table row count
                    count_result = connection.execute(sa.text(f"SELECT COUNT(*) FROM {table_name}"))
                    total_rows = count_result.scalar()
                    
                    if total_rows == 0:
                        log(f"Table {table_name} is already empty")
                        return True
                    
                    log(f"Table {table_name} has {total_rows} rows, deleting in batches...")
                    
                    # Delete in batches of 10000 to avoid long locks
                    batch_size = 10000
                    deleted_rows = 0
                    
                    while deleted_rows < total_rows:
                        delete_query = f"DELETE FROM {table_name} LIMIT {batch_size}"
                        result = connection.execute(sa.text(delete_query))
                        batch_deleted = result.rowcount
                        
                        if batch_deleted == 0:
                            break
                        
                        deleted_rows += batch_deleted
                        log(f"Deleted {deleted_rows}/{total_rows} rows from {table_name}")
                        
                        # Small delay between batches to reduce lock pressure
                        time.sleep(0.1)
                    
                    log(f"✅ DELETE cleanup completed for {table_name}: {deleted_rows} rows deleted")
                    return True
                    
                except Exception as delete_error:
                    log(f"❌ DELETE cleanup also failed for {table_name}: {delete_error}")
                    raise
                    
        elif write_disposition == "merge":
            # For merge operations, we don't need to clean up the table
            log(f"ℹ️  No cleanup needed for {table_name} (merge operation)")
            return True
        else:
            log(f"⚠️  Unknown write disposition '{write_disposition}' for {table_name}")
            return False
    
    return execute_with_transaction_management(
        engine_target,
        f"safe_table_cleanup for {table_name}",
        _cleanup_table
    )

def get_max_timestamp(engine_target, table_name, column_name):
    """Get the maximum timestamp value from the target table with lock timeout handling."""
    def _get_timestamp(connection):
        try:
            query = f"SELECT MAX(`{column_name}`) FROM {table_name}"
            result = connection.execute(sa.text(query))
            max_timestamp = result.scalar()
            return max_timestamp if max_timestamp else pendulum.datetime(2020, 1, 1)
        except Exception as e:
            log(f"Error getting max timestamp for {table_name}.{column_name}: {e}")
            return pendulum.datetime(2020, 1, 1)
    
    return execute_with_transaction_management(
        engine_target,
        f"get_max_timestamp for {table_name}.{column_name}",
        _get_timestamp
    )




            
def process_tables_batch(pipeline, engine_source, engine_target, tables_dict, write_disposition="merge", staging_schema_name=None):
    """Process a batch of tables with proper connection management and lock timeout handling."""
    if not tables_dict:
        return
    
    table_names = list(tables_dict.keys())
    log(f"Processing batch of {len(table_names)} tables: {table_names}")
    
    # Verify staging isolation is working before processing
    if staging_schema_name and STAGING_ISOLATION_ENABLED:
        periodic_staging_verification(engine_target, staging_schema_name, f"batch_processing_{table_names[0]}")
    
    # Pre-cleanup tables for replace operations to avoid lock timeouts during DLT processing
    if write_disposition == "replace":
        log(f"🔒 Pre-cleaning tables for replace operation to prevent lock timeouts...")
        for table_name in table_names:
            try:
                log(f"Pre-cleaning table: {table_name}")
                safe_table_cleanup(engine_target, table_name, write_disposition)
                log(f"✅ Pre-cleanup completed for {table_name}")
            except Exception as cleanup_error:
                log(f"⚠️  Pre-cleanup failed for {table_name}: {cleanup_error}")
                log(f"Will continue with DLT processing, but may encounter lock timeouts...")
    
    try:
        # Create source with only this batch of tables
        if DEBUG_MODE:
            log(f"Creating DLT source for tables: {table_names}")
        source_batch = sql_database(engine_source).with_resources(*table_names)
        
        # Log column name preservation status
        if PRESERVE_COLUMN_NAMES:
            log(f"🔧 Column name preservation is ENABLED")
            log(f"   This will prevent DLT from converting 'ViaInput' to 'via_input'")
            log(f"   Column names will be preserved at the destination level")
        else:
            log(f"⚠️  Column name preservation is DISABLED")
            log(f"   DLT may normalize column names (e.g., 'ViaInput' → 'via_input')")
        
        if write_disposition == "merge":
            # Incremental tables
            for table, config in tables_dict.items():
                log(f"Setting incremental for table {table} on column {config['modifier']}")
                max_timestamp = pendulum.instance(get_max_timestamp(engine_target, table, config["modifier"])).in_tz("Asia/Bangkok")
                log(f"Setting incremental for table {table} on column {config['modifier']} with initial value {max_timestamp}")
                
                # Log primary key information
                log_primary_key_info(table, config["primary_key"])
                
                # Format primary key for DLT
                formatted_primary_key = format_primary_key(config["primary_key"])
                
                getattr(source_batch, table).apply_hints(
                    primary_key=formatted_primary_key,
                    incremental=dlt.sources.incremental(config["modifier"],
                    initial_value=max_timestamp)
                )
        
        # Add debugging before running the pipeline
        if DEBUG_MODE:
            log(f"Starting DLT pipeline run for batch: {table_names}, write_disposition: {write_disposition}")
        
        # Run the batch with enhanced error handling
        if DEBUG_MODE and PRESERVE_COLUMN_NAMES:
            log(f"🔍 Debug mode: About to run pipeline with column name preservation")
            log(f"   Tables: {table_names}")
            log(f"   Write disposition: {write_disposition}")
        
        info = pipeline.run(source_batch, write_disposition=write_disposition)
        log(f"Batch completed successfully: {info}")
        
    except Exception as e:
        error_message = str(e)
        error_type = type(e).__name__
        log(f"Error in batch processing: {error_message}")
        log(f"Error type: {error_type}")
        
        # Check for connection lost errors specifically
        if any(keyword in error_message.lower() for keyword in [
            'lost connection to server during query',
            '2013',
            'connection lost',
            'server has gone away',
            '2006'
        ]):
            log(f"🔌 Connection lost error detected in batch processing!")
            log(f"This is likely caused by long-running queries or server timeouts.")
            log(f"Attempting to recover with individual table processing...")
            
            # Try processing tables individually to isolate the problem
            for table_name in table_names:
                try:
                    log(f"Attempting individual processing for table: {table_name}")
                    
                    # Create single table source
                    single_source = sql_database(engine_source).with_resources(table_name)
                    
                    if write_disposition == "merge" and "modifier" in tables_dict[table_name]:
                        config = tables_dict[table_name]
                        max_timestamp = pendulum.instance(get_max_timestamp(engine_target, table_name, config["modifier"])).in_tz("Asia/Bangkok")
                        
                        # Log primary key information for connection error recovery
                        log_primary_key_info(table_name, config["primary_key"])
                        
                        # Format primary key for DLT
                        formatted_primary_key = format_primary_key(config["primary_key"])
                        
                        getattr(single_source, table_name).apply_hints(
                            primary_key=formatted_primary_key,
                            incremental=dlt.sources.incremental(config["modifier"], initial_value=max_timestamp)
                        )
                    
                    # Pre-cleanup for replace operations using force_table_clear
                    if write_disposition == "replace":
                        force_table_clear(engine_target, table_name)
                    
                    # Process individual table
                    single_info = pipeline.run(single_source, write_disposition=write_disposition)
                    log(f"✅ Individual processing successful for {table_name}: {single_info}")
                    
                except Exception as individual_error:
                    individual_error_str = str(individual_error)
                    log(f"❌ Individual processing failed for {table_name}: {individual_error_str}")
                    
                    # Check if it's still a connection error
                    if any(keyword in individual_error_str.lower() for keyword in [
                        'lost connection', '2013', 'server has gone away', '2006'
                    ]):
                        log(f"🔌 Connection error persists for {table_name}, skipping...")
                        continue
                    else:
                        log(f"Different error for {table_name}, continuing...")
                        continue
            
            log(f"Individual table processing completed")
            return  # Success with individual processing
        
        # Check for lock timeout and deadlock errors specifically
        if any(keyword in error_message.lower() for keyword in [
            'lock wait timeout exceeded',
            '1205',
            'deadlock found',
            '1213',
            'lock wait timeout',
            'try restarting transaction'
        ]):
            log(f"🔒 Lock timeout/deadlock detected in batch processing!")
            log(f"This is likely caused by concurrent access or long-running transactions.")
            log(f"Attempting to recover with individual table processing using isolated staging...")
            
            # Try processing tables individually to isolate the problem
            for table_name in table_names:
                try:
                    log(f"Attempting individual processing for table: {table_name}")
                    
                    # Create a NEW pipeline with isolated staging for this individual table
                    # This ensures we don't inherit the lock timeout issues from the batch pipeline
                    individual_pipeline_name = f"dlt_individual_{table_name}_{int(time.time())}"
                    
                    if staging_schema_name and STAGING_ISOLATION_ENABLED:
                        log(f"🔧 Creating isolated staging pipeline for individual table: {table_name}")
                        individual_pipeline = create_isolated_staging_pipeline(engine_target, individual_pipeline_name, staging_schema_name)
                    else:
                        log(f"⚠️  Using fallback pipeline for individual table: {table_name}")
                        individual_pipeline = create_fresh_pipeline(engine_target, individual_pipeline_name)
                    
                    # Create single table source
                    single_source = sql_database(engine_source).with_resources(table_name)
                    
                    if write_disposition == "merge" and "modifier" in tables_dict[table_name]:
                        config = tables_dict[table_name]
                        max_timestamp = pendulum.instance(get_max_timestamp(engine_target, table_name, config["modifier"])).in_tz("Asia/Bangkok")
                        
                        # Log primary key information for lock timeout recovery
                        log_primary_key_info(table_name, config["primary_key"])
                        
                        # Format primary key for DLT
                        formatted_primary_key = format_primary_key(config["primary_key"])
                        
                        getattr(single_source, table_name).apply_hints(
                            primary_key=formatted_primary_key,
                            incremental=dlt.sources.incremental(config["modifier"], initial_value=max_timestamp)
                        )
                    
                    # Pre-cleanup for replace operations using force_table_clear
                    if write_disposition == "replace":
                        force_table_clear(engine_target, table_name)
                    
                    # Process individual table with the new isolated pipeline
                    single_info = individual_pipeline.run(single_source, write_disposition=write_disposition)
                    log(f"✅ Individual processing successful for {table_name}: {single_info}")
                    
                    # Clean up the individual pipeline after successful processing
                    try:
                        del individual_pipeline
                        log(f"🧹 Cleaned up individual pipeline for {table_name}")
                    except Exception as cleanup_error:
                        log(f"⚠️  Could not clean up individual pipeline for {table_name}: {cleanup_error}")
                    
                except Exception as individual_error:
                    individual_error_str = str(individual_error)
                    log(f"❌ Individual processing failed for {table_name}: {individual_error_str}")
                    
                    # Check if it's still a lock timeout
                    if any(keyword in individual_error_str.lower() for keyword in [
                        'lock wait timeout exceeded', '1205', 'deadlock found', '1213'
                    ]):
                        log(f"🔒 Lock timeout persists for {table_name}, skipping...")
                        log(f"   This suggests the issue is deeper than staging table conflicts")
                        log(f"   Consider checking for long-running transactions or database locks")
                        continue
                    else:
                        log(f"Different error for {table_name}, continuing...")
                        continue
            
            log(f"Individual table processing completed")
            return  # Success with individual processing
        
        # Check for DLT state corruption errors specifically
        if any(keyword in error_message.lower() for keyword in [
            'unexpected character: line 1 column 1 (char 0)',
            'incorrect padding',
            'jsondecodeerror',
            'decompress_state',
            'load_pipeline_state_from_destination'
        ]):
            log(f"🔧 DLT state corruption detected in batch processing!")
            log(f"This is likely caused by corrupted pipeline state, not your data.")
            log(f"Attempting to recover by creating a fresh pipeline...")
            
            try:
                # Create a completely fresh pipeline with a new name
                import uuid
                recovery_pipeline_name = f"dlt_recovery_pipeline_{uuid.uuid4().hex[:8]}"
                log(f"Creating recovery pipeline: {recovery_pipeline_name}")
                
                recovery_pipeline = create_fresh_pipeline(engine_target, recovery_pipeline_name)
                
                # Try the batch again with the fresh pipeline
                log(f"Retrying batch with recovery pipeline: {table_names}")
                recovery_info = recovery_pipeline.run(source_batch, write_disposition=write_disposition)
                log(f"🎉 Recovery successful! Batch completed with fresh pipeline: {recovery_info}")
                return  # Success with recovery
                
            except Exception as recovery_error:
                log(f"Recovery attempt failed: {recovery_error}")
                log(f"Will fall back to individual table processing...")
                # Continue to the individual table processing below
        
        # Handle specific JSON decode errors
        if "JSONDecodeError" in error_message or "orjson" in error_message:
            log(f"JSON parsing error detected for tables: {table_names}")
            log(f"This may be caused by data encoding issues - attempting individual sync with enhanced debugging")
            
            # Try alternative approach: process each table with simpler method
            for table_name in table_names:
                try:
                    # Validate table data first
                    if not validate_table_data(engine_source, table_name):
                        log(f"Table validation failed for {table_name}, skipping")
                        continue
                    
                    log(f"Attempting individual sync for table: {table_name} with enhanced error handling")
                    
                    # Try a more basic approach with error catching
                    single_source = sql_database(engine_source).with_resources(table_name)
                    
                    if write_disposition == "merge" and "modifier" in tables_dict[table_name]:
                        config = tables_dict[table_name]
                        max_timestamp = pendulum.instance(get_max_timestamp(engine_target, table_name, config["modifier"])).in_tz("Asia/Bangkok")
                        
                        # Log primary key information for JSON error recovery
                        log_primary_key_info(table_name, config["primary_key"])
                        
                        # Format primary key for DLT
                        formatted_primary_key = format_primary_key(config["primary_key"])
                        
                        getattr(single_source, table_name).apply_hints(
                            primary_key=formatted_primary_key,
                            incremental=dlt.sources.incremental(config["modifier"], initial_value=max_timestamp)
                        )
                    
                    # Add debugging info before individual table run
                    if DEBUG_MODE:
                        log(f"About to run individual sync for {table_name}")
                    
                    # Try individual sync with same write disposition
                    try:
                        log(f"Starting individual DLT pipeline run for {table_name}")
                        single_info = pipeline.run(single_source, write_disposition=write_disposition)
                        log(f"Individual sync successful for {table_name}: {single_info}")
                    except Exception as pipeline_error:
                        log(f"Pipeline run failed for {table_name}: {pipeline_error}")
                        log(f"Pipeline error type: {type(pipeline_error).__name__}")
                        
                        # Immediately check for JSON errors and debug
                        pipeline_error_str = str(pipeline_error)
                        full_pipeline_error = pipeline_error_str
                        if hasattr(pipeline_error, '__cause__') and pipeline_error.__cause__:
                            full_pipeline_error += " CAUSED BY: " + str(pipeline_error.__cause__)
                        if hasattr(pipeline_error, '__context__') and pipeline_error.__context__:
                            full_pipeline_error += " CONTEXT: " + str(pipeline_error.__context__)
                        
                        # Check if this is a JSON error right away
                        is_immediate_json_error = any(keyword in full_pipeline_error.lower() for keyword in [
                            'jsondecodeerror', 'orjson', 'json decode', 'unexpected character',
                            'line 1 column 1', 'char 0', 'invalid json', 'json parse',
                            'decode error', 'serialization', 'dumps', 'loads'
                        ])
                        
                        if is_immediate_json_error:
                            log(f"*** IMMEDIATE JSON ERROR DETECTED FOR {table_name} ***")
                            log(f"Full pipeline error: {full_pipeline_error}")
                            
                            # Special debugging for "line 1 column 1 (char 0)" errors
                            if "line 1 column 1" in full_pipeline_error.lower() and "char 0" in full_pipeline_error.lower():
                                log(f"*** DETECTED 'line 1 column 1 (char 0)' ERROR - This suggests empty/None value being JSON parsed ***")
                                log(f"This error typically occurs when trying to parse an empty string, None, or invalid data as JSON")
                                
                                # Enhanced debugging for this specific error
                                try:
                                    debug_json_parse_error(engine_source, table_name, limit=10)
                                except Exception as parse_debug_error:
                                    log(f"Enhanced JSON parse debugging failed: {parse_debug_error}")
                            
                            # Do immediate debugging before trying sanitization
                            log(f"Analyzing data immediately to find JSON issue...")
                            try:
                                problematic_data = debug_problematic_rows(engine_source, table_name, limit=20)
                                if problematic_data:
                                    log(f"IMMEDIATE ANALYSIS - Found problematic data:")
                                    for prob_col in problematic_data[:3]:  # Show first 3 problems
                                        log(f"  Problem: {prob_col['column']} = {prob_col['value']}")
                                        log(f"  Error: {prob_col['error']}")
                            except Exception as immediate_debug_error:
                                log(f"Immediate debugging failed: {immediate_debug_error}")
                        
                        # If it's a JSON error, try with sanitization
                        if "JSONDecodeError" in str(pipeline_error) or "orjson" in str(pipeline_error) or is_immediate_json_error:
                            if AUTO_SANITIZE_DATA:
                                log(f"JSON error during pipeline run for {table_name}, trying with data sanitization...")
                                
                                try:
                                    # Create a new source with sanitization transformer
                                    sanitized_source = sql_database(engine_source).with_resources(table_name)
                                    
                                    # Apply sanitization transformer
                                    # sanitized_resource = getattr(sanitized_source, table_name) | sanitize_table_data
                                    
                                    if write_disposition == "merge" and "modifier" in tables_dict[table_name]:
                                        config = tables_dict[table_name]
                                        max_timestamp = pendulum.instance(get_max_timestamp(engine_target, table_name, config["modifier"])).in_tz("Asia/Bangkok")
                                        # sanitized_resource.apply_hints(
                                        #     primary_key=config["primary_key"],
                                        #     incremental=dlt.sources.incremental(config["modifier"], initial_value=max_timestamp)
                                        # )
                                    
                                    # Try running with sanitized data
                                    log(f"Running {table_name} with data sanitization...")
                                    sanitized_info = pipeline.run(sanitized_source, write_disposition=write_disposition)
                                    log(f"Individual sync with sanitization successful for {table_name}: {sanitized_info}")
                                    continue  # Success with sanitization, move to next table
                                    
                                except Exception as sanitized_error:
                                    log(f"Even sanitized sync failed for {table_name}: {sanitized_error}")
                                    # Continue to deep debugging below
                            else:
                                log(f"JSON error during pipeline run for {table_name} - auto sanitization disabled")
                        
                        # Re-raise the error to be handled by the outer exception handler
                        raise pipeline_error
                    
                except Exception as individual_error:
                    individual_error_msg = str(individual_error)
                    log(f"Individual sync also failed for {table_name}: {individual_error_msg}")
                    log(f"Individual error type: {type(individual_error).__name__}")
                    
                    # Check for JSON errors in nested exceptions too
                    full_error_chain = str(individual_error)
                    if hasattr(individual_error, '__cause__') and individual_error.__cause__:
                        full_error_chain += " CAUSED BY: " + str(individual_error.__cause__)
                    if hasattr(individual_error, '__context__') and individual_error.__context__:
                        full_error_chain += " CONTEXT: " + str(individual_error.__context__)
                    
                    # Enhanced JSON error detection
                    is_json_error = any(keyword in full_error_chain.lower() for keyword in [
                        'jsondecodeerror', 'orjson', 'json decode', 'unexpected character',
                        'line 1 column 1', 'char 0', 'invalid json', 'json parse',
                        'decode error', 'serialization', 'dumps', 'loads'
                    ])
                    
                    # If it's still a JSON error, do deep debugging
                    if is_json_error:
                        log(f"=== COMPREHENSIVE JSON ERROR ANALYSIS FOR TABLE: {table_name} ===")
                        log(f"Full error chain: {full_error_chain}")
                        
                        # Special debugging for "line 1 column 1 (char 0)" errors
                        if "line 1 column 1" in full_error_chain.lower() and "char 0" in full_error_chain.lower():
                            log(f"*** COMPREHENSIVE ANALYSIS: DETECTED 'line 1 column 1 (char 0)' ERROR ***")
                            log(f"This error typically occurs when trying to parse an empty string, None, or invalid data as JSON")
                            
                            # Enhanced debugging for this specific error
                            try:
                                debug_json_parse_error(engine_source, table_name, limit=15)
                            except Exception as parse_debug_error:
                                log(f"Enhanced JSON parse debugging failed in comprehensive analysis: {parse_debug_error}")
                        
                        # Always do deep debugging for JSON errors, regardless of DEEP_DEBUG_JSON setting
                        # since we need to identify the root cause
                        
                        # Get full error traceback
                        import traceback
                        log(f"Complete error traceback:")
                        log(traceback.format_exc())
                        
                        # Use the enhanced row-by-row debugging
                        log(f"=== ANALYZING PROBLEMATIC ROWS AND COLUMNS FOR {table_name} ===")
                        try:
                            problematic_data = debug_problematic_rows(engine_source, table_name, limit=100)
                            
                            if problematic_data:
                                log(f"*** FOUND UNSOLVED PROBLEMATIC DATA IN {table_name} ***")
                                log(f"Number of problematic columns after sanitization: {len(problematic_data)}")
                                for i, prob_col in enumerate(problematic_data):
                                    log(f"Unsolved Problem {i+1}: Column '{prob_col['column']}' = {prob_col['value']}")
                                    log(f"  Data type: {prob_col['type']}")
                                    log(f"  JSON Error: {prob_col['error']}")
                                    log(f"  Raw bytes: {repr(prob_col['value'])}")
                            else:
                                log(f"No problematic data found in first 100 rows of {table_name}")
                                log(f"Issue may be in data beyond the first 100 rows or in DLT processing")
                                
                                # Fallback: try a simple sample to see what we can find
                                log(f"Trying fallback analysis...")
                                try:
                                    with engine_source.connect() as conn:
                                        sample_query = f"SELECT * FROM {table_name} LIMIT 5"
                                        result = conn.execute(sa.text(sample_query))
                                        rows = result.fetchall()
                                        if rows:
                                            log(f"Sample data from {table_name}:")
                                            columns = [desc[0] for desc in result.description]
                                            for i, row in enumerate(rows[:2]):  # Show first 2 rows
                                                row_dict = dict(zip(columns, row))
                                                log(f"  Row {i+1}: {row_dict}")
                                                
                                                # Check each value for JSON serializability
                                                for col_name, value in row_dict.items():
                                                    try:
                                                        import orjson
                                                        orjson.dumps(value)
                                                    except Exception as json_test_error:
                                                        log(f"  *** Column {col_name} fails JSON encoding: {json_test_error}")
                                                        log(f"  *** Problematic value: {repr(value)}")
                                except Exception as fallback_error:
                                    log(f"Fallback analysis also failed: {fallback_error}")
                                    
                        except Exception as debug_error:
                            log(f"Failed to debug problematic rows: {debug_error}")
                            
                            # Emergency fallback - at least show us the basic table info
                            log(f"EMERGENCY FALLBACK - Basic table info for {table_name}:")
                            try:
                                with engine_source.connect() as conn:
                                    count_query = f"SELECT COUNT(*) FROM {table_name}"
                                    count = conn.execute(sa.text(count_query)).scalar()
                                    log(f"  Row count: {count}")
                            except Exception as count_error:
                                log(f"  Could not get row count: {count_error}")
                        
                        log(f"=== END COMPREHENSIVE ANALYSIS FOR {table_name} ===")
                        
                        log(f"Persistent JSON error for table {table_name} - skipping this table for now")
                        log(f"Based on analysis above, check the specific problematic columns/values")
                        continue
                    else:
                        log(f"Non-JSON error for table {table_name}: {individual_error_msg}")
                        continue
        else:
            # Re-raise non-JSON errors
            log(f"Non-JSON error in batch processing: {error_message}")
            raise e
    
    # Small delay to let connections settle
    time.sleep(1)

def periodic_connection_monitoring(engine_target, interval_seconds=60):
    """Periodically monitor and clean up problematic connections and queries.
    
    Args:
        engine_target: Target database engine
        interval_seconds: How often to perform monitoring
    """
    def _monitor():
        while True:
            try:
                log(f"🔍 Performing periodic connection monitoring...")
                
                # Monitor and kill long-running queries
                killed_queries = monitor_and_kill_long_queries(engine_target, timeout_seconds=120)
                
                # Check connection pool status
                pool_size = engine_target.pool.size()
                checked_out = engine_target.pool.checkedout()
                overflow = engine_target.pool.overflow()
                
                log(f"📊 Connection pool status - Size: {pool_size}, Checked out: {checked_out}, Overflow: {overflow}")
                
                # If too many connections are checked out, log a warning
                if checked_out > pool_size * 0.8:
                    log(f"⚠️  High connection usage detected ({checked_out}/{pool_size})")
                
                # Wait for next monitoring cycle
                time.sleep(interval_seconds)
                
            except Exception as monitor_error:
                log(f"⚠️  Periodic monitoring error: {monitor_error}")
                time.sleep(interval_seconds)
    
    # Start monitoring in a separate thread
    monitor_thread = threading.Thread(target=_monitor, daemon=True)
    monitor_thread.start()
    log(f"🔄 Started periodic connection monitoring (every {interval_seconds}s)")

def load_select_tables_from_database() -> None:
    """Use the sql_database source to reflect an entire database schema and load select tables from it."""
    def _load_tables():
        # Use global engines instead of creating new ones
        engine_source = ENGINE_SOURCE
        engine_target = ENGINE_TARGET
        
        # Log configuration values being used
        log(f"🔧 Pipeline Configuration:")
        log(f"   Lock Timeout Retries: {LOCK_TIMEOUT_RETRIES}")
        log(f"   Lock Timeout Base Delay: {LOCK_TIMEOUT_BASE_DELAY}s")
        log(f"   Lock Timeout Max Delay: {LOCK_TIMEOUT_MAX_DELAY}s")
        log(f"   Lock Timeout Jitter: {LOCK_TIMEOUT_JITTER}")
        log(f"   Transaction Timeout: {TRANSACTION_TIMEOUT}s")
        log(f"   Max Concurrent Transactions: {MAX_CONCURRENT_TRANSACTIONS}")
        log(f"   Merge Batch Size: {MERGE_BATCH_SIZE}")
        log(f"   Merge Max Batch Size: {MERGE_MAX_BATCH_SIZE}")
        log(f"   Merge Optimization Enabled: {MERGE_OPTIMIZATION_ENABLED}")
        log(f"   Connection Loss Retries: {CONNECTION_LOSS_RETRIES}")
        log(f"   Staging Isolation Enabled: {STAGING_ISOLATION_ENABLED}")
        log(f"   Staging Schema Prefix: {STAGING_SCHEMA_PREFIX}")
        log(f"   Staging Retention Hours: {STAGING_SCHEMA_RETENTION_HOURS}")
        
        if STAGING_ISOLATION_ENABLED:
            log(f"🔒 Collision Prevention: Using unique prefix '{STAGING_SCHEMA_PREFIX}' to prevent staging table conflicts")
        
        # Log connection pool status
        log(f"Source pool status - Size: {engine_source.pool.size()}, Checked out: {engine_source.pool.checkedout()}")
        log(f"Target pool status - Size: {engine_target.pool.size()}, Checked out: {engine_target.pool.checkedout()}")
        
        # Start periodic connection monitoring
        log(f"🔄 Starting periodic connection monitoring...")
        periodic_connection_monitoring(engine_target, interval_seconds=120)
        
        # Staging schema management
        if STAGING_ISOLATION_ENABLED:
            log(f"🔧 Staging isolation is ENABLED - creating unique staging schema...")
            
            # Clean up old staging schemas first
            try:
                cleaned_count = cleanup_old_staging_schemas(
                    engine_target, 
                    STAGING_SCHEMA_PREFIX, 
                    STAGING_SCHEMA_RETENTION_HOURS
                )
                log(f"🧹 Cleaned up {cleaned_count} old staging schemas")
            except Exception as cleanup_error:
                log(f"⚠️  Failed to clean up old staging schemas: {cleanup_error}")
                log(f"   Continuing with pipeline execution...")
            
            # Create unique staging schema for this run
            try:
                staging_schema_name = create_unique_staging_schema(engine_target, STAGING_SCHEMA_PREFIX)
                log(f"✅ Created unique staging schema: {staging_schema_name}")
                
                # Show current staging schema status
                try:
                    status = get_staging_schema_status(engine_target, STAGING_SCHEMA_PREFIX)
                    log(f"📊 Current staging schemas: {status['total_schemas']} total")
                    for schema in status['schemas'][:3]:  # Show first 3
                        log(f"   - {schema['name']}: {schema['table_count']} tables, {schema['total_rows']} rows, age: {schema['age_hours']}h")
                except Exception as status_error:
                    log(f"⚠️  Could not get staging schema status: {status_error}")
                
            except Exception as schema_error:
                log(f"❌ Failed to create unique staging schema: {schema_error}")
                log(f"   Falling back to default staging behavior...")
                staging_schema_name = None
        else:
            log(f"ℹ️  Staging isolation is DISABLED - using default staging behavior")
            staging_schema_name = None
        
        # Use a single pipeline instance with state corruption handling
        pipeline_name = "dlt_unified_pipeline_v1_0_18"
        
        if staging_schema_name and STAGING_ISOLATION_ENABLED:
            pipeline = create_isolated_staging_pipeline(engine_target, pipeline_name, staging_schema_name)
            
            # Verify that DLT is actually using our isolated staging schema
            try:
                log(f"🔍 Verifying staging schema isolation is working...")
                if verify_staging_schema_usage(engine_target, staging_schema_name):
                    log(f"✅ Staging isolation verification successful!")
                else:
                    log(f"❌ Staging isolation verification failed - DLT may not be using isolated staging!")
                    log(f"   This could explain why you're still seeing lock timeouts!")
            except Exception as verify_error:
                log(f"⚠️  Could not verify staging schema usage: {verify_error}")
        else:
            pipeline = create_merge_optimized_pipeline(engine_target, pipeline_name)
        
        # Configure DLT to handle both strict and unrestricted MySQL modes automatically
        try:
            import dlt.common.configuration as dlt_config
            # Use safe defaults that work with both MySQL modes
            dlt_config.get_config()["normalize"]["json_module"] = "orjson"
            dlt_config.get_config()["data_writer"]["disable_compression"] = False
            
            # Additional configurations to handle problematic data
            dlt_config.get_config()["normalize"]["max_nesting_level"] = 10
            dlt_config.get_config()["extract"]["max_parallel_items"] = 1  # Reduce parallelism to avoid conflicts
            
            # Try to handle JSON serialization issues better
            try:
                dlt_config.get_config()["normalize"]["skip_nulls"] = False
                dlt_config.get_config()["extract"]["workers"] = 1  # Single worker to avoid race conditions
            except Exception as nested_config_error:
                log(f"Could not set advanced DLT config: {nested_config_error}")
            
            log(f"DLT configuration applied successfully")
            
        except Exception as config_error:
            log(f"Warning: Could not set DLT configuration: {config_error}")
            # Continue without custom config - use defaults

        incremental_tables = {t: config for t, config in table_configs.items() if "modifier" in config}
        full_refresh_tables = [t for t, config in table_configs.items() if "modifier" not in config]

        log(f"Total tables to process: {len(table_configs)} (Incremental: {len(incremental_tables)}, Full refresh: {len(full_refresh_tables)})")
        log(f"Using batch size: {BATCH_SIZE} with {BATCH_DELAY}s delay between batches")

        # Ensure _dlt_load_id and _dlt_id exist in target tables
        # Process schema changes in batches too to avoid overwhelming connections
        all_tables = list(table_configs.keys())
        for i in range(0, len(all_tables), BATCH_SIZE):
            batch_tables = all_tables[i:i + BATCH_SIZE]
            log(f"Ensuring DLT columns for batch: {batch_tables}")
            for table in batch_tables:
                try:
                    log(f"Processing table: {table}")
                    log(f"Table config: {table_configs.get(table, {})}")
                    
                    ensure_dlt_columns(engine_target, table)
                    sync_table_schema(engine_source, engine_target, table)
                    
                    # Optimize incremental tables for merge operations
                    if table in incremental_tables:
                        config = incremental_tables[table]
                        # Handle both string and list formats for primary keys
                        primary_key_config = config.get('primary_key', 'id')
                        if isinstance(primary_key_config, str):
                            primary_keys = primary_key_config.split(',')
                        else:
                            primary_keys = primary_key_config if isinstance(primary_key_config, list) else ['id']
                        
                        log(f"🔧 Optimizing incremental table {table} for merge operations")
                        log(f"   Primary keys: {primary_keys}")
                        optimize_incremental_merge(engine_target, table, primary_keys)
                    
                    log(f"✅ Completed processing for table: {table}")
                    
                except Exception as table_error:
                    log(f"❌ Error processing table {table}: {table_error}")
                    log(f"   Error type: {type(table_error).__name__}")
                    log(f"   Table config: {table_configs.get(table, {})}")
                    # Continue with next table rather than failing completely
                    continue
                

                
            if i + BATCH_SIZE < len(all_tables):  # Don't delay after the last batch
                time.sleep(BATCH_DELAY)

        # Process incremental tables in batches
        if incremental_tables:
            log(f"Processing {len(incremental_tables)} incremental tables in batches of {BATCH_SIZE}")
            incremental_items = list(incremental_tables.items())
            
            for i in range(0, len(incremental_items), BATCH_SIZE):
                batch_items = incremental_items[i:i + BATCH_SIZE]
                batch_dict = dict(batch_items)
                
                try:
                    process_tables_batch(pipeline, engine_source, engine_target, batch_dict, "merge", staging_schema_name)
                except Exception as e:
                    log(f"Error processing incremental batch {i//BATCH_SIZE + 1}: {e}")
                    log(f"Pipeline execution failed at stage sync with exception:")
                    log(f"{type(e)}")
                    log(f"{str(e)}")
                    # Continue with next batch rather than failing completely
                    continue
                
                # Delay between batches (except for the last one)
                if i + BATCH_SIZE < len(incremental_items):
                    log(f"Waiting {BATCH_DELAY}s before next batch...")
                    time.sleep(BATCH_DELAY)
        
        # Process full refresh tables in batches
        if full_refresh_tables:
            log(f"Processing {len(full_refresh_tables)} full refresh tables in batches of {BATCH_SIZE}")
            
            for i in range(0, len(full_refresh_tables), BATCH_SIZE):
                batch_tables = full_refresh_tables[i:i + BATCH_SIZE]
                batch_dict = {table: table_configs[table] for table in batch_tables}
                
                try:
                    process_tables_batch(pipeline, engine_source, engine_target, batch_dict, "replace", staging_schema_name)
                except Exception as e:
                    log(f"Error processing full refresh batch {i//BATCH_SIZE + 1}: {e}")
                    # Continue with next batch rather than failing completely
                    continue
                
                # Delay between batches (except for the last one)
                if i + BATCH_SIZE < len(full_refresh_tables):
                    log(f"Waiting {BATCH_DELAY}s before next batch...")
                    time.sleep(BATCH_DELAY)

        # Log final connection pool status
        log(f"Final source pool status - Size: {engine_source.pool.size()}, Checked out: {engine_source.pool.checkedout()}")
        log(f"Final target pool status - Size: {engine_target.pool.size()}, Checked out: {engine_target.pool.checkedout()}")
        
        # Clean up staging schema after successful completion
        if staging_schema_name and STAGING_ISOLATION_ENABLED:
            try:
                log(f"🧹 Cleaning up staging schema after successful completion: {staging_schema_name}")
                cleanup_staging_schema_after_completion(engine_target, staging_schema_name)
                log(f"✅ Staging schema cleanup completed")
            except Exception as final_cleanup_error:
                log(f"⚠️  Failed to clean up staging schema after completion: {final_cleanup_error}")
                log(f"   Schema {staging_schema_name} will be cleaned up in next run")

    return retry_on_connection_error(_load_tables, "source+target (main data loading)")

def cleanup_staging_schema_after_completion(engine_target, staging_schema_name):
    """Clean up the staging schema after successful pipeline completion.
    
    Args:
        engine_target: Target database engine
        staging_schema_name: Name of the staging schema to clean up
    """
    def _cleanup_schema(connection):
        log(f"🗑️  Dropping completed staging schema: {staging_schema_name}")
        
        # Drop the schema and all its tables
        drop_schema_query = f"DROP SCHEMA IF EXISTS `{staging_schema_name}`"
        connection.execute(sa.text(drop_schema_query))
        
        # Verify schema was dropped
        verify_query = f"SELECT SCHEMA_NAME FROM INFORMATION_SCHEMA.SCHEMATA WHERE SCHEMA_NAME = '{staging_schema_name}'"
        result = connection.execute(sa.text(verify_query))
        if not result.fetchone():
            log(f"✅ Staging schema {staging_schema_name} successfully cleaned up")
        else:
            log(f"⚠️  Staging schema {staging_schema_name} may not have been fully cleaned up")
    
    return execute_with_transaction_management(
        engine_target,
        f"cleanup_staging_schema_after_completion for {staging_schema_name}",
        _cleanup_schema
    )

def cleanup_corrupted_dlt_state(engine_target, pipeline_name):
    """Clean up corrupted DLT pipeline state tables."""
    try:
        log(f"Checking for corrupted DLT state for pipeline: {pipeline_name}")
        
        # DLT state tables that might contain corrupted data
        state_tables = [
            f"_dlt_pipeline_state",
            f"_dlt_loads", 
            f"_dlt_version"
        ]
        
        with engine_target.connect() as connection:
            inspector = sa.inspect(engine_target)
            existing_tables = inspector.get_table_names()
            
            for state_table in state_tables:
                if state_table in existing_tables:
                    log(f"Found DLT state table: {state_table}")
                    
                    # Check if the state table has corrupted data
                    try:
                        if state_table == "_dlt_pipeline_state":
                            # Check for corrupted pipeline state specifically
                            check_query = f"SELECT pipeline_name, state FROM {state_table} WHERE pipeline_name = '{pipeline_name}'"
                            result = connection.execute(sa.text(check_query))
                            state_rows = result.fetchall()
                            
                            for row in state_rows:
                                pipeline_name_val, state_val = row
                                log(f"Found state for pipeline: {pipeline_name_val}")
                                
                                # Try to validate the state data
                                if state_val is None or state_val == '' or state_val.strip() == '':
                                    log(f"⚠️  Empty or NULL state detected for pipeline {pipeline_name_val} - this causes JSON decode errors!")
                                    
                                    # Delete the corrupted state
                                    delete_query = f"DELETE FROM {state_table} WHERE pipeline_name = '{pipeline_name}'"
                                    connection.execute(sa.text(delete_query))
                                    connection.commit()
                                    log(f"✅ Deleted corrupted state for pipeline: {pipeline_name_val}")
                                    return True
                                
                                # Try to decode the state to check if it's valid
                                try:
                                    import base64
                                    # Test base64 decoding
                                    base64.b64decode(state_val, validate=True)
                                    log(f"✅ State appears valid for pipeline: {pipeline_name_val}")
                                except Exception as decode_error:
                                    log(f"⚠️  Invalid base64 state detected for pipeline {pipeline_name_val}: {decode_error}")
                                    
                                    # Delete the corrupted state
                                    delete_query = f"DELETE FROM {state_table} WHERE pipeline_name = '{pipeline_name}'"
                                    connection.execute(sa.text(delete_query))
                                    connection.commit()
                                    log(f"✅ Deleted corrupted state for pipeline: {pipeline_name_val}")
                                    return True
                        else:
                            # For other state tables, just check if they exist and are accessible
                            count_query = f"SELECT COUNT(*) FROM {state_table}"
                            count = connection.execute(sa.text(count_query)).scalar()
                            log(f"State table {state_table} has {count} rows")
                            
                    except Exception as table_error:
                        log(f"Error checking state table {state_table}: {table_error}")
                        
                        # If we can't even read the table, it might be corrupted
                        if "doesn't exist" not in str(table_error).lower():
                            log(f"⚠️  State table {state_table} appears corrupted, attempting to drop and recreate")
                            try:
                                drop_query = f"DROP TABLE IF EXISTS {state_table}"
                                connection.execute(sa.text(drop_query))
                                connection.commit()
                                log(f"✅ Dropped corrupted state table: {state_table}")
                                return True
                            except Exception as drop_error:
                                log(f"Failed to drop corrupted state table {state_table}: {drop_error}")
                else:
                    log(f"DLT state table {state_table} does not exist yet")
            
        log(f"DLT state validation completed for pipeline: {pipeline_name}")
        return False  # No corruption found
        
    except Exception as cleanup_error:
        log(f"Error during DLT state cleanup: {cleanup_error}")
        return False

def monitor_and_kill_long_queries(engine_target, timeout_seconds=300):
    """Monitor and kill long-running queries that might cause connection timeouts.
    
    Args:
        engine_target: Target database engine
        timeout_seconds: Maximum query execution time before killing
    
    Returns:
        int: Number of queries killed
    """
    def _monitor_queries(connection):
        try:
            # Find long-running queries
            query = """
            SELECT id, user, host, db, command, time, state, info 
            FROM information_schema.PROCESSLIST 
            WHERE command != 'Sleep' 
            AND time > %s 
            AND info IS NOT NULL 
            AND info != ''
            """
            
            result = connection.execute(sa.text(query), (timeout_seconds,))
            long_running = result.fetchall()
            
            killed_count = 0
            
            for process in long_running:
                process_id, user, host, db, command, time, state, info = process
                
                # Skip system processes and our own connections
                if user in ['system user', 'event_scheduler'] or 'dlt' in str(info).lower():
                    continue
                
                log(f"🔍 Found long-running query (ID: {process_id}, Time: {time}s): {info[:100]}...")
                
                try:
                    # Kill the long-running query
                    kill_query = f"KILL {process_id}"
                    connection.execute(sa.text(kill_query))
                    log(f"✅ Killed long-running query ID: {process_id}")
                    killed_count += 1
                except Exception as kill_error:
                    log(f"⚠️  Failed to kill query ID {process_id}: {kill_error}")
            
            if killed_count > 0:
                log(f"🎯 Killed {killed_count} long-running queries to prevent timeouts")
            else:
                log(f"ℹ️  No long-running queries found")
            
            return killed_count
            
        except Exception as monitor_error:
            log(f"⚠️  Error monitoring queries: {monitor_error}")
            return 0
    
    return execute_with_transaction_management(
        engine_target,
        "monitor_and_kill_long_queries",
        _monitor_queries
    )

def create_fresh_pipeline(engine_target, pipeline_name):
    """Create a fresh DLT pipeline instance, handling any existing state corruption."""
    try:
        log(f"Creating fresh DLT pipeline: {pipeline_name}")
        
        # First, clean up any corrupted state
        state_was_corrupted = cleanup_corrupted_dlt_state(engine_target, pipeline_name)
        
        if state_was_corrupted:
            log(f"Corrupted state was cleaned up, creating fresh pipeline")
        
        # Monitor and kill any long-running queries before creating pipeline
        try:
            log(f"🔍 Monitoring for long-running queries before pipeline creation...")
            killed_queries = monitor_and_kill_long_queries(engine_target, timeout_seconds=60)
            if killed_queries > 0:
                log(f"🧹 Cleaned up {killed_queries} long-running queries")
        except Exception as monitor_error:
            log(f"⚠️  Query monitoring failed: {monitor_error}")
        
        # Create destination with comprehensive column name preservation
        if PRESERVE_COLUMN_NAMES:
            log(f"🔧 Configuring DLT destination with comprehensive column name preservation")
            log(f"   This will prevent DLT from converting 'ViaInput' to 'via_input'")
            
            # Create destination with column name preservation using DLT's built-in options
            log(f"   Using DLT's built-in column name preservation features")
            destination = dlt.destinations.sqlalchemy(
                engine_target,
                # Preserve exact table names without conversion
                table_name=lambda table: table,
                # Preserve exact column names without conversion
                column_name=lambda column: column,
                # Disable column name normalization
                normalize_column_name=False
            )
            
            log(f"   ✅ Custom destination created with column name preservation")
        else:
            log(f"⚠️  Using default DLT destination configuration (column names may be normalized)")
            destination = dlt.destinations.sqlalchemy(engine_target)
        
        # Create the pipeline with error handling
        pipeline = dlt.pipeline(
            pipeline_name=pipeline_name,
            destination=destination,
            dataset_name=TARGET_DB_NAME,
            dev_mode=False  # Ensure we're not in dev mode which can cause state issues
        )
        
        log(f"✅ Fresh pipeline created successfully: {pipeline_name}")
        return pipeline
        
    except Exception as pipeline_error:
        log(f"Error creating fresh pipeline: {pipeline_error}")
        
        # If pipeline creation fails due to state issues, try more aggressive cleanup
        log(f"Attempting aggressive state cleanup...")
        try:
            with engine_target.connect() as connection:
                # Drop all DLT state tables for this pipeline
                state_cleanup_queries = [
                    f"DROP TABLE IF EXISTS _dlt_pipeline_state",
                    f"DROP TABLE IF EXISTS _dlt_loads", 
                    f"DROP TABLE IF EXISTS _dlt_version",
                    f"DROP TABLE IF EXISTS {TARGET_DB_NAME}_dlt_pipeline_state",
                    f"DROP TABLE IF EXISTS {TARGET_DB_NAME}_dlt_loads",
                    f"DROP TABLE IF EXISTS {TARGET_DB_NAME}_dlt_version"
                ]
                
                for cleanup_query in state_cleanup_queries:
                    try:
                        connection.execute(sa.text(cleanup_query))
                        connection.commit()
                        log(f"Executed cleanup: {cleanup_query}")
                    except Exception as cleanup_query_error:
                        # Ignore errors for tables that don't exist
                        if "doesn't exist" not in str(cleanup_query_error).lower():
                            log(f"Cleanup query failed (non-critical): {cleanup_query_error}")
                
                log(f"Aggressive state cleanup completed")
                
                # Try creating pipeline again with comprehensive column name preservation
                if PRESERVE_COLUMN_NAMES:
                    log(f"🔧 Configuring recovery DLT destination with comprehensive column name preservation")
                    
                    # Create recovery destination with column name preservation using DLT's built-in options
                    log(f"   Using DLT's built-in column name preservation features for recovery")
                    destination = dlt.destinations.sqlalchemy(
                        engine_target,
                        # Preserve exact table names without conversion
                        table_name=lambda table: table,
                        # Preserve exact column names without conversion
                        column_name=lambda column: column,
                        # Disable column name normalization
                        normalize_column_name=False
                    )
                    
                    log(f"   ✅ Recovery custom destination created with column name preservation")
                else:
                    log(f"⚠️  Using default recovery DLT destination configuration")
                    destination = dlt.destinations.sqlalchemy(engine_target)
                
                pipeline = dlt.pipeline(
                    pipeline_name=pipeline_name,
                    destination=destination,
                    dataset_name=TARGET_DB_NAME,
                    dev_mode=False
                )
                
                log(f"✅ Pipeline created successfully after aggressive cleanup: {pipeline_name}")
                return pipeline
                
        except Exception as aggressive_cleanup_error:
            log(f"Aggressive cleanup also failed: {aggressive_cleanup_error}")
            raise


class SimpleHandler(SimpleHTTPRequestHandler):
    def do_GET(self):
        self.send_response(200)
        self.send_header("Content-type", "text/plain")
        self.end_headers()
        self.wfile.write(b"We are Groot!")

def run_http_server():
    server_address = ("", 8089)  # Serve on all interfaces, port 8089
    httpd = HTTPServer(server_address, SimpleHandler)
    print("Serving on port 8089...")
    httpd.serve_forever()

def run_pipeline():
    # Validate table configurations at startup (after all functions are defined)
    validate_table_configurations()
    
    if INTERVAL > 0:
        while True:
            log(f"### STARTING PIPELINE ###")
            try:
                load_select_tables_from_database()
                log(f"### PIPELINE COMPLETED ###")
            except Exception as e:
                log(f"### PIPELINE ERROR: {str(e)} ###")
                # Wait a bit before retrying
                time.sleep(30)
            log(f"Sleeping for {INTERVAL} seconds...")
            time.sleep(INTERVAL)
    else:
        log(f"### STARTING PIPELINE (Single Run) ###")
        load_select_tables_from_database()
        log(f"### PIPELINE COMPLETED ###")

def cleanup_engines():
    """Clean up engine resources on shutdown."""
    if ENGINE_SOURCE:
        ENGINE_SOURCE.dispose()
    if ENGINE_TARGET:
        ENGINE_TARGET.dispose()

def manual_dlt_state_cleanup():
    """Manually clean up all DLT state tables - use this if you're getting persistent state corruption."""
    try:
        log("🧹 Manual DLT state cleanup initiated...")
        
        with ENGINE_TARGET.connect() as connection:
            # Get all tables in the target database
            inspector = sa.inspect(ENGINE_TARGET)
            all_tables = inspector.get_table_names()
            
            # Find all DLT-related tables
            dlt_tables = [table for table in all_tables if table.startswith('_dlt_') or 'dlt' in table.lower()]
            
            log(f"Found {len(dlt_tables)} DLT-related tables: {dlt_tables}")
            
            # Drop all DLT state tables
            for dlt_table in dlt_tables:
                try:
                    drop_query = f"DROP TABLE IF EXISTS `{dlt_table}`"
                    connection.execute(sa.text(drop_query))
                    connection.commit()
                    log(f"✅ Dropped DLT table: {dlt_table}")
                except Exception as drop_error:
                    log(f"⚠️  Failed to drop table {dlt_table}: {drop_error}")
            
            # Also clean up any dataset-specific DLT tables
            dataset_dlt_tables = [table for table in all_tables if table.startswith(f'{TARGET_DB_NAME}_dlt_')]
            for dataset_table in dataset_dlt_tables:
                try:
                    drop_query = f"DROP TABLE IF EXISTS `{dataset_table}`"
                    connection.execute(sa.text(drop_query))
                    connection.commit()
                    log(f"✅ Dropped dataset DLT table: {dataset_table}")
                except Exception as drop_error:
                    log(f"⚠️  Failed to drop dataset table {dataset_table}: {drop_error}")
            
            log("🎉 Manual DLT state cleanup completed!")
            log("You can now restart the pipeline - it will create fresh state tables.")
            
    except Exception as cleanup_error:
        log(f"❌ Manual DLT state cleanup failed: {cleanup_error}")
        raise

def optimize_incremental_merge(engine_target, table_name, primary_keys):
    """Optimize table for incremental merge operations to prevent connection timeouts.
    
    Args:
        engine_target: Target database engine
        table_name: Name of the table to optimize
        primary_keys: List of primary key columns
    
    Returns:
        bool: True if optimization was successful
    """
    # Check if merge optimization is enabled
    if not MERGE_OPTIMIZATION_ENABLED:
        log(f"ℹ️  Merge optimization disabled for {table_name} (MERGE_OPTIMIZATION_ENABLED=false)")
        return True
    
    def _optimize_table(connection):
        try:
            log(f"🔧 Optimizing table {table_name} for incremental merge operations...")
            
            # Add database hints to optimize DELETE operations
            # These help MySQL choose better execution plans for complex DELETE with EXISTS
            
            # 1. Analyze table to update statistics
            connection.execute(sa.text(f"ANALYZE TABLE {table_name}"))
            log(f"✅ Table statistics updated for {table_name}")
            
            # 2. Add hints for better DELETE performance
            # Set session variables that help with large DELETE operations
            optimization_queries = [
                "SET SESSION sql_buffer_result = ON",
                "SET SESSION join_buffer_size = 8388608",  # 8MB
                "SET SESSION sort_buffer_size = 2097152",  # 2MB
                "SET SESSION read_buffer_size = 1048576",  # 1MB
                "SET SESSION net_read_timeout = 600",  # 10 minutes
                "SET SESSION net_write_timeout = 600",  # 10 minutes
                "SET SESSION innodb_lock_wait_timeout = 120",
                "SET SESSION lock_wait_timeout = 120"
            ]
            
            for query in optimization_queries:
                try:
                    connection.execute(sa.text(query))
                except Exception as opt_error:
                    log(f"⚠️  Could not set optimization: {query} - {opt_error}")
            
            log(f"✅ Session optimizations applied for {table_name}")
            
            # 3. Check if indexes exist on primary keys for better DELETE performance
            inspector = sa.inspect(engine_target)
            indexes = inspector.get_indexes(table_name)
            
            primary_key_set = set(primary_keys)
            existing_indexes = set()
            
            for index in indexes:
                if index.get('unique', False) or index.get('primary_key', False):
                    existing_indexes.update(index['column_names'])
            
            # Log index status
            missing_indexes = primary_key_set - existing_indexes
            if missing_indexes:
                log(f"⚠️  Missing indexes on primary keys: {missing_indexes}")
                log(f"   This may cause slow DELETE operations during incremental merge")
            else:
                log(f"✅ All primary keys have proper indexes: {primary_keys}")
            
            # 4. Set table-specific optimizations
            table_optimizations = [
                f"SET SESSION innodb_lock_wait_timeout = 120",
                f"SET SESSION lock_wait_timeout = 120"
            ]
            
            for query in table_optimizations:
                try:
                    connection.execute(sa.text(query))
                except Exception as table_opt_error:
                    log(f"⚠️  Could not set table optimization: {query} - {table_opt_error}")
            
            log(f"✅ Table {table_name} optimized for incremental merge operations")
            return True
            
        except Exception as optimization_error:
            log(f"❌ Failed to optimize table {table_name}: {optimization_error}")
            return False
    
    return execute_with_transaction_management(
        engine_target,
        f"optimize_incremental_merge for {table_name}",
        _optimize_table
    )

def create_merge_optimized_pipeline(engine_target, pipeline_name):
    """Create a DLT pipeline with optimizations for incremental merge operations."""
    try:
        log(f"Creating merge-optimized DLT pipeline: {pipeline_name}")
        
        # First, clean up any corrupted state
        state_was_corrupted = cleanup_corrupted_dlt_state(engine_target, pipeline_name)
        
        if state_was_corrupted:
            log(f"Corrupted state was cleaned up, creating fresh pipeline")
        
        # Create destination with merge optimizations
        if PRESERVE_COLUMN_NAMES:
            log(f"🔧 Configuring DLT destination with merge optimizations and column name preservation")
            
            destination = dlt.destinations.sqlalchemy(
                engine_target,
                table_name=lambda table: table,
                column_name=lambda column: column,
                normalize_column_name=False,
                # Add merge-specific optimizations
                merge_strategy="merge_using_primary_key",  # Use primary key for merge
                merge_key_columns=lambda table: table_configs.get(table, {}).get('primary_key', 'id').split(',') if isinstance(table_configs.get(table, {}).get('primary_key', 'id'), str) else table_configs.get(table, {}).get('primary_key', ['id']),
                # Optimize for large datasets using configuration
                batch_size=MERGE_BATCH_SIZE,  # Use configured batch size
                max_batch_size=MERGE_MAX_BATCH_SIZE  # Use configured max batch size
            )
            
            log(f"   ✅ Merge-optimized destination created with batch_size={MERGE_BATCH_SIZE}, max_batch_size={MERGE_MAX_BATCH_SIZE}")
        else:
            log(f"⚠️  Using default DLT destination configuration with merge optimizations")
            destination = dlt.destinations.sqlalchemy(
                engine_target,
                merge_strategy="merge_using_primary_key",
                batch_size=MERGE_BATCH_SIZE,
                max_batch_size=MERGE_MAX_BATCH_SIZE
            )
        
        # Create the pipeline with merge optimizations
        pipeline = dlt.pipeline(
            pipeline_name=pipeline_name,
            destination=destination,
            dataset_name=TARGET_DB_NAME,
            dev_mode=False,
            # Add pipeline-level optimizations
            progress="log"  # Log progress for better monitoring
        )
        
        log(f"✅ Merge-optimized pipeline created successfully: {pipeline_name}")
        return pipeline
        
    except Exception as pipeline_error:
        log(f"Error creating merge-optimized pipeline: {pipeline_error}")
        # Fall back to regular pipeline creation
        return create_fresh_pipeline(engine_target, pipeline_name)

def retry_on_connection_loss(func, db_type="unknown", operation_name="operation", *args, **kwargs):
    """Enhanced retry function specifically for connection loss errors (2013) with exponential backoff.
    
    Args:
        func: Function to retry
        db_type: Type of database ('source' or 'target') for better error logging
        operation_name: Name of the operation being retried for better logging
        *args, **kwargs: Arguments to pass to the function
    """
    global ENGINE_SOURCE, ENGINE_TARGET
    
    for attempt in range(CONNECTION_LOSS_RETRIES + 1):  # Use configured retry count
        try:
            return func(*args, **kwargs)
        except sa.exc.OperationalError as e:
            error_str = str(e).lower()
            
            # Check for connection loss errors
            if any(keyword in error_str for keyword in [
                'lost connection to server during query',
                '2013',
                'server has gone away',
                '2006',
                'connection was killed',
                'connection timeout'
            ]):
                if attempt < CONNECTION_LOSS_RETRIES:
                    # Calculate delay with exponential backoff
                    delay = 5 * (2 ** attempt)  # 5s, 10s, 20s
                    
                    log(f"🔌 Connection lost on {db_type} during {operation_name} (attempt {attempt + 1}/{CONNECTION_LOSS_RETRIES + 1})")
                    log(f"   Error: {e}")
                    log(f"   Waiting {delay}s before retry...")
                    
                    time.sleep(delay)
                    
                    # Reset the specific connection that failed
                    try:
                        if db_type == "source" and ENGINE_SOURCE:
                            ENGINE_SOURCE.dispose()
                            ENGINE_SOURCE, _ = create_engines()
                        elif db_type == "target" and ENGINE_TARGET:
                            _, ENGINE_TARGET = create_engines()
                        elif db_type == "source+target":
                            ENGINE_SOURCE.dispose()
                            ENGINE_TARGET.dispose()
                            ENGINE_SOURCE, ENGINE_TARGET = create_engines()
                    except Exception as reset_error:
                        log(f"Warning: Could not reset {db_type} connection: {reset_error}")
                    
                    continue
                else:
                    log(f"❌ Max connection loss retries reached for {operation_name} on {db_type} database")
                    log(f"   Final error: {e}")
                    raise
            else:
                # Re-raise non-connection loss errors immediately
                log(f"Non-connection loss error occurred on {db_type} database: {e}")
                raise
        except Exception as e:
            log(f"Non-connection loss error occurred on {db_type} database: {e}")
            raise

def execute_with_connection_loss_handling(engine, operation_name, operation_func, *args, **kwargs):
    """Execute database operations with connection loss handling for long-running operations.
    
    Args:
        engine: SQLAlchemy engine to use
        operation_name: Name of the operation for logging
        operation_func: Function to execute within transaction
        *args, **kwargs: Arguments to pass to the operation function
    """
    # Use retry logic specifically for connection loss
    return retry_on_connection_loss(
        _execute_transaction,
        "target" if engine == ENGINE_TARGET else "source",
        operation_name,
        engine,
        operation_func,
        *args,
        **kwargs
    )

def create_unique_staging_schema(engine_target, base_prefix="zains_rz_staging"):
    """Create a unique staging schema for this pipeline run to prevent lock conflicts.
    
    Args:
        engine_target: Target database engine
        base_prefix: Base prefix for staging schema names
    
    Returns:
        str: The unique staging schema name created
    """
    def _create_schema(connection):
        # Generate unique schema name with timestamp and random suffix
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        random_suffix = uuid.uuid4().hex[:6]
        schema_name = f"{base_prefix}_{timestamp}_{random_suffix}"
        
        log(f"🔧 Creating unique staging schema: {schema_name}")
        
        # Create the schema
        create_schema_query = f"CREATE SCHEMA IF NOT EXISTS `{schema_name}`"
        connection.execute(sa.text(create_schema_query))
        
        # Verify schema was created
        verify_query = f"SELECT SCHEMA_NAME FROM INFORMATION_SCHEMA.SCHEMATA WHERE SCHEMA_NAME = '{schema_name}'"
        result = connection.execute(sa.text(verify_query))
        if result.fetchone():
            log(f"✅ Staging schema created successfully: {schema_name}")
            return schema_name
        else:
            raise Exception(f"Failed to create staging schema: {schema_name}")
    
    return execute_with_transaction_management(
        engine_target,
        "create_unique_staging_schema",
        _create_schema
    )

def cleanup_old_staging_schemas(engine_target, base_prefix="zains_rz_staging", retention_hours=24):
    """Clean up old staging schemas to prevent database clutter.
    
    Args:
        engine_target: Target database engine
        base_prefix: Base prefix for staging schema names
        retention_hours: How many hours to keep staging schemas
    
    Returns:
        int: Number of schemas cleaned up
    """
    def _cleanup_schemas(connection):
        log(f"🧹 Cleaning up old staging schemas older than {retention_hours} hours...")
        
        # Find all staging schemas
        find_schemas_query = f"""
        SELECT SCHEMA_NAME 
        FROM INFORMATION_SCHEMA.SCHEMATA 
        WHERE SCHEMA_NAME LIKE '{base_prefix}_%'
        """
        result = connection.execute(sa.text(find_schemas_query))
        staging_schemas = [row[0] for row in result.fetchall()]
        
        if not staging_schemas:
            log(f"ℹ️  No old staging schemas found to clean up")
            return 0
        
        log(f"Found {len(staging_schemas)} staging schemas to check")
        
        cleaned_count = 0
        current_time = datetime.now()
        
        for schema_name in staging_schemas:
            try:
                # Extract timestamp from schema name (format: prefix_YYYYMMDD_HHMMSS_random)
                parts = schema_name.split('_')
                if len(parts) >= 3:
                    try:
                        # Parse timestamp from schema name
                        date_str = f"{parts[-3]}_{parts[-2]}"  # YYYYMMDD_HHMMSS
                        schema_time = datetime.strptime(date_str, "%Y%m%d_%H%M%S")
                        
                        # Check if schema is old enough to delete
                        age_hours = (current_time - schema_time).total_seconds() / 3600
                        
                        if age_hours > retention_hours:
                            log(f"🗑️  Dropping old staging schema: {schema_name} (age: {age_hours:.1f} hours)")
                            
                            # Drop the schema and all its tables
                            drop_schema_query = f"DROP SCHEMA IF EXISTS `{schema_name}`"
                            connection.execute(sa.text(drop_schema_query))
                            
                            cleaned_count += 1
                        else:
                            log(f"ℹ️  Keeping staging schema: {schema_name} (age: {age_hours:.1f} hours)")
                    except ValueError:
                        log(f"⚠️  Could not parse timestamp from schema: {schema_name}")
                        # If we can't parse the timestamp, assume it's old and delete it
                        log(f"🗑️  Dropping unparseable staging schema: {schema_name}")
                        drop_schema_query = f"DROP SCHEMA IF EXISTS `{schema_name}`"
                        connection.execute(sa.text(drop_schema_query))
                        cleaned_count += 1
                else:
                    log(f"⚠️  Unexpected staging schema format: {schema_name}")
                    
            except Exception as cleanup_error:
                log(f"⚠️  Error cleaning up schema {schema_name}: {cleanup_error}")
                continue
        
        log(f"✅ Cleanup completed: {cleaned_count} staging schemas removed")
        return cleaned_count
    
    return execute_with_transaction_management(
        engine_target,
        "cleanup_old_staging_schemas",
        _cleanup_schemas
    )

def get_staging_schema_status(engine_target, base_prefix="zains_rz_staging"):
    """Get current status of staging schemas for monitoring.
    
    Args:
        engine_target: Target database engine
        base_prefix: Base prefix for staging schema names
    
    Returns:
        dict: Status information about staging schemas
    """
    def _get_status(connection):
        # Find all staging schemas
        find_schemas_query = f"""
        SELECT SCHEMA_NAME, 
               COUNT(TABLE_NAME) as table_count,
               SUM(TABLE_ROWS) as total_rows
        FROM INFORMATION_SCHEMA.SCHEMATA s
        LEFT JOIN INFORMATION_SCHEMA.TABLES t ON s.SCHEMA_NAME = t.TABLE_SCHEMA
        WHERE s.SCHEMA_NAME LIKE '{base_prefix}_%'
        GROUP BY s.SCHEMA_NAME
        ORDER BY s.SCHEMA_NAME
        """
        
        result = connection.execute(sa.text(find_schemas_query))
        schemas = []
        
        for row in result.fetchall():
            schema_name, table_count, total_rows = row
            table_count = table_count or 0
            total_rows = total_rows or 0
            
            # Extract timestamp for age calculation
            parts = schema_name.split('_')
            age_hours = "unknown"
            if len(parts) >= 3:
                try:
                    date_str = f"{parts[-3]}_{parts[-2]}"
                    schema_time = datetime.strptime(date_str, "%Y%m%d_%H%M%S")
                    age_hours = f"{((datetime.now() - schema_time).total_seconds() / 3600):.1f}"
                except ValueError:
                    age_hours = "unparseable"
            
            schemas.append({
                'name': schema_name,
                'table_count': table_count,
                'total_rows': total_rows,
                'age_hours': age_hours
            })
        
        return {
            'total_schemas': len(schemas),
            'schemas': schemas
        }
    
    return execute_with_transaction_management(
        engine_target,
        "get_staging_schema_status",
        _get_status
    )

class IsolatedStagingDestination:
    """Custom DLT destination that uses isolated staging schemas to prevent lock conflicts."""
    
    def __init__(self, engine_target, staging_schema_name, preserve_column_names=True):
        self.engine_target = engine_target
        self.staging_schema_name = staging_schema_name
        self.preserve_column_names = preserve_column_names
        
        # Create the base destination with proper error handling
        try:
            if preserve_column_names:
                self.base_destination = dlt.destinations.sqlalchemy(
                    engine_target,
                    column_name=lambda column: column,
                    normalize_column_name=False,
                    # Override staging schema
                    staging_schema=staging_schema_name,
                    # Optimize for large datasets
                    batch_size=MERGE_BATCH_SIZE,
                    max_batch_size=MERGE_MAX_BATCH_SIZE
                )
            else:
                self.base_destination = dlt.destinations.sqlalchemy(
                    engine_target,
                    staging_schema=staging_schema_name,
                    batch_size=MERGE_BATCH_SIZE,
                    max_batch_size=MERGE_MAX_BATCH_SIZE
                )
            
            # Validate the destination was created properly
            if not hasattr(self.base_destination, 'capabilities'):
                raise Exception("Base destination missing required capabilities attribute")
            
            log(f"🔧 Created isolated staging destination using schema: {staging_schema_name}")
            log(f"   Destination type: {type(self.base_destination)}")
            log(f"   Has capabilities: {hasattr(self.base_destination, 'capabilities')}")
            
        except Exception as dest_error:
            log(f"❌ Failed to create base destination: {dest_error}")
            raise Exception(f"Destination creation failed: {dest_error}")
    
    def __getattr__(self, name):
        """Delegate all other attributes to the base destination."""
        if not hasattr(self.base_destination, name):
            raise AttributeError(f"'{type(self).__name__}' object has no attribute '{name}'")
        return getattr(self.base_destination, name)
    
    def __call__(self, *args, **kwargs):
        """Make the destination callable like the base destination."""
        if not callable(self.base_destination):
            raise Exception(f"Base destination is not callable: {type(self.base_destination)}")
        return self.base_destination(*args, **kwargs)
    
    def __repr__(self):
        """Provide a clear representation of the destination."""
        return f"IsolatedStagingDestination(schema={self.staging_schema_name}, base={type(self.base_destination)})"

def create_isolated_staging_pipeline(engine_target, pipeline_name, staging_schema_name):
    """Create a DLT pipeline with isolated staging to prevent lock conflicts.
    
    Args:
        engine_target: Target database engine
        pipeline_name: Name of the pipeline
        staging_schema_name: Unique staging schema name to use
    
    Returns:
        dlt.Pipeline: Configured pipeline with isolated staging
    """
    try:
        log(f"Creating isolated staging DLT pipeline: {pipeline_name}")
        log(f"Using staging schema: {staging_schema_name}")
        
        # First, clean up any corrupted state
        state_was_corrupted = cleanup_corrupted_dlt_state(engine_target, pipeline_name)
        
        if state_was_corrupted:
            log(f"Corrupted state was cleaned up, creating fresh pipeline")
        
        # Create destination with isolated staging
        log(f"🔧 Creating isolated staging destination...")
        destination = IsolatedStagingDestination(
            engine_target, 
            staging_schema_name, 
            preserve_column_names=PRESERVE_COLUMN_NAMES
        )
        
        # Validate destination was created properly
        if not destination or not hasattr(destination, 'base_destination'):
            raise Exception("Failed to create valid isolated staging destination")
        
        log(f"✅ Isolated staging destination created successfully")
        
        # Create the pipeline with isolated staging
        log(f"🔧 Creating DLT pipeline with isolated staging...")
        pipeline = dlt.pipeline(
            pipeline_name=pipeline_name,
            destination=destination,
            dataset_name=TARGET_DB_NAME,
            dev_mode=False,
            # Add pipeline-level optimizations
            progress="log"  # Log progress for better monitoring
        )
        
        # Validate pipeline was created
        if not pipeline:
            raise Exception("Failed to create DLT pipeline")
        
        log(f"✅ Isolated staging pipeline created successfully: {pipeline_name}")
        log(f"   Staging schema: {staging_schema_name}")
        log(f"   Pipeline type: {type(pipeline)}")
        
        # Verify that the pipeline is actually using our isolated staging
        log(f"🔍 Verifying pipeline configuration...")
        if hasattr(pipeline, 'destination') and hasattr(pipeline.destination, 'staging_schema_name'):
            actual_staging = pipeline.destination.staging_schema_name
            if actual_staging == staging_schema_name:
                log(f"✅ Pipeline confirmed using isolated staging schema: {actual_staging}")
            else:
                log(f"⚠️  Pipeline staging schema mismatch - Expected: {staging_schema_name}, Got: {actual_staging}")
        else:
            log(f"⚠️  Could not verify pipeline staging schema configuration")
        
        # Enforce staging isolation to prevent any fallback to default staging
        log(f"🔒 Enforcing staging isolation...")
        if not enforce_staging_isolation(pipeline, staging_schema_name):
            log(f"❌ CRITICAL: Failed to enforce staging isolation!")
            log(f"❌ Pipeline will not use isolated staging - this will cause lock timeouts!")
            raise Exception("Staging isolation enforcement failed - cannot proceed without isolation")
        
        log(f"✅ Staging isolation successfully enforced for pipeline: {pipeline_name}")
        return pipeline
        
    except Exception as pipeline_error:
        log(f"❌ Error creating isolated staging pipeline: {pipeline_error}")
        log(f"❌ CRITICAL: Cannot fall back to regular pipeline - this would break staging isolation!")
        log(f"❌ Pipeline creation must succeed with isolated staging to prevent lock timeouts!")
        
        # Instead of falling back, try to diagnose and fix the issue
        log(f"🔍 Diagnosing isolated staging pipeline creation failure...")
        
        # Check if staging schema exists
        try:
            with engine_target.connect() as connection:
                schema_check = f"SELECT SCHEMA_NAME FROM INFORMATION_SCHEMA.SCHEMATA WHERE SCHEMA_NAME = '{staging_schema_name}'"
                result = connection.execute(sa.text(schema_check))
                if result.fetchone():
                    log(f"✅ Staging schema exists: {staging_schema_name}")
                else:
                    log(f"❌ Staging schema does not exist: {staging_schema_name}")
        except Exception as check_error:
            log(f"⚠️  Could not check staging schema: {check_error}")
        
        # Re-raise the error to prevent fallback
        raise Exception(f"Isolated staging pipeline creation failed - cannot proceed without isolation: {pipeline_error}")

def verify_staging_schema_usage(engine_target, expected_staging_schema):
    """Verify that DLT is actually using our isolated staging schema.
    
    Args:
        engine_target: Target database engine
        expected_staging_schema: The staging schema we expect DLT to use
    
    Returns:
        bool: True if DLT is using our isolated staging schema
    """
    def _verify_usage(connection):
        log(f"🔍 Verifying DLT staging schema usage...")
        
        # Check if our expected staging schema exists
        schema_check_query = f"SELECT SCHEMA_NAME FROM INFORMATION_SCHEMA.SCHEMATA WHERE SCHEMA_NAME = '{expected_staging_schema}'"
        result = connection.execute(sa.text(schema_check_query))
        if not result.fetchone():
            log(f"❌ Expected staging schema '{expected_staging_schema}' does not exist!")
            return False
        
        # Check for any tables in the default staging schema (this indicates DLT is not using isolation)
        default_staging_check = f"SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = 'zains_rz_staging'"
        result = connection.execute(sa.text(default_staging_check))
        default_staging_tables = result.scalar()
        
        if default_staging_tables > 0:
            log(f"⚠️  WARNING: Found {default_staging_tables} tables in default staging schema 'zains_rz_staging'")
            log(f"   This suggests DLT is NOT using our isolated staging approach!")
            log(f"   Expected: {expected_staging_schema}")
            log(f"   Actual: zains_rz_staging")
            return False
        
        # Check for tables in our isolated staging schema
        isolated_staging_check = f"SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = '{expected_staging_schema}'"
        result = connection.execute(sa.text(isolated_staging_check))
        isolated_staging_tables = result.scalar()
        
        log(f"✅ DLT is using isolated staging schema '{expected_staging_schema}'")
        log(f"   Tables in isolated schema: {isolated_staging_tables}")
        log(f"   Tables in default schema: 0")
        
        return True
    
    return execute_with_transaction_management(
        engine_target,
        f"verify_staging_schema_usage for {expected_staging_schema}",
        _verify_usage
    )

def periodic_staging_verification(engine_target, expected_staging_schema, operation_name):
    """Periodically verify that DLT is still using our isolated staging schema.
    
    Args:
        engine_target: Target database engine
        expected_staging_schema: The staging schema we expect DLT to use
        operation_name: Name of the operation being verified
    """
    try:
        log(f"🔍 Periodic staging verification for {operation_name}...")
        if verify_staging_schema_usage(engine_target, expected_staging_schema):
            log(f"✅ Staging isolation still working for {operation_name}")
        else:
            log(f"❌ Staging isolation broken for {operation_name} - this may cause lock timeouts!")
    except Exception as verify_error:
        log(f"⚠️  Could not verify staging schema usage for {operation_name}: {verify_error}")

def validate_dlt_staging_usage(pipeline, expected_staging_schema):
    """Validate that DLT is actually using our isolated staging schema during pipeline execution.
    
    Args:
        pipeline: The DLT pipeline to validate
        expected_staging_schema: The staging schema we expect DLT to use
    
    Returns:
        bool: True if DLT is using our isolated staging schema
    """
    try:
        log(f"🔍 Validating DLT staging schema usage for pipeline: {pipeline.pipeline_name}")
        
        # Check pipeline destination configuration
        if not hasattr(pipeline, 'destination'):
            log(f"❌ Pipeline has no destination attribute")
            return False
        
        destination = pipeline.destination
        
        # Check if destination has staging schema information
        if hasattr(destination, 'staging_schema_name'):
            actual_staging = destination.staging_schema_name
            log(f"   Destination staging schema: {actual_staging}")
            
            if actual_staging == expected_staging_schema:
                log(f"✅ Pipeline destination correctly configured with isolated staging")
                return True
            else:
                log(f"❌ Pipeline destination staging schema mismatch")
                log(f"   Expected: {expected_staging_schema}")
                log(f"   Actual: {actual_staging}")
                return False
        else:
            log(f"⚠️  Pipeline destination has no staging_schema_name attribute")
            log(f"   Destination type: {type(destination)}")
            log(f"   Destination attributes: {dir(destination)}")
            
            # Try to check if it's our custom destination
            if hasattr(destination, 'base_destination'):
                log(f"   Base destination type: {type(destination.base_destination)}")
                if hasattr(destination.base_destination, 'staging_schema'):
                    base_staging = destination.base_destination.staging_schema
                    log(f"   Base destination staging schema: {base_staging}")
                    return base_staging == expected_staging_schema
            
            return False
            
    except Exception as validation_error:
        log(f"❌ Error validating DLT staging usage: {validation_error}")
        return False

def enforce_staging_isolation(pipeline, expected_staging_schema):
    """Enforce that DLT uses our isolated staging schema and prevent fallback to default staging.
    
    Args:
        pipeline: The DLT pipeline to enforce isolation on
        expected_staging_schema: The staging schema that must be used
    
    Returns:
        bool: True if isolation is enforced and working
    """
    try:
        log(f"🔒 Enforcing staging isolation for pipeline: {pipeline.pipeline_name}")
        
        # Validate current staging usage
        if not validate_dlt_staging_usage(pipeline, expected_staging_schema):
            log(f"❌ CRITICAL: Pipeline is not using isolated staging schema!")
            log(f"❌ This will cause lock timeouts and defeat the isolation strategy!")
            
            # Try to force the staging schema
            if hasattr(pipeline, 'destination') and hasattr(pipeline.destination, 'base_destination'):
                try:
                    # Force the staging schema on the base destination
                    pipeline.destination.base_destination.staging_schema = expected_staging_schema
                    log(f"🔧 Attempted to force staging schema on base destination")
                    
                    # Re-validate
                    if validate_dlt_staging_usage(pipeline, expected_staging_schema):
                        log(f"✅ Successfully enforced staging isolation")
                        return True
                    else:
                        log(f"❌ Failed to enforce staging isolation after forcing schema")
                        return False
                        
                except Exception as force_error:
                    log(f"❌ Could not force staging schema: {force_error}")
                    return False
            else:
                log(f"❌ Cannot enforce staging isolation - pipeline structure not compatible")
                return False
        
        log(f"✅ Staging isolation is properly enforced")
        return True
        
    except Exception as enforce_error:
        log(f"❌ Error enforcing staging isolation: {enforce_error}")
        return False

if __name__ == "__main__":
    import sys
    
    # Check for command line arguments
    if len(sys.argv) > 1 and sys.argv[1] == "cleanup":
        # Manual cleanup mode
        log("🧹 Running manual DLT state cleanup...")
        try:
            manual_dlt_state_cleanup()
            log("✅ Cleanup completed successfully!")
            log("You can now restart the pipeline normally.")
        except Exception as cleanup_error:
            log(f"❌ Cleanup failed: {cleanup_error}")
        finally:
            cleanup_engines()
        sys.exit(0)
    
    try:
        # Start the HTTP server in a separate thread
        http_thread = threading.Thread(target=run_http_server)
        http_thread.daemon = True
        http_thread.start()

        # Start the pipeline function
        run_pipeline()
    except KeyboardInterrupt:
        log("Shutting down pipeline...")
    finally:
        cleanup_engines()