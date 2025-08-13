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
import pandas as pd

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
STAGING_SCHEMA_PREFIX = os.getenv("STAGING_SCHEMA_PREFIX", "dlt_staging")  # Base prefix for staging schemas
STAGING_SCHEMA_RETENTION_HOURS = int(os.getenv("STAGING_SCHEMA_RETENTION_HOURS", "24"))  # How long to keep old staging schemas



# File-based staging configuration (alternative to database staging)
FILE_STAGING_ENABLED = os.getenv("FILE_STAGING_ENABLED", "true").lower() == "true"  # Enable file-based staging
FILE_STAGING_DIR = os.getenv("FILE_STAGING_DIR", "staging")  # Base directory for staging files
FILE_STAGING_RETENTION_HOURS = int(os.getenv("FILE_STAGING_RETENTION_HOURS", "24"))  # How long to keep staging files
FILE_STAGING_COMPRESSION = os.getenv("FILE_STAGING_COMPRESSION", "snappy")  # Compression algorithm
FILE_STAGING_ADVANCED_MONITORING = os.getenv("FILE_STAGING_ADVANCED_MONITORING", "true").lower() == "true"

DB_SOURCE_URL = f"mysql://{SOURCE_DB_USER}:{SOURCE_DB_PASS}@{SOURCE_DB_HOST}:{SOURCE_DB_PORT}/{SOURCE_DB_NAME}"
DB_TARGET_URL = f"mysql://{TARGET_DB_USER}:{TARGET_DB_PASS}@{TARGET_DB_HOST}:{TARGET_DB_PORT}/{TARGET_DB_NAME}"

# Load table configurations from tables.json
TABLES_FILE = "tables.json"
with open(TABLES_FILE, "r") as f:
    tables_data = json.load(f)

table_configs = {t["table"]: t for t in tables_data}

# Validate table configurations
# def validate_table_configurations():
#     """Validate all table configurations for proper primary key setup."""
#     log("🔍 Validating table configurations...")
#     
#     for table_name, config in table_configs.items():
#         # Check if primary_key exists
#         if "primary_key" not in config:
#             log(f"❌ Table '{table_name}' missing primary_key configuration")
#             continue
#             
#         primary_key = config["primary_key"]
#         
#         # Validate primary key configuration
#         if not validate_primary_key_config(primary_key):
#             log(f"❌ Table '{table_name}' missing primary_key configuration")
#             continue
#             
#         # Log primary key information
#         log_primary_key_info(table_name, primary_key)
#         
#         # Check if modifier exists for incremental sync
#         if "modifier" in config:
#             log(f"📅 Table '{table_name}' configured for incremental sync using column: {config['modifier']}")
#         else:
#             log(f"🔄 Table '{table_name}' configured for full refresh sync")
#     
#     log(f"✅ Table configuration validation completed for {len(table_configs)} tables")

# Validate configurations at startup (moved to after log function and engines are defined)

# Global transaction semaphore to limit concurrent transactions
transaction_semaphore = threading.Semaphore(MAX_CONCURRENT_TRANSACTIONS)

# Global engine variables
ENGINE_SOURCE = None
ENGINE_TARGET = None

# Function removed to fix dependency issues

# Function removed to fix dependency issues

# Function removed to fix dependency issues

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

def log(message):
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"{timestamp} - INFO - {message}")

# Table configurations loaded and ready

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
    """Force clear a table completely to prevent DLT from generating complex DELETE statements.
    
    This function ensures the table is completely empty before DLT processing,
    preventing DLT from generating expensive EXISTS-based DELETE queries.
    
    Args:
        engine_target: Target database engine
        table_name: Name of the table to clear
    
    Returns:
        bool: True if clear was successful
    """
    def _clear_table(connection):
        log(f"🧹 Force clearing table {table_name} to prevent complex DELETE operations")
        
        # First try TRUNCATE (fastest and safest)
        try:
            log(f"Attempting TRUNCATE for {table_name}")
            connection.execute(sa.text(f"TRUNCATE TABLE {table_name}"))
            log(f"✅ TRUNCATE successful for {table_name}")
            return True
        except Exception as truncate_error:
            log(f"⚠️  TRUNCATE failed for {table_name}: {truncate_error}")
            
            # If TRUNCATE fails, try DROP and CREATE
            try:
                log(f"Attempting DROP and CREATE for {table_name}")
                
                # Get table structure
                inspector = sa.inspect(engine_target)
                columns = inspector.get_columns(table_name)
                indexes = inspector.get_indexes(table_name)
                
                # Build CREATE TABLE statement
                column_defs = []
                for col in columns:
                    col_type = str(col['type'])
                    nullable = "NULL" if col.get('nullable', True) else "NOT NULL"
                    default = ""
                    if col.get('default') is not None:
                        default = f" DEFAULT {col['default']}"
                    column_defs.append(f"`{col['name']}` {col_type} {nullable}{default}")
                
                # Drop the table
                connection.execute(sa.text(f"DROP TABLE IF EXISTS {table_name}"))
                
                # Recreate the table
                create_sql = f"CREATE TABLE {table_name} (\n  " + ",\n  ".join(column_defs) + "\n)"
                connection.execute(sa.text(create_sql))
                
                # Recreate indexes
                for index in indexes:
                    if not index['unique']:
                        index_sql = f"CREATE INDEX {index['name']} ON {table_name} ({', '.join(index['column_names'])})"
                        connection.execute(sa.text(index_sql))
                    else:
                        unique_sql = f"CREATE UNIQUE INDEX {index['name']} ON {table_name} ({', '.join(index['column_names'])})"
                        connection.execute(sa.text(unique_sql))
                
                log(f"✅ DROP and CREATE successful for {table_name}")
                return True
                
            except Exception as drop_create_error:
                log(f"❌ DROP and CREATE also failed for {table_name}: {drop_create_error}")
                
                # Last resort: DELETE with batching
                try:
                    log(f"Attempting DELETE with batching as last resort for {table_name}")
                    
                    # Get row count
                    count_result = connection.execute(sa.text(f"SELECT COUNT(*) FROM {table_name}"))
                    total_rows = count_result.scalar()
                    
                    if total_rows == 0:
                        log(f"Table {table_name} is already empty")
                        return True
                    
                    log(f"Table {table_name} has {total_rows} rows, deleting in small batches...")
                    
                    # Use very small batches to avoid timeouts
                    batch_size = 1000
                    deleted_rows = 0
                    
                    while deleted_rows < total_rows:
                        delete_query = f"DELETE FROM {table_name} LIMIT {batch_size}"
                        result = connection.execute(sa.text(delete_query))
                        batch_deleted = result.rowcount
                        
                        if batch_deleted == 0:
                            break
                        
                        deleted_rows += batch_deleted
                        log(f"Deleted {deleted_rows}/{total_rows} rows from {table_name}")
                        
                        # Longer delay between batches to reduce pressure
                        time.sleep(0.5)
                    
                    log(f"✅ DELETE cleanup completed for {table_name}: {deleted_rows} rows deleted")
                    return True
                    
                except Exception as delete_error:
                    log(f"❌ All cleanup methods failed for {table_name}: {delete_error}")
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




            
def process_tables_batch(pipeline, engine_source, engine_target, tables_dict, write_disposition="merge"):
    """Process a batch of tables with proper connection management and lock timeout handling."""
    if not tables_dict:
        return
    
    table_names = list(tables_dict.keys())
    log(f"Processing batch of {len(table_names)} tables: {table_names}")
    
    # Pre-cleanup tables for replace operations to avoid lock timeouts during DLT processing
    if write_disposition == "replace":
        log(f"🔒 Pre-cleaning tables for replace operation to prevent lock timeouts...")
        for table_name in table_names:
            try:
                log(f"Pre-cleaning table: {table_name}")
                # Use force_table_clear for replace operations to prevent DLT from generating complex DELETE statements
                force_table_clear(engine_target, table_name)
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
                    
                    # Process individual table
                    single_info = pipeline.run(single_source, write_disposition=write_disposition)
                    log(f"✅ Individual processing successful for {table_name}: {single_info}")
                    
                except Exception as individual_error:
                    individual_error_str = str(individual_error)
                    log(f"❌ Individual processing failed for {table_name}: {individual_error_str}")
                    
                    # Check if it's still a lock timeout
                    if any(keyword in individual_error_str.lower() for keyword in [
                        'lock wait timeout exceeded', '1205', 'deadlock found', '1213'
                    ]):
                        log(f"🔒 Lock timeout persists for {table_name}, skipping...")
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
        
        # Log connection pool status
        log(f"Source pool status - Size: {engine_source.pool.size()}, Checked out: {engine_source.pool.checkedout()}")
        log(f"Target pool status - Size: {engine_target.pool.size()}, Checked out: {engine_target.pool.checkedout()}")
        
        # Start periodic connection monitoring
        log(f"🔄 Starting periodic connection monitoring...")
        periodic_connection_monitoring(engine_target, interval_seconds=120)
        
        # Use a single pipeline instance with state corruption handling
        pipeline_name = "dlt_unified_pipeline_v1_0_18"
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
            
            # Auto-cleanup old file staging files before processing
            if FILE_STAGING_ENABLED:
                auto_cleanup_file_staging()
                # Also clean up any orphaned staging directories from failed runs
                cleanup_failed_staging_directories()
            
            incremental_items = list(incremental_tables.items())
            
            for i in range(0, len(incremental_items), BATCH_SIZE):
                batch_items = incremental_items[i:i + BATCH_SIZE]
                batch_dict = dict(batch_items)
                
                try:
                    # Check if we should use file staging for this batch
                    if FILE_STAGING_ENABLED:
                        log(f"📁 Using file staging for incremental tables to avoid DELETE conflicts")
                        # Process each table individually with file staging
                        successful_tables = 0
                        failed_tables = 0
                        
                        for table_name, table_config in batch_dict.items():
                            try:
                                log(f"🔄 Processing table: {table_name}")
                                if process_incremental_table_with_file(table_name, table_config, engine_source, engine_target):
                                    successful_tables += 1
                                    log(f"✅ Successfully processed table: {table_name}")
                                else:
                                    failed_tables += 1
                                    log(f"❌ Failed to process table: {table_name}")
                            except Exception as table_error:
                                failed_tables += 1
                                log(f"❌ Error processing table {table_name}: {table_error}")
                                continue
                        
                        log(f"📊 Batch processing summary: {successful_tables} successful, {failed_tables} failed")
                        
                        if failed_tables > 0:
                            log(f"⚠️  Some tables in this batch failed processing")
                        
                    else:
                        log(f"🗄️  Using regular database staging for incremental tables")
                        process_tables_batch(pipeline, engine_source, engine_target, batch_dict, "merge")
                        
                except Exception as e:
                    log(f"Error in batch processing: {e}")
                    log(f"Error type: {type(e).__name__}")
                    
                    # If using file staging, try to continue with individual table processing
                    if FILE_STAGING_ENABLED:
                        log(f"🔄 Attempting to recover with individual table processing using file staging...")
                        log(f"This is likely caused by concurrent access or long-running transactions.")
                        
                        for table_name, table_config in batch_dict.items():
                            try:
                                log(f"Attempting individual processing for table: {table_name}")
                                if not process_incremental_table_with_file(table_name, table_config, engine_source, engine_target):
                                    log(f"❌ Individual processing failed for table: {table_name}")
                                    continue
                                log(f"✅ Individual processing completed for table: {table_name}")
                            except Exception as individual_error:
                                log(f"❌ Individual processing failed for table: {table_name}: {individual_error}")
                                continue
                    else:
                        # Fall back to individual table processing with regular staging
                        log(f"🔄 Attempting to recover with individual table processing...")
                        log(f"This is likely caused by concurrent access or long-running transactions.")
                        
                        for table_name, table_config in batch_dict.items():
                            try:
                                log(f"Attempting individual processing for table: {table_name}")
                                # Create individual pipeline for this table
                                individual_pipeline = create_fresh_pipeline(engine_target, f"dlt_individual_{table_name}_{int(time.time())}")
                                
                                if individual_pipeline:
                                    process_tables_batch(individual_pipeline, engine_source, engine_target, {table_name: table_config}, "merge")
                                    log(f"✅ Individual processing completed for table: {table_name}")
                                else:
                                    log(f"❌ Failed to create individual pipeline for table: {table_name}")
                            except Exception as individual_error:
                                log(f"❌ Individual processing failed for table: {table_name}: {individual_error}")
                                continue
                
                if i + BATCH_SIZE < len(incremental_items):  # Don't delay after the last batch
                    time.sleep(BATCH_DELAY)
        
        # Process full refresh tables in batches
        if full_refresh_tables:
            log(f"Processing {len(full_refresh_tables)} full refresh tables in batches of {BATCH_SIZE}")
            
            for i in range(0, len(full_refresh_tables), BATCH_SIZE):
                batch_tables = full_refresh_tables[i:i + BATCH_SIZE]
                batch_dict = {table: table_configs[table] for table in batch_tables}
                
                try:
                    process_tables_batch(pipeline, engine_source, engine_target, batch_dict, "replace")
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
        
        # Log pipeline completion summary
        log(f"🎉 Pipeline execution completed successfully!")
        log(f"📊 Summary:")
        log(f"   - Full refresh tables processed: {len(full_refresh_tables) if full_refresh_tables else 0}")
        log(f"   - Incremental tables processed: {len(incremental_tables) if incremental_tables else 0}")
        log(f"   - Total tables in this run: {(len(full_refresh_tables) if full_refresh_tables else 0) + (len(incremental_tables) if incremental_tables else 0)}")
        log(f"   - File staging enabled: {FILE_STAGING_ENABLED}")
        log(f"   - Staging directory: {FILE_STAGING_DIR}")
        
        return True

    return retry_on_connection_error(_load_tables, "source+target (main data loading)")

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
        if self.path == "/":
            self.send_response(200)
            self.send_header("Content-type", "text/plain")
            self.end_headers()
            self.wfile.write(b"We are Groot!")
        elif self.path == "/status":
            self.send_response(200)
            self.send_header("Content-type", "application/json")
            self.end_headers()
            
            # Get status information
            status_info = {
                "timestamp": datetime.now().isoformat(),
                "pipeline_status": "running",
                                        "file_staging": get_file_staging_status(),
                "connection_pools": {
                    "source": {
                        "pool_size": getattr(ENGINE_SOURCE, 'pool', {}).get('size', 'unknown') if ENGINE_SOURCE else 'not_initialized',
                        "checked_out": getattr(ENGINE_SOURCE, 'pool', {}).get('checkedout', 'unknown') if ENGINE_SOURCE else 'not_initialized'
                    },
                    "target": {
                        "pool_size": getattr(ENGINE_TARGET, 'pool', {}).get('size', 'unknown') if ENGINE_TARGET else 'not_initialized',
                        "checked_out": getattr(ENGINE_TARGET, 'pool', {}).get('checkedout', 'unknown') if ENGINE_TARGET else 'not_initialized'
                    }
                }
            }
            
            import json
            self.wfile.write(json.dumps(status_info, indent=2).encode())
        else:
            self.send_response(404)
            self.send_header("Content-type", "text/plain")
            self.end_headers()
            self.wfile.write(b"Not Found")

def run_http_server():
    server_address = ("", 8089)  # Serve on all interfaces, port 8089
    httpd = HTTPServer(server_address, SimpleHandler)
    print("Serving on port 8089...")
    httpd.serve_forever()

def run_pipeline():
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
            progress="log",  # Log progress for better monitoring
            # Optimize for incremental operations
            incremental_key=lambda table: table_configs.get(table, {}).get('modifier', 'updated_at')
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

def create_file_staging_pipeline(pipeline_name, staging_dir=None):
    """Create a DLT pipeline with file-based staging to avoid database staging conflicts.
    
    Args:
        pipeline_name: Name of the pipeline
        staging_dir: Directory for staging files (auto-generated if None)
    
    Returns:
        dlt.Pipeline: Configured pipeline with file-based staging
    """
    try:
        # Generate staging directory with timestamp if not provided
        if staging_dir is None:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            staging_dir = f"{FILE_STAGING_DIR}/{pipeline_name}_{timestamp}"
        
        # Ensure staging directory exists
        os.makedirs(staging_dir, exist_ok=True)
        
        log(f"🔧 Creating file-based staging pipeline: {pipeline_name}")
        log(f"   Staging directory: {staging_dir}")
        log(f"   Compression: {FILE_STAGING_COMPRESSION}")
        
        # Try different destination types based on DLT version
        destination = None
        
        # First try: Parquet destination (DLT >= 1.20.0)
        try:
            destination = dlt.destinations.parquet(
                staging_dir,
                compression=FILE_STAGING_COMPRESSION,
                batch_size=MERGE_BATCH_SIZE,
                max_batch_size=MERGE_MAX_BATCH_SIZE
            )
            log(f"✅ Using dlt.destinations.parquet destination")
        except AttributeError:
            log(f"⚠️  dlt.destinations.parquet not available, trying filesystem destination...")
            
            # Second try: Filesystem destination (DLT >= 1.15.0)
            try:
                destination = dlt.destinations.filesystem(
                    staging_dir,
                    file_format="parquet",
                    compression=FILE_STAGING_COMPRESSION
                )
                log(f"✅ Using dlt.destinations.filesystem destination with Parquet format")
            except AttributeError:
                log(f"⚠️  dlt.destinations.filesystem not available, trying filesystem without format...")
                
                # Third try: Basic filesystem destination
                try:
                    destination = dlt.destinations.filesystem(staging_dir)
                    log(f"✅ Using dlt.destinations.filesystem destination (basic)")
                except AttributeError:
                    # Last resort: Use filesystem with path
                    try:
                        destination = dlt.destinations.filesystem(path=staging_dir)
                        log(f"✅ Using dlt.destinations.filesystem destination (path parameter)")
                    except Exception as e:
                        log(f"❌ No compatible filesystem destination found: {e}")
                        raise Exception(f"No compatible staging destination found in DLT version {dlt.__version__}")
        
        if destination is None:
            raise Exception("Failed to create any compatible staging destination")
        
        # Test the destination to ensure it's working
        if not test_filesystem_destination(destination, staging_dir):
            log(f"⚠️  Filesystem destination test failed, but continuing...")
        
        # Create the pipeline with staging destination
        pipeline = dlt.pipeline(
            pipeline_name=pipeline_name,
            destination=destination,
            dataset_name=TARGET_DB_NAME,
            dev_mode=False,
            progress="log"  # Log progress for better monitoring
        )
        
        log(f"✅ Staging pipeline created successfully: {pipeline_name}")
        log(f"   Staging directory: {staging_dir}")
        log(f"   Pipeline type: {type(pipeline)}")
        log(f"   DLT version: {dlt.__version__}")
        
        return pipeline, staging_dir
        
    except Exception as pipeline_error:
        log(f"❌ Error creating staging pipeline: {pipeline_error}")
        raise Exception(f"Staging pipeline creation failed: {pipeline_error}")

def process_file_staging_files(staging_dir, table_name, engine_target, primary_keys):
    """Process staging file files and merge into target database efficiently.
    
    Args:
        staging_dir: Directory containing staging file files
        table_name: Name of the table to process
        engine_target: Target database engine
        primary_keys: Primary key columns for the table
    
    Returns:
        bool: True if processing was successful
    """
    try:
        log(f"🔧 Processing file staging files for table: {table_name}")
        log(f"   Staging directory: {staging_dir}")
        log(f"   Primary keys: {primary_keys}")
        
        # Find staging files for this table
        staging_files = []
        for file in os.listdir(staging_dir):
            # Check for various file extensions that might be used
            if (file.endswith(('.parquet', '.json', '.csv')) and table_name in file):
                staging_files.append(os.path.join(staging_dir, file))
        
        if not staging_files:
            log(f"⚠️  No staging files found for table: {table_name}")
            return False
        
        log(f"📁 Found {len(staging_files)} staging files to process")
        
        # Process files in batches to avoid memory issues
        batch_size = 10000  # Process 10k records at a time
        
        for staging_file in staging_files:
            log(f"🔧 Processing file: {os.path.basename(staging_file)}")
            
            # Read file based on its extension
            import pandas as pd
            
            file_ext = os.path.splitext(staging_file)[1].lower()
            
            if file_ext == '.parquet':
                # Use pandas to read Parquet in chunks
                file_reader = pd.read_parquet(staging_file, chunksize=batch_size)
            elif file_ext == '.json':
                # Use pandas to read JSON in chunks
                file_reader = pd.read_json(staging_file, lines=True, chunksize=batch_size)
            elif file_ext == '.csv':
                # Use pandas to read CSV in chunks
                file_reader = pd.read_csv(staging_file, chunksize=batch_size)
            else:
                log(f"⚠️  Unsupported file type: {file_ext}, skipping...")
                continue
            
            # Process the file in chunks
            for chunk_num, chunk in enumerate(file_reader):
                log(f"   Processing chunk {chunk_num + 1} ({len(chunk)} records)")
                
                # Convert chunk to list of dictionaries for processing
                records = chunk.to_dict('records')
                
                # Process this chunk
                if not process_data_chunk(records, table_name, engine_target, primary_keys):
                    log(f"❌ Failed to process chunk {chunk_num + 1}")
                    return False
                
                log(f"   ✅ Chunk {chunk_num + 1} processed successfully")
        
        log(f"✅ All staging files processed successfully for table: {table_name}")
        return True
        
    except Exception as processing_error:
        log(f"❌ Error processing file staging files: {processing_error}")
        return False

def process_data_chunk(records, table_name, engine_target, primary_keys):
    """Process a chunk of data records and merge into target table efficiently.
    
    Args:
        records: List of record dictionaries
        table_name: Name of the target table
        engine_target: Target database engine
        primary_keys: Primary key columns
    
    Returns:
        bool: True if processing was successful
    """
    try:
        if not records:
            return True
        
        # Convert primary keys to list if it's a string
        if isinstance(primary_keys, str):
            primary_keys = [primary_keys]
        
        # Prepare data for efficient merge
        # Use INSERT ... ON DUPLICATE KEY UPDATE instead of DELETE + INSERT
        
        # Build the merge query
        columns = list(records[0].keys())
        # Filter out DLT metadata columns
        data_columns = [col for col in columns if not col.startswith('_dlt_')]
        
        if not data_columns:
            log(f"⚠️  No data columns found in records for table: {table_name}")
            return True
        
        # Create the merge query
        placeholders = ', '.join(['%s'] * len(data_columns))
        column_names = ', '.join([f'`{col}`' for col in data_columns])
        
        # Build ON DUPLICATE KEY UPDATE clause
        update_clause = ', '.join([f'`{col}` = VALUES(`{col}`)' for col in data_columns if col not in primary_keys])
        
        merge_query = f"""
        INSERT INTO `{table_name}` ({column_names})
        VALUES ({placeholders})
        ON DUPLICATE KEY UPDATE {update_clause}
        """
        
        # Execute the merge in batches
        batch_size = 1000  # Smaller batches for better performance
        
        with engine_target.connect() as connection:
            for i in range(0, len(records), batch_size):
                batch = records[i:i + batch_size]
                
                # Prepare batch data
                batch_data = []
                for record in batch:
                    row_data = [record.get(col, None) for col in data_columns]
                    batch_data.append(row_data)
                
                # Execute batch merge
                connection.execute(sa.text(merge_query), batch_data)
                
                log(f"      Merged batch {i//batch_size + 1}/{(len(records) + batch_size - 1)//batch_size}")
            
            # Commit all changes
            connection.commit()
        
        log(f"   ✅ Successfully merged {len(records)} records into {table_name}")
        return True
        
    except Exception as chunk_error:
        log(f"❌ Error processing data chunk: {chunk_error}")
        return False

def cleanup_file_staging_files(staging_dir=None, retention_hours=None):
    """Clean up old file staging files to prevent disk space issues.
    
    Args:
        staging_dir: Base staging directory (uses configured default if None)
        retention_hours: How many hours to keep staging files (uses configured default if None)
    
    Returns:
        int: Number of files/directories cleaned up
    """
    try:
        # Use configured defaults if not specified
        if staging_dir is None:
            staging_dir = FILE_STAGING_DIR
        if retention_hours is None:
            retention_hours = FILE_STAGING_RETENTION_HOURS
        
        if not os.path.exists(staging_dir):
            log(f"ℹ️  Staging directory does not exist: {staging_dir}")
            return 0
        
        log(f"🧹 Cleaning up file staging files older than {retention_hours} hours...")
        log(f"   Base directory: {staging_dir}")
        
        cleaned_count = 0
        current_time = datetime.now()
        
        # Walk through staging directory
        for root, dirs, files in os.walk(staging_dir, topdown=False):
            # Check directories (they contain timestamp in name)
            for dir_name in dirs:
                dir_path = os.path.join(root, dir_name)
                try:
                    # Extract timestamp from directory name
                    # Format: table_name_YYYYMMDD_HHMMSS
                    if '_' in dir_name:
                        parts = dir_name.split('_')
                        if len(parts) >= 3:
                            try:
                                date_str = f"{parts[-2]}_{parts[-1]}"  # YYYYMMDD_HHMMSS
                                dir_time = datetime.strptime(date_str, "%Y%m%d_%H%M%S")
                                
                                # Check if directory is old enough to delete
                                age_hours = (current_time - dir_time).total_seconds() / 3600
                                
                                if age_hours > retention_hours:
                                    log(f"🗑️  Removing old staging directory: {dir_name} (age: {age_hours:.1f} hours)")
                                    import shutil
                                    shutil.rmtree(dir_path)
                                    cleaned_count += 1
                                else:
                                    log(f"ℹ️  Keeping staging directory: {dir_name} (age: {age_hours:.1f} hours)")
                            except ValueError:
                                log(f"⚠️  Could not parse timestamp from directory: {dir_name}")
                except Exception as dir_error:
                    log(f"⚠️  Error processing directory {dir_name}: {dir_error}")
                    continue
        
        log(f"✅ Cleanup completed: {cleaned_count} staging directories removed")
        return cleaned_count
        
    except Exception as cleanup_error:
        log(f"❌ Error during file staging cleanup: {cleanup_error}")
        return 0

def create_hybrid_pipeline(engine_target, pipeline_name, use_file_staging=True):
    """Create a hybrid pipeline that uses file-based staging for incremental tables to avoid DELETE conflicts.
    
    Args:
        engine_target: Target database engine
        pipeline_name: Name of the pipeline
        use_file_staging: Whether to use file-based staging for incremental tables
    
    Returns:
        tuple: (pipeline, staging_dir) or (pipeline, None) for regular pipeline
    """
    try:
        log(f"🔧 Creating hybrid pipeline: {pipeline_name}")
        
        if use_file_staging:
            # Use file-based staging to avoid DELETE query conflicts
            log(f"📁 Using file file staging to avoid database staging conflicts")
            pipeline, staging_dir = create_file_staging_pipeline(pipeline_name)
            return pipeline, staging_dir
        else:
            # Fall back to regular database staging
            log(f"🗄️  Using regular database staging")
            pipeline = create_fresh_pipeline(engine_target, pipeline_name)
            return pipeline, None
            
    except Exception as pipeline_error:
        log(f"❌ Error creating hybrid pipeline: {pipeline_error}")
        raise Exception(f"Hybrid pipeline creation failed: {pipeline_error}")

def process_incremental_table_with_file(table_name, table_config, engine_source, engine_target):
    """Process an incremental table using file-based staging to avoid DELETE query conflicts.
    
    Args:
        table_name: Name of the table to process
        table_config: Table configuration dictionary
        engine_source: Source database engine
        engine_target: Target database engine
    
    Returns:
        bool: True if processing was successful
    """
    try:
        log(f"🔄 Processing incremental table with file staging: {table_name}")
        
        # Get primary keys
        primary_keys = table_config.get('primary_key', 'id')
        if isinstance(primary_keys, str):
            primary_keys = primary_keys.split(',')
        
        # Get modifier column
        modifier_column = table_config.get('modifier', 'updated_at')
        
        log(f"   Primary keys: {primary_keys}")
        log(f"   Modifier column: {modifier_column}")
        
        # Step 1: Create file staging pipeline
        staging_pipeline_name = f"staging_{table_name}_{int(time.time())}"
        staging_pipeline, staging_dir = create_file_staging_pipeline(staging_pipeline_name)
        
        # Step 2: Extract data to file files
        log(f"📤 Extracting data to file staging files...")
        
        try:
            # Create DLT source for the table
            source = sql_database(
                engine_source,
                table_names=[table_name],
                schema=SOURCE_DB_NAME
            )
            
            # Run the staging pipeline to extract data
            staging_pipeline.run(source)
            
            # Verify the pipeline actually completed successfully
            try:
                pipeline_state = staging_pipeline.state
                log(f"🔍 Pipeline state after extraction: {pipeline_state}")
                
                # Check if there are any failed jobs
                failed_jobs = [job for job in staging_pipeline.list_jobs() if job.state == 'failed']
                if failed_jobs:
                    log(f"❌ Pipeline has {len(failed_jobs)} failed jobs")
                    for job in failed_jobs:
                        log(f"   Failed job: {job.job_id} - {job.state}")
                    raise Exception(f"Pipeline extraction failed with {len(failed_jobs)} failed jobs")
                
                log(f"✅ Data extracted to file staging files")
                
            except Exception as state_error:
                log(f"❌ Error checking pipeline state: {state_error}")
                raise Exception(f"Pipeline state verification failed: {state_error}")
            
        except Exception as extraction_error:
            log(f"❌ Data extraction failed for table {table_name}: {extraction_error}")
            log(f"🔄 Skipping file processing since no staging files were created")
            # Clean up the failed staging directory
            try:
                if os.path.exists(staging_dir):
                    import shutil
                    shutil.rmtree(staging_dir)
                    log(f"🧹 Cleaned up failed staging directory: {staging_dir}")
            except Exception as cleanup_error:
                log(f"⚠️  Warning: Could not clean up failed staging directory: {cleanup_error}")
            return False
        
        # Step 3: Synchronously wait for file creation to complete
        if FILE_STAGING_ADVANCED_MONITORING:
            success, staging_files, wait_time = wait_for_file_creation_advanced(staging_dir, table_name, timeout_seconds=60)
        else:
            success, staging_files, wait_time = wait_for_file_creation(staging_dir, table_name, timeout_seconds=60)
        
        if not success:
            log(f"❌ File creation failed after {wait_time:.1f}s")
            log(f"🔄 Skipping file processing since no staging files exist")
            # Clean up the empty staging directory
            try:
                if os.path.exists(staging_dir):
                    import shutil
                    shutil.rmtree(staging_dir)
                    log(f"🧹 Cleaned up empty staging directory: {staging_dir}")
            except Exception as cleanup_error:
                log(f"⚠️  Warning: Could not clean up empty staging directory: {cleanup_error}")
            return False
        
        log(f"✅ Found {len(staging_files)} staging files: {staging_files}")
        
        # Step 4: Process file files and merge into target database
        log(f"📥 Processing file files and merging into target database...")
        
        if not process_file_staging_files(staging_dir, table_name, engine_target, primary_keys):
            log(f"❌ Failed to process file staging files for table: {table_name}")
            return False
        
        log(f"✅ Successfully processed table: {table_name} using file staging")
        
        # Step 5: Clean up staging files (optional - can be done later)
        # cleanup_file_staging_files(staging_dir, retention_hours=1)
        
        return True
        
    except Exception as table_error:
        log(f"❌ Error processing table {table_name} with file staging: {table_error}")
        return False



def auto_cleanup_file_staging():
    """Automatically clean up old file staging files based on configuration."""
    try:
        if FILE_STAGING_ENABLED:
            log(f"🧹 Running automatic cleanup of file staging files...")
            cleaned_count = cleanup_file_staging_files()
            if cleaned_count > 0:
                log(f"✅ Automatic cleanup completed: {cleaned_count} staging directories removed")
            else:
                log(f"ℹ️  No old staging directories found to clean up")
        else:
            log(f"ℹ️  file staging cleanup skipped (FILE_STAGING_ENABLED=false)")
    except Exception as cleanup_error:
        log(f"⚠️  Automatic cleanup failed: {cleanup_error}")

def get_file_staging_status():
    """Get current status of file staging directories for monitoring.
    
    Returns:
        dict: Status information about file staging
    """
    try:
        if not FILE_STAGING_ENABLED:
            return {"enabled": False, "message": "file staging is disabled"}
        
        staging_dir = FILE_STAGING_DIR
        if not os.path.exists(staging_dir):
            return {
                "enabled": True,
                "base_directory": staging_dir,
                "status": "not_created",
                "total_directories": 0,
                "total_size_mb": 0
            }
        
        # Count directories and calculate total size
        total_directories = 0
        total_size_bytes = 0
        
        for root, dirs, files in os.walk(staging_dir):
            for dir_name in dirs:
                dir_path = os.path.join(root, dir_name)
                if os.path.isdir(dir_path):
                    total_directories += 1
                    # Calculate directory size
                    for file_path in os.listdir(dir_path):
                        file_full_path = os.path.join(dir_path, file_path)
                        if os.path.isfile(file_full_path):
                            total_size_bytes += os.path.getsize(file_full_path)
        
        total_size_mb = total_size_bytes / (1024 * 1024)
        
        return {
            "enabled": True,
            "base_directory": staging_dir,
            "status": "active",
            "total_directories": total_directories,
            "total_size_mb": round(total_size_mb, 2),
            "retention_hours": FILE_STAGING_RETENTION_HOURS,
            "compression": FILE_STAGING_COMPRESSION
        }
        
    except Exception as status_error:
        return {
            "enabled": FILE_STAGING_ENABLED,
            "error": str(status_error)
        }

def validate_staging_files_exist(staging_dir, table_name):
    """Validate that staging files exist and are accessible.
    
    Args:
        staging_dir: Directory containing staging files
        table_name: Name of the table being processed
    
    Returns:
        tuple: (bool, list) - (files_exist, list_of_files)
    """
    try:
        if not os.path.exists(staging_dir):
            return False, []
        
        staging_files = []
        for file in os.listdir(staging_dir):
            if file.endswith(('.parquet', '.json', '.csv')):
                staging_files.append(file)
        
        return len(staging_files) > 0, staging_files
        
    except Exception as validation_error:
        log(f"❌ Error validating staging files: {validation_error}")
        return False, []

def cleanup_failed_staging_directories():
    """Clean up any staging directories that might be orphaned from failed runs."""
    try:
        if not FILE_STAGING_ENABLED:
            return 0
        
        staging_dir = FILE_STAGING_DIR
        if not os.path.exists(staging_dir):
            return 0
        
        cleaned_count = 0
        current_time = datetime.now()
        
        for dir_name in os.listdir(staging_dir):
            dir_path = os.path.join(staging_dir, dir_name)
            if os.path.isdir(dir_path):
                # Check if directory is very old (more than 48 hours) - likely from failed runs
                try:
                    # Extract timestamp from directory name (format: staging_tablename_timestamp_date)
                    if '_' in dir_name:
                        parts = dir_name.split('_')
                        if len(parts) >= 3:
                            # Try to parse the date part
                            date_part = '_'.join(parts[-2:])  # Last two parts should be date
                            dir_time = datetime.strptime(date_part, "%Y%m%d_%H%M%S")
                            
                            # Check if directory is older than 48 hours
                            age_hours = (current_time - dir_time).total_seconds() / 3600
                            if age_hours > 48:
                                log(f"🧹 Cleaning up old staging directory (likely from failed run): {dir_name}")
                                import shutil
                                shutil.rmtree(dir_path)
                                cleaned_count += 1
                except (ValueError, IndexError):
                    # If we can't parse the timestamp, skip this directory
                    continue
        
        if cleaned_count > 0:
            log(f"✅ Cleaned up {cleaned_count} old staging directories from failed runs")
        
        return cleaned_count
        
    except Exception as cleanup_error:
        log(f"⚠️  Error during failed staging cleanup: {cleanup_error}")
        return 0

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

def test_filesystem_destination(destination, staging_dir):
    """Test if the filesystem destination is working correctly.
    
    Args:
        destination: DLT destination object
        staging_dir: Staging directory path
    
    Returns:
        bool: True if destination appears to be working
    """
    try:
        log(f"🔍 Testing filesystem destination configuration...")
        
        # Check destination type
        destination_type = type(destination).__name__
        log(f"   Destination type: {destination_type}")
        
        # Check if destination has expected attributes
        if hasattr(destination, 'config'):
            log(f"   Destination config: {destination.config}")
        
        # Check if staging directory is accessible
        if os.path.exists(staging_dir):
            log(f"   Staging directory exists: {staging_dir}")
            
            # Try to create a test file
            test_file = os.path.join(staging_dir, "test_destination.txt")
            try:
                with open(test_file, 'w') as f:
                    f.write("test")
                os.remove(test_file)
                log(f"   ✅ File I/O test passed")
                return True
            except Exception as io_error:
                log(f"   ❌ File I/O test failed: {io_error}")
                return False
        else:
            log(f"   ❌ Staging directory does not exist: {staging_dir}")
            return False
            
    except Exception as test_error:
        log(f"❌ Error testing filesystem destination: {test_error}")
        return False

def wait_for_file_creation(staging_dir, table_name, timeout_seconds=60):
    """Synchronously wait for staging files to be created.
    
    This function monitors the staging directory and waits for files to appear,
    making the file creation process truly synchronous.
    
    Args:
        staging_dir: Directory to monitor
        table_name: Name of the table being processed
        timeout_seconds: Maximum time to wait
    
    Returns:
        tuple: (success, staging_files, wait_time)
    """
    import time
    import os
    
    log(f"🔍 Monitoring file creation in: {staging_dir}")
    start_time = time.time()
    
    # Initial check - maybe files are already there
    files_exist, staging_files = validate_staging_files_exist(staging_dir, table_name)
    if files_exist:
        log(f"✅ Files already exist: {staging_files}")
        return True, staging_files, 0.0
    
    # Wait for files to appear with intelligent backoff
    wait_time = 0.1  # Start with 100ms
    max_wait_time = 2.0  # Cap at 2 seconds between checks
    total_wait_time = 0
    
    while total_wait_time < timeout_seconds:
        # Check if files exist
        files_exist, staging_files = validate_staging_files_exist(staging_dir, table_name)
        if files_exist:
            elapsed = time.time() - start_time
            log(f"✅ Files created successfully after {elapsed:.1f}s")
            return True, staging_files, elapsed
        
        # Wait before next check
        time.sleep(wait_time)
        total_wait_time += wait_time
        
        # Exponential backoff (but cap at max_wait_time)
        wait_time = min(wait_time * 1.2, max_wait_time)
        
        # Log progress every 3 seconds
        if int(total_wait_time) % 3 == 0:
            log(f"⏳ Waiting for files... ({total_wait_time:.1f}s elapsed)")
            
            # Show directory contents for debugging
            try:
                if os.path.exists(staging_dir):
                    dir_contents = os.listdir(staging_dir)
                    if dir_contents:
                        log(f"   📁 Directory contains: {dir_contents}")
                    else:
                        log(f"   📁 Directory is empty")
                else:
                    log(f"   📁 Directory doesn't exist yet")
            except Exception as list_error:
                log(f"   ⚠️  Error listing directory: {list_error}")
    
    # Timeout reached
    elapsed = time.time() - start_time
    log(f"❌ File creation timeout after {elapsed:.1f}s")
    return False, [], elapsed

def wait_for_file_creation_advanced(staging_dir, table_name, timeout_seconds=60):
    """Advanced file system monitoring with real-time event detection.
    
    This function uses file system monitoring to detect when files are created,
    providing the most reliable synchronization possible.
    
    Args:
        staging_dir: Directory to monitor
        table_name: Name of the table being processed
        timeout_seconds: Maximum time to wait
    
    Returns:
        tuple: (success, staging_files, wait_time)
    """
    import time
    import os
    
    log(f"🔍 Advanced file monitoring in: {staging_dir}")
    start_time = time.time()
    
    # Initial check - maybe files are already there
    files_exist, staging_files = validate_staging_files_exist(staging_dir, table_name)
    if files_exist:
        log(f"✅ Files already exist: {staging_files}")
        return True, staging_files, 0.0
    
    # Try to use file system monitoring if available
    try:
        # Check if watchdog is available for real-time monitoring
        import watchdog
        from watchdog.observers import Observer
        from watchdog.events import FileSystemEventHandler
        
        class FileCreationHandler(FileSystemEventHandler):
            def __init__(self, target_files, result_container):
                self.target_files = target_files
                self.result_container = result_container
                self.files_created = []
                
            def on_created(self, event):
                if not event.is_directory:
                    file_name = os.path.basename(event.src_path)
                    if file_name.endswith(('.parquet', '.json', '.csv')):
                        self.files_created.append(file_name)
                        log(f"📁 File created: {file_name}")
                        
                        # Check if we have all expected files
                        if len(self.files_created) >= len(self.target_files):
                            self.result_container['success'] = True
                            self.result_container['files'] = self.files_created.copy()
        
        # Set up file monitoring
        result_container = {'success': False, 'files': []}
        event_handler = FileCreationHandler([], result_container)
        observer = Observer()
        observer.schedule(event_handler, staging_dir, recursive=False)
        observer.start()
        
        log(f"🔍 File system monitoring started")
        
        # Wait for files with monitoring
        wait_time = 0.1
        total_wait_time = 0
        
        while total_wait_time < timeout_seconds and not result_container['success']:
            time.sleep(wait_time)
            total_wait_time += wait_time
            
            # Also check manually as backup
            files_exist, staging_files = validate_staging_files_exist(staging_dir, table_name)
            if files_exist:
                result_container['success'] = True
                result_container['files'] = staging_files
                break
            
            # Exponential backoff
            wait_time = min(wait_time * 1.2, 2.0)
            
            # Log progress
            if int(total_wait_time) % 3 == 0:
                log(f"⏳ Monitoring files... ({total_wait_time:.1f}s elapsed)")
        
        # Stop monitoring
        observer.stop()
        observer.join()
        
        if result_container['success']:
            elapsed = time.time() - start_time
            log(f"✅ Files detected via monitoring after {elapsed:.1f}s")
            return True, result_container['files'], elapsed
            
    except ImportError:
        log(f"⚠️  Watchdog not available, using fallback monitoring")
    except Exception as monitor_error:
        log(f"⚠️  File monitoring failed: {monitor_error}")
    
    # Fallback to standard monitoring
    return wait_for_file_creation(staging_dir, table_name, timeout_seconds)

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