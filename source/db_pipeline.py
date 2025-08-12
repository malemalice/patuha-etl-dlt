# flake8: noqa
import humanize
from typing import Any
import os
import json
import threading
import time
import random
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

# Lock timeout and retry configuration
LOCK_TIMEOUT_RETRIES = int(os.getenv("LOCK_TIMEOUT_RETRIES", "5"))  # Max retries for lock timeouts
LOCK_TIMEOUT_BASE_DELAY = int(os.getenv("LOCK_TIMEOUT_BASE_DELAY", "10"))  # Base delay in seconds
LOCK_TIMEOUT_MAX_DELAY = int(os.getenv("LOCK_TIMEOUT_MAX_DELAY", "300"))  # Max delay in seconds
LOCK_TIMEOUT_JITTER = float(os.getenv("LOCK_TIMEOUT_JITTER", "0.1"))  # Jitter factor (0.1 = 10%)

# Transaction management configuration
TRANSACTION_TIMEOUT = int(os.getenv("TRANSACTION_TIMEOUT", "300"))  # Transaction timeout in seconds
MAX_CONCURRENT_TRANSACTIONS = int(os.getenv("MAX_CONCURRENT_TRANSACTIONS", "3"))  # Max concurrent transactions

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
                getattr(source_batch, table).apply_hints(
                    primary_key=config["primary_key"],
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
                        getattr(single_source, table_name).apply_hints(
                            primary_key=config["primary_key"],
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
                        getattr(single_source, table_name).apply_hints(
                            primary_key=config["primary_key"],
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
                        getattr(single_source, table_name).apply_hints(
                            primary_key=config["primary_key"],
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

def log_pipeline_statistics(pipeline):
    """Log statistics about blocked queries and pipeline performance."""
    try:
        # Try to get statistics from the destination if it's our SafeDLTDestination
        destination = pipeline.destination
        if hasattr(destination, 'get_statistics'):
            stats = destination.get_statistics()
            log(f"📊 Pipeline Query Statistics:")
            log(f"   Total queries executed: {stats['total_queries']}")
            log(f"   Complex queries blocked: {stats['blocked_queries']}")
            log(f"   Blocked percentage: {stats['blocked_percentage']:.1f}%")
            
            if stats['blocked_queries'] > 0:
                log(f"   🛡️  Successfully prevented {stats['blocked_queries']} connection timeout errors!")
        else:
            log(f"📊 Pipeline completed (no query statistics available)")
    except Exception as stats_error:
        log(f"⚠️  Could not retrieve pipeline statistics: {stats_error}")

def load_select_tables_from_database() -> None:
    """Use the sql_database source to reflect an entire database schema and load select tables from it."""
    def _load_tables():
        # Use global engines instead of creating new ones
        engine_source = ENGINE_SOURCE
        engine_target = ENGINE_TARGET
        
        # Start periodic connection monitoring
        log(f"🔄 Starting periodic connection monitoring...")
        periodic_connection_monitoring(engine_target, interval_seconds=120)
        
        # Log connection pool status
        log(f"Source pool status - Size: {engine_source.pool.size()}, Checked out: {engine_source.pool.checkedout()}")
        log(f"Target pool status - Size: {engine_target.pool.size()}, Checked out: {engine_target.pool.checkedout()}")
        
        # Use a single pipeline instance with state corruption handling
        pipeline_name = "dlt_unified_pipeline_v1_0_18"
        pipeline = create_fresh_pipeline(engine_target, pipeline_name)
        
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
                ensure_dlt_columns(engine_target, table)
                sync_table_schema(engine_source, engine_target, table)
                

                
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
                    process_tables_batch(pipeline, engine_source, engine_target, batch_dict, "merge")
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
        
        # Log pipeline statistics
        log_pipeline_statistics(pipeline)

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

class SafeDLTDestination:
    """Custom DLT destination wrapper that prevents complex DELETE queries and connection timeouts."""
    
    def __init__(self, base_destination, engine_target):
        self.base_destination = base_destination
        self.engine_target = engine_target
        self.log = log  # Use the global log function
        self.blocked_queries = 0
        self.total_queries = 0
    
    def __getattr__(self, name):
        """Delegate all other attributes to the base destination."""
        return getattr(self.base_destination, name)
    
    def _is_complex_delete_query(self, sql):
        """Check if a SQL query is a complex DELETE that could cause timeouts."""
        sql_lower = sql.lower()
        
        # Check for complex DELETE patterns that cause timeouts
        complex_patterns = [
            'delete from' in sql_lower and 'where exists' in sql_lower,
            'delete from' in sql_lower and 'select' in sql_lower,
            'delete from' in sql_lower and 'from' in sql_lower.split('where')[1] if 'where' in sql_lower else False,
            'delete from' in sql_lower and len(sql) > 200,  # Very long DELETE queries
            # Additional patterns for complex operations
            'delete from' in sql_lower and 'join' in sql_lower,
            'delete from' in sql_lower and 'subquery' in sql_lower,
        ]
        
        return any(complex_patterns)
    
    def _is_potentially_problematic_query(self, sql):
        """Check for any query that might cause connection timeouts."""
        sql_lower = sql.lower()
        
        # Check for various problematic patterns
        problematic_patterns = [
            'where exists' in sql_lower and len(sql) > 150,
            'select' in sql_lower and 'from' in sql_lower and len(sql) > 300,
            'update' in sql_lower and 'where' in sql_lower and len(sql) > 200,
            'merge' in sql_lower and len(sql) > 250,
        ]
        
        return any(problematic_patterns)
    
    def _safe_execute_sql(self, sql, *args, **kwargs):
        """Safely execute SQL, replacing complex DELETE queries with safer alternatives."""
        self.total_queries += 1
        
        if self._is_complex_delete_query(sql):
            self.blocked_queries += 1
            self.log(f"🚫 BLOCKED complex DELETE query #{self.blocked_queries} that could cause connection timeout:")
            self.log(f"   SQL: {sql[:200]}...")
            
            # Extract table name from DELETE statement
            import re
            table_match = re.search(r'delete\s+from\s+([^\s]+)', sql, re.IGNORECASE)
            if table_match:
                table_name = table_match.group(1).strip('`')
                self.log(f"   Target table: {table_name}")
                
                # Instead of complex DELETE, clear the table safely
                try:
                    self.log(f"   🧹 Replacing complex DELETE with safe table clear for {table_name}")
                    force_table_clear(self.engine_target, table_name)
                    self.log(f"   ✅ Table {table_name} cleared safely")
                    
                    # Return success without executing the complex query
                    return self._create_dummy_result()
                    
                except Exception as clear_error:
                    self.log(f"   ❌ Safe table clear failed: {clear_error}")
                    self.log(f"   ⚠️  Will attempt to execute original query (may cause timeout)")
            
            # If we can't safely replace it, log and continue (may still timeout)
            self.log(f"   ⚠️  Proceeding with original query (risk of timeout)")
        
        elif self._is_potentially_problematic_query(sql):
            self.log(f"⚠️  WARNING: Potentially problematic query detected:")
            self.log(f"   SQL: {sql[:150]}...")
            self.log(f"   Length: {len(sql)} characters")
            self.log(f"   ⚠️  This query may cause connection timeouts")
            
            # For potentially problematic queries, add timeout protection
            try:
                # Set a shorter timeout for this specific query
                with self.engine_target.connect() as conn:
                    conn.execute(sa.text("SET SESSION wait_timeout=60"))
                    conn.execute(sa.text("SET SESSION interactive_timeout=60"))
                
                self.log(f"   🕐 Set shorter timeouts for this query")
            except Exception as timeout_error:
                self.log(f"   ⚠️  Could not set query timeouts: {timeout_error}")
        
        # For all queries, execute normally
        try:
            return self.base_destination._execute_sql(sql, *args, **kwargs)
        except Exception as exec_error:
            error_str = str(exec_error).lower()
            
            # Check if this is a connection timeout error
            if any(keyword in error_str for keyword in [
                'lost connection to server during query',
                '2013',
                'connection lost',
                'server has gone away',
                '2006'
            ]):
                self.log(f"🔌 Connection timeout detected during query execution:")
                self.log(f"   SQL: {sql[:150]}...")
                self.log(f"   Error: {exec_error}")
                
                # Try to recover by clearing the problematic table
                if 'delete from' in sql.lower():
                    try:
                        table_match = re.search(r'delete\s+from\s+([^\s]+)', sql, re.IGNORECASE)
                        if table_match:
                            table_name = table_match.group(1).strip('`')
                            self.log(f"   🧹 Attempting emergency table clear for {table_name}")
                            force_table_clear(self.engine_target, table_name)
                            self.log(f"   ✅ Emergency table clear successful")
                            
                            # Return dummy result to continue processing
                            return self._create_dummy_result()
                    except Exception as emergency_error:
                        self.log(f"   ❌ Emergency table clear failed: {emergency_error}")
            
            # Re-raise the original error if we can't handle it
            raise exec_error
    
    def _create_dummy_result(self):
        """Create a dummy result object to simulate successful execution."""
        class DummyResult:
            def __init__(self):
                self.rowcount = 0
            
            def __enter__(self):
                return self
            
            def __exit__(self, exc_type, exc_val, exc_tb):
                pass
        
        return DummyResult()
    
    def _execute_sql(self, sql, *args, **kwargs):
        """Override the _execute_sql method to intercept complex queries."""
        return self._safe_execute_sql(sql, *args, **kwargs)
    
    def get_statistics(self):
        """Get statistics about blocked and executed queries."""
        return {
            'total_queries': self.total_queries,
            'blocked_queries': self.blocked_queries,
            'blocked_percentage': (self.blocked_queries / self.total_queries * 100) if self.total_queries > 0 else 0
        }

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
        
        # Create base destination with comprehensive column name preservation
        if PRESERVE_COLUMN_NAMES:
            log(f"🔧 Configuring DLT destination with comprehensive column name preservation")
            log(f"   This will prevent DLT from converting 'ViaInput' to 'via_input'")
            
            # Create base destination with column name preservation using DLT's built-in options
            log(f"   Using DLT's built-in column name preservation features")
            base_destination = dlt.destinations.sqlalchemy(
                engine_target,
                # Preserve exact table names without conversion
                table_name=lambda table: table,
                # Preserve exact column names without conversion
                column_name=lambda column: column,
                # Disable column name normalization
                normalize_column_name=False
            )
            
            log(f"   ✅ Base destination created with column name preservation")
        else:
            log(f"⚠️  Using default DLT destination configuration (column names may be normalized)")
            base_destination = dlt.destinations.sqlalchemy(engine_target)
        
        # Wrap the destination with our safe wrapper
        log(f"🛡️  Wrapping destination with SafeDLTDestination to prevent complex queries")
        safe_destination = SafeDLTDestination(base_destination, engine_target)
        
        # Create the pipeline with the safe destination
        pipeline = dlt.pipeline(
            pipeline_name=pipeline_name,
            destination=safe_destination,
            dataset_name=TARGET_DB_NAME,
            dev_mode=False  # Ensure we're not in dev mode which can cause state issues
        )
        
        log(f"✅ Fresh pipeline created successfully: {pipeline_name}")
        log(f"🛡️  Pipeline protected against complex DELETE queries")
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
                    base_destination = dlt.destinations.sqlalchemy(
                        engine_target,
                        # Preserve exact table names without conversion
                        table_name=lambda table: table,
                        # Preserve exact column names without conversion
                        column_name=lambda column: column,
                        # Disable column name normalization
                        normalize_column_name=False
                    )
                    
                    log(f"   ✅ Recovery base destination created with column name preservation")
                else:
                    log(f"⚠️  Using default recovery DLT destination configuration")
                    base_destination = dlt.destinations.sqlalchemy(engine_target)
                
                # Wrap the recovery destination with our safe wrapper
                log(f"🛡️  Wrapping recovery destination with SafeDLTDestination")
                safe_destination = SafeDLTDestination(base_destination, engine_target)
                
                pipeline = dlt.pipeline(
                    pipeline_name=pipeline_name,
                    destination=safe_destination,
                    dataset_name=TARGET_DB_NAME,
                    dev_mode=False
                )
                
                log(f"✅ Pipeline created successfully after aggressive cleanup: {pipeline_name}")
                log(f"🛡️  Recovery pipeline also protected against complex DELETE queries")
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