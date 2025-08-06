# flake8: noqa
import humanize
from typing import Any
import os
import json
import threading
import time
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

DB_SOURCE_URL = f"mysql://{SOURCE_DB_USER}:{SOURCE_DB_PASS}@{SOURCE_DB_HOST}:{SOURCE_DB_PORT}/{SOURCE_DB_NAME}"
DB_TARGET_URL = f"mysql://{TARGET_DB_USER}:{TARGET_DB_PASS}@{TARGET_DB_HOST}:{TARGET_DB_PORT}/{TARGET_DB_NAME}"

# Load table configurations from tables.json
TABLES_FILE = "tables.json"
with open(TABLES_FILE, "r") as f:
    tables_data = json.load(f)

table_configs = {t["table"]: t for t in tables_data}

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
        'autocommit': True,
        'charset': 'utf8mb4',
        # Permissive SQL mode that allows zero dates and works with both strict/unrestricted modes
        # 'init_command': "SET SESSION sql_mode='ERROR_FOR_DIVISION_BY_ZERO'",
        'init_command': "SET sql_mode=(SELECT REPLACE(@@sql_mode,'ONLY_FULL_GROUP_BY',''))",
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

# Global engine instances
ENGINE_SOURCE, ENGINE_TARGET = create_engines()

def log(message):
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"{timestamp} - INFO - {message}")

def retry_on_connection_error(func, db_type="unknown", *args, **kwargs):
    """Retry function on MySQL connection errors.
    
    Args:
        func: Function to retry
        db_type: Type of database ('source' or 'target') for better error logging
        *args, **kwargs: Arguments to pass to the function
    """
    global ENGINE_SOURCE, ENGINE_TARGET  # Declare global at the top
    
    for attempt in range(3): # Simplified retry logic
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

def ensure_dlt_columns(engine_target, table_name):
    """Check if _dlt_load_id and _dlt_id exist in the target table, add them if not."""
    def _ensure_columns():
        inspector = sa.inspect(engine_target)
        columns = [col["name"] for col in inspector.get_columns(table_name)]
        alter_statements = []
        
        if "_dlt_load_id" not in columns:
            alter_statements.append("ADD COLUMN `_dlt_load_id` TEXT NOT NULL")
        if "_dlt_id" not in columns:
            alter_statements.append("ADD COLUMN `_dlt_id` VARCHAR(128) NOT NULL")
        
        if alter_statements:
            alter_query = f"ALTER TABLE {table_name} {', '.join(alter_statements)};"
            with engine_target.connect() as connection:
                log(f"Altering table {table_name}: {alter_query}")
                connection.execute(sa.text(alter_query))
                connection.commit()
    
    return retry_on_connection_error(_ensure_columns, f"target (ensure_dlt_columns for {table_name})")
            
def sync_table_schema(engine_source, engine_target, table_name):
    """Sync schema from source to target, handling new, changed, and deleted columns."""
    def _sync_schema():
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
            with engine_target.connect() as connection:
                log(f"Syncing schema for {table_name}: {alter_query}")
                connection.execute(sa.text(alter_query))
                connection.commit()
    
    # This function uses both source and target, but let's specify based on the main operation
    return retry_on_connection_error(_sync_schema, f"source+target (sync_table_schema for {table_name})")

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
        if hasattr(value, '__class__') and value.__class__.__name__ == 'Decimal':
            log(f"DEBUG: Converting Decimal to float in column {column_name}: {value}")
            try:
                # Convert Decimal to float for JSON compatibility
                from decimal import Decimal
                if isinstance(value, Decimal):
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
                        log(f"DEBUG: *** FOUND PROBLEMATIC ROW {row_num + 1} in {table_name} ***")
                        for prob_col in problematic_columns:
                            log(f"DEBUG: Problem column '{prob_col['column']}' = {prob_col['value']}")
                            log(f"DEBUG: Value type: {prob_col['type']}, Error: {prob_col['error']}")
                        
                        # Try sanitizing the problematic row
                        log(f"DEBUG: Attempting to sanitize row {row_num + 1}")
                        sanitized_row = {}
                        for col_name, value in row_dict.items():
                            sanitized_row[col_name] = sanitize_data_value(value, col_name)
                        
                        # Test if sanitized row can be JSON encoded
                        try:
                            import orjson
                            orjson.dumps(sanitized_row)
                            log(f"DEBUG: Row {row_num + 1} successfully sanitized")
                        except Exception as sanitize_test_error:
                            log(f"DEBUG: Row {row_num + 1} still problematic after sanitization: {sanitize_test_error}")
                        
                        return problematic_columns  # Return details of first problematic row
                        
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

def get_max_timestamp(engine_target, table_name, column_name):
    """Fetch the max timestamp from the target table."""
    def _get_timestamp():
        query = f"SELECT MAX({column_name}) FROM {table_name}"
        with engine_target.connect() as connection:
            result = connection.execute(sa.text(query)).scalar()
        return result if result else datetime(1970, 1, 1, 0, 0, 0)
    
    return retry_on_connection_error(_get_timestamp, f"target (get_max_timestamp for {table_name}.{column_name})")
            
def process_tables_batch(pipeline, engine_source, engine_target, tables_dict, write_disposition="merge"):
    """Process a batch of tables with proper connection management."""
    if not tables_dict:
        return
    
    table_names = list(tables_dict.keys())
    log(f"Processing batch of {len(table_names)} tables: {table_names}")
    
    try:
        # Create source with only this batch of tables
        if DEBUG_MODE:
            log(f"Creating DLT source for tables: {table_names}")
        source_batch = sql_database(engine_source).with_resources(*table_names)
        
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
        info = pipeline.run(source_batch, write_disposition=write_disposition)
        log(f"Batch completed successfully: {info}")
        
    except Exception as e:
        error_message = str(e)
        error_type = type(e).__name__
        log(f"Error in batch processing: {error_message}")
        log(f"Error type: {error_type}")
        
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
                                    sanitized_resource = getattr(sanitized_source, table_name) | sanitize_table_data
                                    
                                    if write_disposition == "merge" and "modifier" in tables_dict[table_name]:
                                        config = tables_dict[table_name]
                                        max_timestamp = pendulum.instance(get_max_timestamp(engine_target, table_name, config["modifier"])).in_tz("Asia/Bangkok")
                                        sanitized_resource.apply_hints(
                                            primary_key=config["primary_key"],
                                            incremental=dlt.sources.incremental(config["modifier"], initial_value=max_timestamp)
                                        )
                                    
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
                                log(f"*** FOUND PROBLEMATIC DATA IN {table_name} ***")
                                log(f"Number of problematic columns: {len(problematic_data)}")
                                for i, prob_col in enumerate(problematic_data):
                                    log(f"Problem {i+1}: Column '{prob_col['column']}' = {prob_col['value']}")
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
                                    
                                    # Show column types
                                    inspector = sa.inspect(engine_source)
                                    columns = inspector.get_columns(table_name)
                                    log(f"  Columns ({len(columns)} total):")
                                    for col in columns[:10]:  # Show first 10 columns
                                        log(f"    {col['name']}: {col['type']}")
                            except Exception as emergency_error:
                                log(f"Even emergency fallback failed: {emergency_error}")
                        
                        # Test basic database operations
                        log(f"=== TESTING BASIC DATABASE OPERATIONS FOR {table_name} ===")
                        try:
                            with engine_source.connect() as conn:
                                # Test count
                                test_query = f"SELECT COUNT(*) FROM {table_name}"
                                count = conn.execute(sa.text(test_query)).scalar()
                                log(f"Row count: {count}")
                                
                                # Test schema
                                inspector = sa.inspect(engine_source)
                                columns = inspector.get_columns(table_name)
                                log(f"Table schema: {len(columns)} columns")
                                for col in columns:
                                    log(f"  {col['name']}: {col['type']}")
                                
                        except Exception as basic_test_error:
                            log(f"Basic table operations failed: {basic_test_error}")
                        
                        # Test DLT source creation
                        log(f"=== TESTING DLT SOURCE CREATION FOR {table_name} ===")
                        try:
                            test_source = sql_database(engine_source).with_resources(table_name)
                            log(f"DLT source creation: SUCCESS")
                            
                            # Try to iterate over a few records
                            table_resource = getattr(test_source, table_name)
                            log(f"Table resource access: SUCCESS")
                            
                            # Try to extract just a few records to see where it fails
                            log(f"Testing data extraction...")
                            record_count = 0
                            for record in table_resource:
                                record_count += 1
                                if record_count <= 3:
                                    log(f"Record {record_count}: {type(record)} with {len(record) if hasattr(record, '__len__') else 'unknown'} items")
                                if record_count >= 5:
                                    break
                            log(f"Successfully extracted {record_count} records from DLT source")
                            
                        except Exception as dlt_test_error:
                            log(f"DLT source operations failed: {dlt_test_error}")
                            log(f"DLT error type: {type(dlt_test_error).__name__}")
                            import traceback
                            log(f"DLT error traceback:")
                            log(traceback.format_exc())
                        
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

def load_select_tables_from_database() -> None:
    """Use the sql_database source to reflect an entire database schema and load select tables from it."""
    def _load_tables():
        # Use global engines instead of creating new ones
        engine_source = ENGINE_SOURCE
        engine_target = ENGINE_TARGET
        
        # Log connection pool status
        log(f"Source pool status - Size: {engine_source.pool.size()}, Checked out: {engine_source.pool.checkedout()}")
        log(f"Target pool status - Size: {engine_target.pool.size()}, Checked out: {engine_target.pool.checkedout()}")
        
        # Use a single pipeline instance to reduce MetaData conflicts
        pipeline = dlt.pipeline(
            pipeline_name="dlt_unified_pipeline_v1_0_17", 
            destination=dlt.destinations.sqlalchemy(engine_target), 
            dataset_name=TARGET_DB_NAME
        )
        
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

    return retry_on_connection_error(_load_tables, "source+target (main data loading)")


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

if __name__ == "__main__":
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