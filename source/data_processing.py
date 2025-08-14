"""
Data processing and sanitization module for DLT Database Sync Pipeline.
Contains data validation, sanitization, and transformation functions.
"""

import dlt
import sqlalchemy as sa
import pandas as pd
from decimal import Decimal
import math
import orjson
from config import DEBUG_MODE, AUTO_SANITIZE_DATA, DEEP_DEBUG_JSON
from utils import log

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
            if math.isnan(value) or math.isinf(value):
                log(f"DEBUG: Converting NaN/Inf value to NULL in column {column_name}")
                return None
                
        # Test if value can be JSON serialized
        orjson.dumps(value)
        return value
        
    except Exception as sanitize_error:
        log(f"DEBUG: Failed to sanitize value in column {column_name}: {sanitize_error}, converting to string")
        try:
            return str(value) if value is not None else None
        except:
            log(f"DEBUG: Could not convert value to string in column {column_name}, setting to NULL")
            return None

@dlt.transformer
def sanitize_table_data(items):
    """DLT transformer to sanitize data before JSON serialization."""
    if not AUTO_SANITIZE_DATA:
        return items
        
    for item in items:
        if isinstance(item, dict):
            for key, value in item.items():
                item[key] = sanitize_data_value(value, key)
        yield item

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
                    
                    # Test each column value for JSON parse errors
                    for col_name, value in row_dict.items():
                        debug_value_for_json_parse_error(col_name, value, row_num + 1)
                    
                except Exception as row_error:
                    log(f"PARSE DEBUG: Error testing row {row_num + 1}: {row_error}")
                    continue
                    
    except Exception as debug_error:
        log(f"PARSE DEBUG: Failed to debug JSON parse errors for {table_name}: {debug_error}")

def debug_value_for_json_parse_error(col_name, value, row_num):
    """Debug a specific value for JSON parse error potential."""
    if not DEEP_DEBUG_JSON:
        return
        
    try:
        # Test if value can be JSON serialized
        orjson.dumps(value)
    except Exception as json_error:
        log(f"PARSE DEBUG: Row {row_num}, Column '{col_name}' causes JSON error: {json_error}")
        log(f"PARSE DEBUG: Value type: {type(value)}")
        log(f"PARSE DEBUG: Value repr: {repr(value)}")
        
        # Show hex representation for problematic values
        if isinstance(value, (str, bytes)):
            debug_value_hex_representation(col_name, value, "JSON parse error")

def debug_value_hex_representation(col_name, value, reason):
    """Show hex representation of problematic values."""
    try:
        if isinstance(value, str):
            hex_repr = value.encode('utf-8').hex()
            log(f"HEX DEBUG: Column '{col_name}' [{reason}] - String hex: {hex_repr[:100]}{'...' if len(hex_repr) > 100 else ''}")
        elif isinstance(value, bytes):
            hex_repr = value.hex()
            log(f"HEX DEBUG: Column '{col_name}' [{reason}] - Bytes hex: {hex_repr[:100]}{'...' if len(hex_repr) > 100 else ''}")
    except Exception as hex_error:
        log(f"HEX DEBUG: Failed to show hex representation for column '{col_name}': {hex_error}")

def debug_table_data(engine_source, table_name, sample_size=3):
    """Debug function to inspect table data that might cause JSON issues."""
    try:
        with engine_source.connect() as connection:
            # Get basic table info
            count_query = f"SELECT COUNT(*) FROM {table_name}"
            row_count = connection.execute(sa.text(count_query)).scalar()
            
            log(f"DEBUG: Table {table_name} has {row_count} rows")
            
            if row_count == 0:
                log(f"DEBUG: Table {table_name} is empty, no data to debug")
                return
            
            # Get table schema
            inspector = sa.inspect(engine_source)
            columns = inspector.get_columns(table_name)
            
            log(f"DEBUG: Table {table_name} has {len(columns)} columns:")
            for col in columns[:10]:  # Show first 10 columns
                log(f"DEBUG:   - {col['name']}: {col['type']}")
            if len(columns) > 10:
                log(f"DEBUG:   ... and {len(columns) - 10} more columns")
            
            # Get sample data
            sample_query = f"SELECT * FROM {table_name} LIMIT {sample_size}"
            result = connection.execute(sa.text(sample_query))
            sample_rows = result.fetchall()
            
            log(f"DEBUG: Sample data from {table_name} ({len(sample_rows)} rows):")
            
            column_names = [col['name'] for col in columns]
            for i, row in enumerate(sample_rows):
                log(f"DEBUG: Row {i + 1}:")
                row_dict = dict(zip(column_names, row))
                for col_name, value in row_dict.items():
                    # Show only first few characters of large values
                    display_value = str(value)[:50] + ("..." if len(str(value)) > 50 else "")
                    log(f"DEBUG:   {col_name}: {repr(display_value)} (type: {type(value).__name__})")
                    
    except Exception as debug_error:
        log(f"DEBUG: Failed to debug table data for {table_name}: {debug_error}")