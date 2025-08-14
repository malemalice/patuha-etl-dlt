"""
Utility functions for DLT Database Sync Pipeline.
Contains logging, debugging, and common helper functions.
"""

from datetime import datetime
from typing import Any, Union, List
import json

def log(message):
    """Log a message with timestamp."""
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"{timestamp} - {message}")

def format_primary_key(primary_key: Union[str, List[str]]) -> str:
    """Format primary key for DLT hints with validation."""
    if isinstance(primary_key, list):
        # For composite keys, join with comma and validate
        if not all(isinstance(key, str) and key.strip() for key in primary_key):
            raise ValueError(f"Invalid primary key configuration: {primary_key}")
        return ", ".join(primary_key)
    else:
        # For single keys, validate and return
        if not isinstance(primary_key, str) or not primary_key.strip():
            raise ValueError(f"Invalid primary key configuration: {primary_key}")
        return primary_key

def validate_primary_key_config(primary_key: Union[str, List[str]]) -> bool:
    """Enhanced validation of primary key configuration."""
    try:
        if isinstance(primary_key, str):
            return bool(primary_key.strip())
        elif isinstance(primary_key, list):
            return (len(primary_key) > 0 and 
                    all(isinstance(key, str) and bool(key.strip()) for key in primary_key))
        else:
            return False
    except Exception:
        return False

def log_primary_key_info(table_name: str, primary_key: Union[str, List[str]]):
    """Log primary key information for a table."""
    if isinstance(primary_key, list):
        log(f"🔑 Table '{table_name}' has composite primary key: {primary_key} (count: {len(primary_key)})")
    else:
        log(f"🔑 Table '{table_name}' has single primary key: {primary_key}")

def safe_json_loads(json_string, context="", default=None):
    """Safely load JSON with enhanced debugging for 'line 1 column 1 (char 0)' errors."""
    try:
        if json_string is None or json_string == "":
            log(f"🔍 JSON Debug [{context}]: Empty or None JSON string, returning default: {default}")
            return default
            
        # Try to parse JSON
        result = json.loads(json_string)
        return result
        
    except json.JSONDecodeError as e:
        log(f"❌ JSON Parse Error [{context}]: {e}")
        log(f"🔍 JSON String (first 200 chars): {repr(json_string[:200])}")
        
        # Check for common issues
        if "line 1 column 1 (char 0)" in str(e):
            log(f"🔍 Specific error 'line 1 column 1 (char 0)' detected")
            log(f"🔍 JSON String length: {len(json_string) if json_string else 0}")
            log(f"🔍 JSON String type: {type(json_string)}")
            
            if json_string:
                log(f"🔍 First character: {repr(json_string[0])}")
                log(f"🔍 First 10 characters: {repr(json_string[:10])}")
        
        return default
    except Exception as e:
        log(f"❌ Unexpected JSON Error [{context}]: {e}")
        return default

def debug_json_operation(operation_name, value, context=""):
    """Debug JSON operations that might cause 'line 1 column 1 (char 0)' errors."""
    log(f"🔍 JSON Operation Debug: {operation_name} [{context}]")
    log(f"🔍 Value type: {type(value)}")
    log(f"🔍 Value: {repr(value)}")
    
    if isinstance(value, str):
        log(f"🔍 String length: {len(value)}")
        if value:
            log(f"🔍 First character: {repr(value[0])}")
            log(f"🔍 First 20 characters: {repr(value[:20])}")
        else:
            log(f"🔍 Empty string detected")
    
    # Try JSON operation
    try:
        if operation_name == "loads":
            result = json.loads(value)
            log(f"✅ JSON loads successful")
            return result
        elif operation_name == "dumps":
            result = json.dumps(value)
            log(f"✅ JSON dumps successful")
            return result
    except Exception as e:
        log(f"❌ JSON operation failed: {e}")
        return None