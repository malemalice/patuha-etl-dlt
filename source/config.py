"""
Configuration module for DLT Database Sync Pipeline.
Contains all environment variables, constants, and configuration loading.
"""

import os
import json
import threading
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Database Configuration
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

# Pipeline Configuration
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

# Pipeline Mode Configuration
PIPELINE_MODE = os.getenv("PIPELINE_MODE", "direct")  # Options: 'direct' (db-to-db) or 'file_staging' (extract to files first)

# DLT Load Configuration (for direct mode)
TRUNCATE_STAGING_DATASET = os.getenv("TRUNCATE_STAGING_DATASET", "true").lower() == "true"  # Enable DLT staging management via pipeline configuration (DLT 1.15.0 compatible)

# File-based staging configuration (used when PIPELINE_MODE='file_staging')
FILE_STAGING_DIR = os.getenv("FILE_STAGING_DIR", "staging")  # Base directory for staging files
FILE_STAGING_RETENTION_HOURS = int(os.getenv("FILE_STAGING_RETENTION_HOURS", "24"))  # How long to keep staging files
FILE_STAGING_COMPRESSION = os.getenv("FILE_STAGING_COMPRESSION", "snappy")  # Compression algorithm
FILE_STAGING_ADVANCED_MONITORING = os.getenv("FILE_STAGING_ADVANCED_MONITORING", "true").lower() == "true"

# Legacy support - maintain FILE_STAGING_ENABLED for backward compatibility
FILE_STAGING_ENABLED = PIPELINE_MODE.lower() == "file_staging"

# HTTP Server Configuration
HTTP_SERVER_PORT = int(os.getenv("HTTP_SERVER_PORT", "8089"))
HTTP_SERVER_HOST = os.getenv("HTTP_SERVER_HOST", "")  # Empty string = all interfaces
HTTP_SERVER_TIMEOUT = int(os.getenv("HTTP_SERVER_TIMEOUT", "30"))  # Request timeout in seconds
HTTP_SERVER_MAX_REQUESTS = int(os.getenv("HTTP_SERVER_MAX_REQUESTS", "1000"))  # Max requests before restart
HTTP_SERVER_ENABLE_KEEPALIVE = os.getenv("HTTP_SERVER_ENABLE_KEEPALIVE", "true").lower() == "true"
HTTP_SERVER_ENABLE_REUSEADDR = os.getenv("HTTP_SERVER_ENABLE_REUSEADDR", "true").lower() == "true"

# Database URLs with intelligent driver selection
def _get_mysql_driver_url_prefix():
    """Get the appropriate MySQL URL prefix based on available drivers."""
    try:
        # Check if MySQLdb (mysqlclient) is available
        import MySQLdb
        return "mysql"  # Use default mysql:// for MySQLdb
    except ImportError:
        try:
            # Check if mysql.connector is available
            import mysql.connector
            return "mysql+mysqlconnector"
        except ImportError:
            try:
                # Check if pymysql is available
                import pymysql
                return "mysql+pymysql"
            except ImportError:
                # Fallback to default
                return "mysql"

# Get the appropriate driver prefix
_MYSQL_DRIVER_PREFIX = _get_mysql_driver_url_prefix()

# Log the selected driver for debugging
def _log_driver_selection():
    """Log which MySQL driver prefix was selected."""
    # Import log here to avoid circular imports
    try:
        from utils import log
        log(f"🔧 Using MySQL driver URL prefix: {_MYSQL_DRIVER_PREFIX}://")
    except ImportError:
        # utils module not available during config loading
        pass

# Construct database URLs with the correct driver
DB_SOURCE_URL = f"{_MYSQL_DRIVER_PREFIX}://{SOURCE_DB_USER}:{SOURCE_DB_PASS}@{SOURCE_DB_HOST}:{SOURCE_DB_PORT}/{SOURCE_DB_NAME}"
DB_TARGET_URL = f"{_MYSQL_DRIVER_PREFIX}://{TARGET_DB_USER}:{TARGET_DB_PASS}@{TARGET_DB_HOST}:{TARGET_DB_PORT}/{TARGET_DB_NAME}"

# Log driver selection when config is loaded (will be called later by main pipeline)
_log_driver_selection()

# Global transaction semaphore to limit concurrent transactions
transaction_semaphore = threading.Semaphore(MAX_CONCURRENT_TRANSACTIONS)

# Global engine variables (will be set by database module)
ENGINE_SOURCE = None
ENGINE_TARGET = None

def load_table_configs():
    """Load table configurations from tables.json."""
    # Try to find tables.json in current directory or source directory
    possible_paths = [
        "tables.json",
        "source/tables.json",
        os.path.join(os.path.dirname(__file__), "tables.json")
    ]
    
    tables_file = None
    for path in possible_paths:
        if os.path.exists(path):
            tables_file = path
            break
    
    if not tables_file:
        raise FileNotFoundError(f"tables.json not found. Tried paths: {possible_paths}")
    
    with open(tables_file, "r") as f:
        tables_data = json.load(f)
    
    return {t["table"]: t for t in tables_data}

# Load table configurations
table_configs = load_table_configs()