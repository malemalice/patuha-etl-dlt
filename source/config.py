"""
Configuration module for DLT Database Sync Pipeline.
Contains all environment variables, constants, and configuration loading.
"""

import os
import json
import threading
from urllib.parse import quote_plus
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
INTERVAL = int(os.getenv("INTERVAL", 60))  # Interval in seconds

# Batch processing configuration
BATCH_SIZE = int(os.getenv("BATCH_SIZE", 8))  # Process tables in batches of 8
BATCH_DELAY = int(os.getenv("BATCH_DELAY", 2))  # Delay between batches in seconds

# Debug mode for detailed logging
DEBUG_MODE = os.getenv("DEBUG_MODE", "false").lower() == "true"
DEEP_DEBUG_JSON = os.getenv("DEEP_DEBUG_JSON", "true").lower() == "true"
AUTO_SANITIZE_DATA = os.getenv("AUTO_SANITIZE_DATA", "true").lower() == "true"

# Column name preservation configuration
PRESERVE_COLUMN_NAMES = os.getenv("PRESERVE_COLUMN_NAMES", "true").lower() == "true"

# Lock timeout and retry configuration
LOCK_TIMEOUT_RETRIES = int(os.getenv("LOCK_TIMEOUT_RETRIES", "5"))  # Max retries for lock timeouts
LOCK_TIMEOUT_BASE_DELAY = int(os.getenv("LOCK_TIMEOUT_BASE_DELAY", "10"))  # Base delay in seconds
LOCK_TIMEOUT_MAX_DELAY = int(os.getenv("LOCK_TIMEOUT_MAX_DELAY", "300"))  # Max delay in seconds
LOCK_TIMEOUT_JITTER = float(os.getenv("LOCK_TIMEOUT_JITTER", "0.1"))  # Jitter factor (0.1 = 10%)

# Connection loss retry configuration
CONNECTION_LOSS_RETRIES = int(os.getenv("CONNECTION_LOSS_RETRIES", "3"))  # Max retries for connection loss

# Pipeline Mode Configuration
PIPELINE_MODE = os.getenv("PIPELINE_MODE", "direct")  # Options: 'direct' (db-to-db) or 'file_staging' (extract to files first)

# DLT Load Configuration (for direct mode)
TRUNCATE_STAGING_DATASET = os.getenv("TRUNCATE_STAGING_DATASET", "true").lower() == "true"  # Enable DLT staging management via pipeline configuration (DLT 1.15.0 compatible)

# Index Management Configuration
ENABLE_INDEX_OPTIMIZATION = os.getenv("ENABLE_INDEX_OPTIMIZATION", "true").lower() == "true"  # Enable automatic index creation for DLT operations
INDEX_OPTIMIZATION_TIMEOUT = int(os.getenv("INDEX_OPTIMIZATION_TIMEOUT", "45"))  # Timeout for staging table index creation (seconds)
CLEANUP_TEMPORARY_INDEXES = os.getenv("CLEANUP_TEMPORARY_INDEXES", "true").lower() == "true"  # Clean up temporary indexes after operations

# File-based staging configuration (used when PIPELINE_MODE='file_staging')
FILE_STAGING_DIR = os.getenv("FILE_STAGING_DIR", "staging")  # Base directory for staging files
FILE_STAGING_RETENTION_HOURS = int(os.getenv("FILE_STAGING_RETENTION_HOURS", "24"))  # How long to keep staging files
FILE_STAGING_WAIT_TIMEOUT = int(os.getenv("FILE_STAGING_WAIT_TIMEOUT", "60"))  # Timeout for file watcher (seconds)

# Legacy support - maintain FILE_STAGING_ENABLED for backward compatibility
FILE_STAGING_ENABLED = PIPELINE_MODE.lower() == "file_staging"

# Connection Pool Configuration (MariaDB-optimized)
POOL_SIZE = int(os.getenv("POOL_SIZE", "15"))  # Reduced for MariaDB stability
MAX_OVERFLOW = int(os.getenv("MAX_OVERFLOW", "20"))  # Reduced for MariaDB stability
POOL_TIMEOUT = int(os.getenv("POOL_TIMEOUT", "30"))  # Faster timeout for MariaDB
POOL_RECYCLE = int(os.getenv("POOL_RECYCLE", "1800"))  # More frequent recycling for MariaDB
POOL_PRE_PING = os.getenv("POOL_PRE_PING", "true").lower() == "true"

# HTTP Server Configuration
HTTP_SERVER_PORT = int(os.getenv("HTTP_SERVER_PORT", "8089"))
HTTP_SERVER_HOST = os.getenv("HTTP_SERVER_HOST", "")  # Empty string = all interfaces
HTTP_SERVER_TIMEOUT = int(os.getenv("HTTP_SERVER_TIMEOUT", "30"))  # Request timeout in seconds
HTTP_SERVER_MAX_REQUESTS = int(os.getenv("HTTP_SERVER_MAX_REQUESTS", "1000"))  # Max requests before restart
HTTP_SERVER_ENABLE_KEEPALIVE = os.getenv("HTTP_SERVER_ENABLE_KEEPALIVE", "true").lower() == "true"
HTTP_SERVER_ENABLE_REUSEADDR = os.getenv("HTTP_SERVER_ENABLE_REUSEADDR", "true").lower() == "true"

# Database URLs with intelligent driver selection
def _get_mysql_driver_url_prefix():
    """Get the appropriate MySQL URL prefix prioritizing pymysql for stability."""
    try:
        # Primary: Check if pymysql is available (recommended for stability)
        import pymysql
        return "mysql+pymysql"
    except ImportError:
        try:
            # Fallback: Check if MySQLdb (mysqlclient) is available
            import MySQLdb
            return "mysql"  # Use default mysql:// for MySQLdb
        except ImportError:
            # Fallback to default if no drivers available
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

# Helper function to URL-encode database credentials
def _url_encode_credential(credential):
    """URL-encode database credentials to handle special characters like @, :, etc."""
    return quote_plus(str(credential)) if credential else ""

# Construct database URLs with the correct driver and URL-encoded credentials
DB_SOURCE_URL = f"{_MYSQL_DRIVER_PREFIX}://{_url_encode_credential(SOURCE_DB_USER)}:{_url_encode_credential(SOURCE_DB_PASS)}@{SOURCE_DB_HOST}:{SOURCE_DB_PORT}/{SOURCE_DB_NAME}"
DB_TARGET_URL = f"{_MYSQL_DRIVER_PREFIX}://{_url_encode_credential(TARGET_DB_USER)}:{_url_encode_credential(TARGET_DB_PASS)}@{TARGET_DB_HOST}:{TARGET_DB_PORT}/{TARGET_DB_NAME}"

# Log driver selection when config is loaded (will be called later by main pipeline)
_log_driver_selection()

# Global transaction semaphore to limit concurrent transactions
transaction_semaphore = threading.Semaphore(3)  # Default to 3 concurrent transactions

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