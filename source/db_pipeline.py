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

# Connection pool configuration
POOL_SIZE = int(os.getenv("POOL_SIZE", 20))
MAX_OVERFLOW = int(os.getenv("MAX_OVERFLOW", 30))
POOL_TIMEOUT = int(os.getenv("POOL_TIMEOUT", 60))
POOL_RECYCLE = int(os.getenv("POOL_RECYCLE", 3600))

# MySQL-specific timeout configurations
CONNECT_TIMEOUT = int(os.getenv("MYSQL_CONNECT_TIMEOUT", 60))
READ_TIMEOUT = int(os.getenv("MYSQL_READ_TIMEOUT", 300))  # 5 minutes
WRITE_TIMEOUT = int(os.getenv("MYSQL_WRITE_TIMEOUT", 300))  # 5 minutes

# Retry configuration
MAX_RETRIES = int(os.getenv("MAX_RETRIES", 3))
RETRY_DELAY = int(os.getenv("RETRY_DELAY", 30))  # seconds

# Batch processing configuration
BATCH_SIZE = int(os.getenv("BATCH_SIZE", 8))  # Process tables in batches of 5
BATCH_DELAY = int(os.getenv("BATCH_DELAY", 2))  # Delay between batches in seconds

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
        'pool_size': POOL_SIZE,           # Configurable base pool size
        'max_overflow': MAX_OVERFLOW,     # Configurable overflow limit  
        'pool_timeout': POOL_TIMEOUT,     # Configurable timeout
        'pool_recycle': POOL_RECYCLE,     # Configurable connection recycle time
        'pool_pre_ping': True,            # Validate connections before use
    }
    
    # MySQL-specific connection arguments to handle timeouts and connection stability
    mysql_connect_args = {
        'connect_timeout': CONNECT_TIMEOUT,
        'read_timeout': READ_TIMEOUT,
        'write_timeout': WRITE_TIMEOUT,
        'autocommit': True,
        'charset': 'utf8mb4',
        # Set SQL mode to handle data compatibility
        'init_command': "SET sql_mode=(SELECT REPLACE(@@sql_mode,'ONLY_FULL_GROUP_BY',''))",
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
    
    for attempt in range(MAX_RETRIES):
        try:
            return func(*args, **kwargs)
        except sa.exc.OperationalError as e:
            if "MySQL server has gone away" in str(e) or "2006" in str(e):
                log(f"MySQL connection lost on {db_type} database (attempt {attempt + 1}/{MAX_RETRIES}). Error: {e}")
                if attempt < MAX_RETRIES - 1:
                    log(f"Retrying {db_type} database connection in {RETRY_DELAY} seconds...")
                    time.sleep(RETRY_DELAY)
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
                log(f"MySQL 'Commands out of sync' error on {db_type} database (attempt {attempt + 1}/{MAX_RETRIES}). Error: {e}")
                if attempt < MAX_RETRIES - 1:
                    log(f"Retrying {db_type} database connection in {RETRY_DELAY} seconds...")
                    time.sleep(RETRY_DELAY)
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
    
    # Create source with only this batch of tables
    if write_disposition == "merge":
        # Incremental tables
        source_batch = sql_database(engine_source).with_resources(*table_names)
        for table, config in tables_dict.items():
            log(f"Setting incremental for table {table} on column {config['modifier']}")
            max_timestamp = pendulum.instance(get_max_timestamp(engine_target, table, config["modifier"])).in_tz("Asia/Bangkok")
            log(f"Setting incremental for table {table} on column {config['modifier']} with initial value {max_timestamp}")
            getattr(source_batch, table).apply_hints(
                primary_key=config["primary_key"],
                incremental=dlt.sources.incremental(config["modifier"],
                initial_value=max_timestamp)
                )
    else:
        # Full refresh tables
        source_batch = sql_database(engine_source).with_resources(*table_names)
    
    # Run the batch
    info = pipeline.run(source_batch, write_disposition=write_disposition)
    log(f"Batch completed: {info}")
    
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
            pipeline_name="dlt_unified_pipeline", 
            destination=dlt.destinations.sqlalchemy(engine_target), 
            dataset_name=TARGET_DB_NAME
        )

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