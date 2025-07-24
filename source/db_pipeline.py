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

DB_SOURCE_URL = f"mysql://{SOURCE_DB_USER}:{SOURCE_DB_PASS}@{SOURCE_DB_HOST}:{SOURCE_DB_PORT}/{SOURCE_DB_NAME}"
DB_TARGET_URL = f"mysql://{TARGET_DB_USER}:{TARGET_DB_PASS}@{TARGET_DB_HOST}:{TARGET_DB_PORT}/{TARGET_DB_NAME}"

# Load table configurations from tables.json
TABLES_FILE = "tables.json"
with open(TABLES_FILE, "r") as f:
    tables_data = json.load(f)

table_configs = {t["table"]: t for t in tables_data}

# Create engines once with proper connection pool configuration
def create_engines():
    """Create SQLAlchemy engines with optimized connection pool settings."""
    pool_settings = {
        'pool_size': POOL_SIZE,           # Configurable base pool size
        'max_overflow': MAX_OVERFLOW,     # Configurable overflow limit  
        'pool_timeout': POOL_TIMEOUT,     # Configurable timeout
        'pool_recycle': POOL_RECYCLE,     # Configurable connection recycle time
        'pool_pre_ping': True,            # Validate connections before use
    }
    
    log(f"Creating engines with pool settings: {pool_settings}")
    
    engine_source = sa.create_engine(DB_SOURCE_URL, **pool_settings)
    engine_target = sa.create_engine(DB_TARGET_URL, **pool_settings)
    
    return engine_source, engine_target

# Global engine instances
ENGINE_SOURCE, ENGINE_TARGET = create_engines()

def log(message):
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"{timestamp} - INFO - {message}")

def ensure_dlt_columns(engine_target, table_name):
    """Check if _dlt_load_id and _dlt_id exist in the target table, add them if not."""
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
            
def sync_table_schema(engine_source, engine_target, table_name):
    """Sync schema from source to target, handling new, changed, and deleted columns."""
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

def get_max_timestamp(engine_source, table_name, column_name):
    """Fetch the max timestamp from the source table."""
    query = f"SELECT MAX({column_name}) FROM {table_name}"
    with engine_source.connect() as connection:
        result = connection.execute(sa.text(query)).scalar()
    return result if result else datetime(1970, 1, 1, 0, 0, 0)
            
def load_select_tables_from_database() -> None:
    """Use the sql_database source to reflect an entire database schema and load select tables from it."""
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

    # Ensure _dlt_load_id and _dlt_id exist in target tables
    for table in table_configs.keys():
        ensure_dlt_columns(engine_target, table)
        sync_table_schema(engine_source, engine_target, table)

    # Process incremental tables
    if incremental_tables:
        log(f"Processing incremental tables: {list(incremental_tables.keys())}")
        source_incremental = sql_database(engine_source).with_resources(*incremental_tables.keys())
        for table, config in incremental_tables.items():
            log(f"Setting incremental for table {table} on column {config['modifier']}")
            max_timestamp = pendulum.instance(get_max_timestamp(engine_target, table, config["modifier"])).in_tz("Asia/Bangkok")
            log(f"Setting incremental for table {table} on column {config['modifier']} with initial value {max_timestamp}")
            getattr(source_incremental, table).apply_hints(
                primary_key=config["primary_key"],
                incremental=dlt.sources.incremental(config["modifier"],
                initial_value=max_timestamp)
                )
        info = pipeline.run(source_incremental, write_disposition="merge")
        log(info)
    
    # Process full refresh tables
    if full_refresh_tables:
        log(f"Processing full refresh tables: {full_refresh_tables}")
        source_full_refresh = sql_database(engine_source).with_resources(*full_refresh_tables)
        info = pipeline.run(source_full_refresh, write_disposition="replace")
        log(info)

    # Log final connection pool status
    log(f"Final source pool status - Size: {engine_source.pool.size()}, Checked out: {engine_source.pool.checkedout()}")
    log(f"Final target pool status - Size: {engine_target.pool.size()}, Checked out: {engine_target.pool.checkedout()}")


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