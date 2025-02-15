# flake8: noqa
import humanize
from typing import Any
import os
from dotenv import load_dotenv

import dlt
from dlt.common import pendulum
from dlt.sources.sql_database import sql_database
import sqlalchemy as sa

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

DB_SOURCE_URL = f"mysql://{SOURCE_DB_USER}:{SOURCE_DB_PASS}@{SOURCE_DB_HOST}:{SOURCE_DB_PORT}/{SOURCE_DB_NAME}"
DB_TARGET_URL = f"mysql://{TARGET_DB_USER}:{TARGET_DB_PASS}@{TARGET_DB_HOST}:{TARGET_DB_PORT}/{TARGET_DB_NAME}"

# Load table configurations from environment variables
TABLES = os.getenv("TABLES", "").split(";")
table_configs = {}
for t in TABLES:
    parts = t.split(":")
    table_name = parts[0]
    column = parts[1] if len(parts) > 1 else None
    table_configs[table_name] = column

def load_select_tables_from_database() -> None:
    """Use the sql_database source to reflect an entire database schema and load select tables from it."""
    engine_source = sa.create_engine(DB_SOURCE_URL)
    engine_target = sa.create_engine(DB_TARGET_URL)

    # Create separate pipelines for incremental and full refresh
    pipeline_incremental = dlt.pipeline(
        pipeline_name="dlt_incremental", 
        destination=dlt.destinations.sqlalchemy(engine_target), 
        dataset_name=TARGET_DB_NAME
    )
    
    pipeline_full_refresh = dlt.pipeline(
        pipeline_name="dlt_full_refresh", 
        destination=dlt.destinations.sqlalchemy(engine_target), 
        dataset_name=TARGET_DB_NAME
    )

    incremental_tables = {t: c for t, c in table_configs.items() if c}
    full_refresh_tables = [t for t, c in table_configs.items() if not c]

    if incremental_tables:
        print(f"Adding tables to incremental with_resources: {list(incremental_tables.keys())}")
        source_incremental = sql_database(engine_source).with_resources(*incremental_tables.keys())
        for table, column in incremental_tables.items():
            print(f"Setting incremental for table {table} on column {column}")
            getattr(source_incremental, table).apply_hints(incremental=dlt.sources.incremental(column))
        info = pipeline_incremental.run(source_incremental, write_disposition="merge")
        print(info)
    
    if full_refresh_tables:
        print(f"Adding tables to full refresh with_resources: {full_refresh_tables}")
        source_full_refresh = sql_database(engine_source).with_resources(*full_refresh_tables)
        info = pipeline_full_refresh.run(source_full_refresh, write_disposition="replace")
        print(info)


if __name__ == "__main__":
    load_select_tables_from_database()
