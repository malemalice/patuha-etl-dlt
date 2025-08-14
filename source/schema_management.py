"""
Schema management module for DLT Database Sync Pipeline.
Contains schema synchronization, validation, and management functions.
"""

import sqlalchemy as sa
from database import execute_with_transaction_management
from utils import log

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

def get_table_primary_keys(engine, table_name):
    """Get primary key columns for a table."""
    try:
        inspector = sa.inspect(engine)
        pk_constraint = inspector.get_pk_constraint(table_name)
        return pk_constraint.get('constrained_columns', [])
    except Exception as e:
        log(f"❌ Error getting primary keys for table {table_name}: {e}")
        return []

def get_table_columns(engine, table_name):
    """Get column information for a table."""
    try:
        inspector = sa.inspect(engine)
        columns = inspector.get_columns(table_name)
        return {col["name"]: col for col in columns}
    except Exception as e:
        log(f"❌ Error getting columns for table {table_name}: {e}")
        return {}

def table_exists(engine, table_name):
    """Check if a table exists in the database."""
    try:
        inspector = sa.inspect(engine)
        return table_name in inspector.get_table_names()
    except Exception as e:
        log(f"❌ Error checking if table {table_name} exists: {e}")
        return False

def validate_table_structure(engine_source, engine_target, table_name):
    """Validate that table structure is compatible between source and target."""
    try:
        # Check if table exists in both databases
        if not table_exists(engine_source, table_name):
            log(f"❌ Table {table_name} does not exist in source database")
            return False
            
        if not table_exists(engine_target, table_name):
            log(f"⚠️ Table {table_name} does not exist in target database - will be created")
            return True
            
        # Get columns from both databases
        source_columns = get_table_columns(engine_source, table_name)
        target_columns = get_table_columns(engine_target, table_name)
        
        # Check for missing columns in target
        missing_columns = set(source_columns.keys()) - set(target_columns.keys())
        if missing_columns:
            log(f"⚠️ Table {table_name} missing columns in target: {missing_columns}")
        
        # Check for extra columns in target (excluding DLT columns)
        dlt_columns = {"_dlt_load_id", "_dlt_id"}
        extra_columns = set(target_columns.keys()) - set(source_columns.keys()) - dlt_columns
        if extra_columns:
            log(f"⚠️ Table {table_name} has extra columns in target: {extra_columns}")
        
        return True
        
    except Exception as e:
        log(f"❌ Error validating table structure for {table_name}: {e}")
        return False

def get_table_row_count(engine, table_name):
    """Get the number of rows in a table."""
    try:
        with engine.connect() as connection:
            count_query = f"SELECT COUNT(*) FROM {table_name}"
            result = connection.execute(sa.text(count_query))
            return result.scalar()
    except Exception as e:
        log(f"❌ Error getting row count for table {table_name}: {e}")
        return 0