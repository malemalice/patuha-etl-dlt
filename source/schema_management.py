"""
Schema management module for DLT Database Sync Pipeline.
Contains table schema synchronization and column management functions.
"""

import sqlalchemy as sa
from typing import List, Dict, Any
import config
from utils import log, log_phase, log_debug, log_error

def sync_table_schema(engine_source, engine_target, table_name: str) -> bool:
    """Sync table schema from source to target database."""
    try:
        log_phase(f"🔄 Syncing schema for {table_name}...")
        
        # Get source table columns
        source_columns = get_table_columns(engine_source, table_name)
        if not source_columns:
            return False
        
        # Get target table columns
        target_columns = get_table_columns(engine_target, table_name)
        if not target_columns:
            return False
        
        # Find missing columns in target
        missing_columns = []
        for col_name, col_info in source_columns.items():
            if col_name not in target_columns:
                missing_columns.append((col_name, col_info))
        
        if missing_columns:
            # Add missing columns to target table
            alter_statements = []
            for col_name, col_info in missing_columns:
                alter_stmt = f"ALTER TABLE {table_name} ADD COLUMN {col_name} {col_info['type']}"
                if col_info.get('nullable') is False:
                    alter_stmt += " NOT NULL"
                if col_info.get('default') is not None:
                    alter_stmt += f" DEFAULT {col_info['default']}"
                alter_statements.append(alter_stmt)
            
            # Execute alter statements
            for alter_stmt in alter_statements:
                with engine_target.connect() as conn:
                    conn.execute(sa.text(alter_stmt))
                    conn.commit()
            
            log_phase(f"✅ Schema sync SUCCESS: Added {len(alter_statements)} columns to {table_name}")
        else:
            log_phase(f"✅ Schema sync SUCCESS: {table_name} already synchronized")
        
        return True
        
    except Exception as e:
        log_error(f"❌ Schema sync failed for {table_name}: {e}")
        return False

def get_table_primary_keys(engine, table_name: str) -> List[str]:
    """Get primary key columns for a table."""
    try:
        with engine.connect() as conn:
            # Get primary key information
            query = """
                SELECT COLUMN_NAME 
                FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE 
                WHERE TABLE_SCHEMA = :schema 
                AND TABLE_NAME = :table 
                AND CONSTRAINT_NAME = 'PRIMARY'
                ORDER BY ORDINAL_POSITION
            """
            result = conn.execute(sa.text(query), {"schema": config.TARGET_DB_NAME, "table": table_name})
            primary_keys = [row[0] for row in result]
            return primary_keys
    except Exception as e:
        log_error(f"❌ Error getting primary keys for table {table_name}: {e}")
        return []

def get_table_columns(engine, table_name: str) -> Dict[str, Dict[str, Any]]:
    """Get column information for a table."""
    try:
        with engine.connect() as conn:
            query = """
                SELECT COLUMN_NAME, DATA_TYPE, IS_NULLABLE, COLUMN_DEFAULT, CHARACTER_MAXIMUM_LENGTH
                FROM INFORMATION_SCHEMA.COLUMNS 
                WHERE TABLE_SCHEMA = :schema 
                AND TABLE_NAME = :table
                ORDER BY ORDINAL_POSITION
            """
            result = conn.execute(sa.text(query), {"schema": config.TARGET_DB_NAME, "table": table_name})
            
            columns = {}
            for row in result:
                col_name, data_type, nullable, default, max_length = row
                columns[col_name] = {
                    'type': data_type,
                    'nullable': nullable == 'YES',
                    'default': default,
                    'max_length': max_length
                }
            return columns
    except Exception as e:
        log_error(f"❌ Error getting columns for table {table_name}: {e}")
        return {}

def table_exists(engine, table_name: str) -> bool:
    """Check if a table exists in the database."""
    try:
        with engine.connect() as conn:
            query = """
                SELECT COUNT(*) 
                FROM INFORMATION_SCHEMA.TABLES 
                WHERE TABLE_SCHEMA = :schema 
                AND TABLE_NAME = :table
            """
            result = conn.execute(sa.text(query), {"schema": config.TARGET_DB_NAME, "table": table_name})
            count = result.scalar()
            return count > 0
    except Exception as e:
        log_error(f"❌ Error checking if table {table_name} exists: {e}")
        return False

def validate_table_structure(engine_source, engine_target, table_name: str) -> bool:
    """Validate that table structure is compatible between source and target."""
    try:
        # Check if table exists in source
        if not table_exists(engine_source, table_name):
            log_error(f"❌ Table {table_name} does not exist in source database")
            return False
        
        # Check if table exists in target
        if not table_exists(engine_target, table_name):
            log_debug(f"⚠️ Table {table_name} does not exist in target database - will be created")
            return True
        
        # Get column information
        source_columns = get_table_columns(engine_source, table_name)
        target_columns = get_table_columns(engine_target, table_name)
        
        if not source_columns or not target_columns:
            return False
        
        # Check for missing columns in target
        missing_columns = set(source_columns.keys()) - set(target_columns.keys())
        if missing_columns:
            log_debug(f"⚠️ Table {table_name} missing columns in target: {missing_columns}")
        
        # Check for extra columns in target (not critical)
        extra_columns = set(target_columns.keys()) - set(source_columns.keys())
        if extra_columns:
            log_debug(f"⚠️ Table {table_name} has extra columns in target: {extra_columns}")
        
        return True
        
    except Exception as e:
        log_error(f"❌ Error validating table structure for {table_name}: {e}")
        return False

# get_table_row_count function moved to pipeline_management.py to avoid duplication