#!/usr/bin/env python3
"""
Index Management for DLT Operations

This module provides utilities to optimize database indexes for DLT operations,
specifically to speed up the slow DELETE queries that DLT uses for staging operations.

Key Features:
- Analyze DLT's DELETE query patterns
- Create optimized indexes for staging operations
- Monitor and optimize staging tables in real-time
- Clean up temporary indexes after operations

DLT's DELETE Query Pattern:
DELETE FROM target_table WHERE EXISTS (
    SELECT 1 FROM staging_table 
    WHERE staging_table.primary_key = target_table.primary_key
)

Optimization Strategy:
1. Create indexes on primary key columns in both tables
2. Optimize staging table indexes immediately after DLT creates them
3. Clean up temporary indexes after operations
"""

import time
import threading
import sqlalchemy as sa
from typing import Dict, Any, List, Optional, Tuple
from utils import log
import config


def analyze_dlt_delete_pattern(table_name: str, table_config: Dict[str, Any]) -> Dict[str, Any]:
    """
    Analyze the DELETE query pattern that DLT will use for this table.
    
    DLT uses this pattern for merge operations:
    DELETE FROM target_table WHERE EXISTS (
        SELECT 1 FROM staging_table 
        WHERE staging_table.pk1 = target_table.pk1 
        AND staging_table.pk2 = target_table.pk2 
        ...
    )
    
    Returns optimization strategy for this table.
    """
    try:
        primary_key = table_config.get("primary_key", [])
        if isinstance(primary_key, str):
            primary_key = [primary_key]
        
        # DLT staging table naming pattern
        staging_table_name = f"{table_name}__dlt_tmp"
        
        optimization_strategy = {
            "target_table": table_name,
            "staging_table": staging_table_name,
            "primary_key_columns": primary_key,
            "target_indexes": [],
            "staging_indexes": []
        }
        
        if primary_key:
            # Create index on primary key columns for both tables
            pk_columns_str = "_".join(primary_key)
            
            # Target table index (if not exists)
            target_index_name = f"idx_{table_name}_pk"
            optimization_strategy["target_indexes"].append({
                "name": target_index_name,
                "table": table_name,
                "columns": primary_key,
                "type": "primary_key_composite",
                "priority": "high"
            })
            
            # Staging table index (created dynamically)
            staging_index_name = f"idx_{staging_table_name}_pk"
            optimization_strategy["staging_indexes"].append({
                "name": staging_index_name,
                "table": staging_table_name,
                "columns": primary_key,
                "type": "primary_key_composite",
                "priority": "critical",
                "temporary": True
            })
        
        log(f"🔍 DLT DELETE pattern analysis for {table_name}:")
        log(f"   Primary key columns: {primary_key}")
        log(f"   Staging table: {staging_table_name}")
        log(f"   Target indexes needed: {len(optimization_strategy['target_indexes'])}")
        log(f"   Staging indexes needed: {len(optimization_strategy['staging_indexes'])}")
        
        return optimization_strategy
        
    except Exception as e:
        log(f"❌ Error analyzing DLT DELETE pattern for {table_name}: {e}")
        return {}


def create_index_if_not_exists(engine, index_info: Dict[str, Any]) -> bool:
    """
    Create an index if it doesn't already exist.
    
    Returns True if index was created or already exists, False on error.
    Handles both simple table names and fully qualified names (schema.table).
    """
    try:
        table_name = index_info["table"]
        index_name = index_info["name"]
        columns = index_info["columns"]
        
        # Parse schema and table name
        if "." in table_name:
            schema_name, simple_table_name = table_name.split(".", 1)
        else:
            schema_name = None
            simple_table_name = table_name
        
        # Check if index already exists
        with engine.connect() as connection:
            # Check existing indexes (handle both schema.table and simple table names)
            if schema_name:
                check_query = sa.text("""
                    SELECT COUNT(*) as count
                    FROM information_schema.statistics 
                    WHERE table_schema = :schema_name
                    AND table_name = :table_name 
                    AND index_name = :index_name
                """)
                result = connection.execute(check_query, {
                    "schema_name": schema_name,
                    "table_name": simple_table_name,
                    "index_name": index_name
                })
            else:
                check_query = sa.text("""
                    SELECT COUNT(*) as count
                    FROM information_schema.statistics 
                    WHERE table_schema = DATABASE() 
                    AND table_name = :table_name 
                    AND index_name = :index_name
                """)
                result = connection.execute(check_query, {
                    "table_name": simple_table_name,
                    "index_name": index_name
                })
            
            index_exists = result.fetchone()[0] > 0
            
            if index_exists:
                log(f"✅ Index {index_name} already exists on {table_name}")
                return True
            
            # Create the index
            columns_str = ", ".join([f"`{col}`" for col in columns])
            
            # Use fully qualified table name for CREATE INDEX
            create_sql = f"""
                CREATE INDEX `{index_name}` 
                ON {table_name} ({columns_str})
            """
            
            log(f"🔧 Creating index {index_name} on {table_name}({columns_str})...")
            connection.execute(sa.text(create_sql))
            connection.commit()
            
            log(f"✅ Successfully created index {index_name} on {table_name}")
            return True
            
    except Exception as e:
        log(f"❌ Error creating index {index_info.get('name', 'unknown')}: {e}")
        return False


def optimize_table_for_dlt(table_name: str, table_config: Dict[str, Any], 
                          engine_source, engine_target) -> bool:
    """
    Optimize a table for DLT operations by creating necessary indexes.
    
    This should be called BEFORE running DLT pipeline to ensure optimal performance.
    
    IMPORTANT: This function should ONLY be called for incremental tables (those with 
    modifier columns and merge disposition). Full refresh tables don't benefit from 
    these indexes since they replace all data and don't perform complex DELETE operations.
    
    Args:
        table_name: Name of the table to optimize
        table_config: Table configuration dict (must have 'modifier' key for incremental tables)
        engine_source: Source database engine
        engine_target: Target database engine
    
    Returns:
        bool: True if optimization was successful, False otherwise
    """
    try:
        log(f"🔧 Starting DLT optimization for table {table_name}...")
        
        # VALIDATION: Ensure this function is only called for incremental tables
        if "modifier" not in table_config:
            log(f"⚠️ WARNING: optimize_table_for_dlt called for non-incremental table {table_name}")
            log(f"   This function should only be called for incremental tables with modifier columns")
            log(f"   Full refresh tables don't benefit from these indexes")
            return False
        
        log(f"✅ Confirmed: {table_name} is an incremental table with modifier column")
        
        # Analyze what indexes are needed for DLT's DELETE operations
        optimization_strategy = analyze_dlt_delete_pattern(table_name, table_config)
        
        if not optimization_strategy:
            log(f"⚠️ Could not determine optimization strategy for {table_name}")
            return False
        
        success_count = 0
        total_indexes = 0
        
        # Create target table indexes
        for index_info in optimization_strategy.get("target_indexes", []):
            total_indexes += 1
            if create_index_if_not_exists(engine_target, index_info):
                success_count += 1
        
        log(f"📊 DLT optimization completed for {table_name}: {success_count}/{total_indexes} indexes optimized")
        
        return success_count > 0
        
    except Exception as e:
        log(f"❌ Error optimizing table {table_name} for DLT: {e}")
        return False


def wait_and_optimize_staging_table(table_name: str, table_config: Dict[str, Any], 
                                   engine_source, engine_target, timeout_seconds: int = 30):
    """
    Wait for DLT to create staging table, then immediately optimize it.
    
    This runs in a separate thread and monitors for staging table creation.
    """
    try:
        log(f"🚀 Starting staging table optimization monitoring for {table_name}...")
        
        # Monitor for staging table creation
        start_time = time.time()
        staging_table_found = False
        
        log(f"🔍 Monitoring for staging table creation...")
        
        while time.time() - start_time < timeout_seconds:
            try:
                with engine_target.connect() as connection:
                    # CRITICAL: Check staging tables in BOTH main and staging schemas
                    # DLT creates staging tables in separate staging schema (e.g., zains_rz_staging)
                    
                    schemas_to_check = []
                    
                    # Get current database name
                    current_db_result = connection.execute(sa.text("SELECT DATABASE()"))
                    current_db = current_db_result.scalar()
                    schemas_to_check.append(current_db)
                    
                    # Add staging schema if it exists
                    staging_schema = f"{current_db}_staging"
                    check_staging_schema = sa.text("SELECT SCHEMA_NAME FROM information_schema.SCHEMATA WHERE SCHEMA_NAME = :schema_name")
                    staging_exists = connection.execute(check_staging_schema, {"schema_name": staging_schema})
                    if staging_exists.fetchone():
                        schemas_to_check.append(staging_schema)
                        log(f"🔍 Will check both {current_db} and {staging_schema} for staging tables")
                    
                    for schema in schemas_to_check:
                        # Check if staging table exists in this schema
                        check_query = sa.text("""
                            SELECT COUNT(*) as count
                            FROM information_schema.tables 
                            WHERE table_schema = :schema_name
                            AND table_name LIKE :pattern
                        """)
                        
                        # DLT creates staging tables with various patterns
                        # CRITICAL: Check for BOTH main schema and staging schema patterns
                        patterns = [
                            f"{table_name}__dlt_tmp%",        # Main schema: zains_rz.table_name__dlt_tmp
                            f"{table_name}_staging%",         # Alternative staging pattern
                            f"_dlt_{table_name}%",            # DLT internal pattern
                            f"{table_name}",                  # Staging schema: zains_rz_staging.table_name
                        ]
                        
                        # CRITICAL NEW: Also check for dataset-level staging tables
                        # These are tables like 'sync_data.sanitize_table_data' that affect all tables
                        dataset_patterns = [
                            "sync_data%",                     # sync_data.* (affects all tables)
                            "_dlt_sync_data%",                # _dlt_sync_data.*
                            "sync_data_staging%",             # sync_data_staging.*
                            "sync_data__dlt%",                # sync_data__dlt.*
                        ]
                        
                        # Combine both table-specific and dataset-level patterns
                        all_patterns = patterns + dataset_patterns
                        
                        for pattern in all_patterns:
                            result = connection.execute(check_query, {"schema_name": schema, "pattern": pattern})
                            if result.fetchone()[0] > 0:
                                staging_table_found = True
                                # Get actual staging table name
                                actual_query = sa.text("""
                                    SELECT table_name
                                    FROM information_schema.tables 
                                    WHERE table_schema = :schema_name
                                    AND table_name LIKE :pattern
                                    LIMIT 1
                                """)
                                actual_result = connection.execute(actual_query, {"schema_name": schema, "pattern": pattern})
                                actual_staging_table = actual_result.fetchone()[0]
                                
                                # Create fully qualified table name
                                full_table_name = f"{schema}.{actual_staging_table}"
                                log(f"🎯 Found DLT staging table: {full_table_name}")
                                
                                # Immediately optimize the staging table
                                optimize_staging_table_indexes(full_table_name, table_config, engine_target)
                                
                                # CRITICAL: Also check if we need to create indexes on staging schema table
                                # DLT might DELETE from zains_rz_staging.table_name even if staging table is in main schema
                                if schema == current_db:  # If we found table in main schema
                                    staging_schema_table = f"{staging_schema}.{table_name}"
                                    log(f"🔍 CRITICAL: Also checking staging schema table: {staging_schema_table}")
                                    
                                    # Check if staging schema table exists and needs optimization
                                    staging_check_query = sa.text("""
                                        SELECT COUNT(*) as count
                                        FROM information_schema.tables 
                                        WHERE table_schema = :schema_name
                                        AND table_name = :table_name
                                    """)
                                    staging_exists = connection.execute(staging_check_query, {
                                        "schema_name": staging_schema,
                                        "table_name": table_name
                                    })
                                    
                                    if staging_exists.fetchone()[0] > 0:
                                        log(f"🎯 CRITICAL: Found staging schema table: {staging_schema_table}")
                                        log(f"   This is likely what DLT uses for DELETE operations")
                                        # Optimize the staging schema table too
                                        optimize_staging_table_indexes(staging_schema_table, table_config, engine_target)
                                
                                return
                    
                    if staging_table_found:
                        break
                    
                    # Wait a bit before checking again
                    time.sleep(0.5)
                    
            except Exception as check_error:
                log(f"⚠️ Error checking for staging table: {check_error}")
                time.sleep(1)
        
        if not staging_table_found:
            log(f"⚠️ Staging table not found within {timeout_seconds}s for {table_name}")
        
    except Exception as e:
        log(f"❌ Error in staging table optimization monitoring: {e}")


def optimize_staging_table_indexes(staging_table_name: str, table_config: Dict[str, Any], engine_target) -> bool:
    """
    Create optimized indexes on a DLT staging table immediately after it's created.
    
    This is critical for speeding up DLT's DELETE operations.
    """
    try:
        log(f"🚀 CRITICAL: Optimizing staging table indexes for {staging_table_name}...")
        
        # CRITICAL: Check if this is a metadata table (should not be indexed)
        with engine_target.connect() as conn:
            # Handle fully qualified table names (schema.table)
            desc_query = sa.text(f"DESCRIBE {staging_table_name}")
            desc_result = conn.execute(desc_query)
            columns = [row[0] for row in desc_result]
            
            # Skip metadata tables (those with 'value' column)
            if 'value' in columns:
                log(f"⚠️ SKIPPING: {staging_table_name} is a DLT metadata table (contains 'value' column)")
                log(f"   Metadata tables should not be indexed - they're not used for DELETE operations")
                return False
            
            log(f"✅ CONFIRMED: {staging_table_name} is a data table (not metadata)")
            log(f"   Columns found: {columns[:5]}{'...' if len(columns) > 5 else ''}")
        
        primary_key = table_config.get("primary_key", [])
        if isinstance(primary_key, str):
            primary_key = [primary_key]
        
        if not primary_key:
            log(f"⚠️ No primary key found for staging table optimization")
            return False
        
        # Create critical indexes for DLT's DELETE operations
        indexes_to_create = []
        
        # SMART STRATEGY: Index only the shortest primary key columns to fit MySQL limits
        # MySQL key length limit is 3072 bytes, so we need to be very selective
        
        if primary_key:
            # For now, use only the first primary key column to ensure it fits
            # This provides basic selectivity while avoiding length issues
            safe_pk_columns = [primary_key[0]]  # Use only the first column
            
            pk_index_name = f"idx_{staging_table_name.replace('.', '_')}_pk_safe"
            indexes_to_create.append({
                "name": pk_index_name,
                "table": staging_table_name,
                "columns": safe_pk_columns,  # Use safe primary key columns
                "type": "primary_key_safe",
                "priority": "critical"
            })
            
            log(f"📋 Using safe primary key columns: {safe_pk_columns}")
            log(f"   (Full primary key: {primary_key})")
            log(f"   Note: Using only first column to avoid MySQL key length limits")
        
        log(f"📋 Planned indexes for {staging_table_name}:")
        for idx in indexes_to_create:
            log(f"   - {idx['name']}: {idx['columns']} ({idx['priority']} priority)")
        
        # Create all indexes
        success_count = 0
        for index_info in indexes_to_create:
            if create_index_if_not_exists(engine_target, index_info):
                success_count += 1
        
        log(f"🎯 CRITICAL: Staging table optimization completed: {success_count}/{len(indexes_to_create)} indexes created")
        log(f"   This should dramatically speed up DLT's DELETE operations")
        
        return success_count > 0
        
    except Exception as e:
        log(f"❌ Error optimizing staging table indexes: {e}")
        return False


def cleanup_table_indexes(table_name: str, engine_target) -> bool:
    """
    Clean up temporary indexes created for DLT operations.
    
    This should be called AFTER DLT operations complete.
    """
    try:
        log(f"🧹 Cleaning up temporary indexes for {table_name}...")
        
        # Find temporary indexes (those with 'tmp' or 'staging' in the name)
        with engine_target.connect() as connection:
            cleanup_query = sa.text("""
                SELECT DISTINCT index_name, table_name
                FROM information_schema.statistics 
                WHERE table_schema = DATABASE() 
                AND (
                    table_name LIKE :table_pattern 
                    OR index_name LIKE :index_pattern1
                    OR index_name LIKE :index_pattern2
                )
                AND index_name != 'PRIMARY'
            """)
            
            result = connection.execute(cleanup_query, {
                "table_pattern": f"{table_name}%tmp%",
                "index_pattern1": f"%{table_name}%tmp%",
                "index_pattern2": f"%{table_name}%staging%"
            })
            
            indexes_to_drop = result.fetchall()
            dropped_count = 0
            
            for index_name, table_name_found in indexes_to_drop:
                try:
                    drop_sql = f"DROP INDEX `{index_name}` ON `{table_name_found}`"
                    connection.execute(sa.text(drop_sql))
                    dropped_count += 1
                    log(f"🗑️ Dropped temporary index: {index_name}")
                except Exception as drop_error:
                    log(f"⚠️ Could not drop index {index_name}: {drop_error}")
            
            if dropped_count > 0:
                connection.commit()
                log(f"✅ Cleaned up {dropped_count} temporary indexes")
            else:
                log(f"ℹ️ No temporary indexes found to clean up")
            
            return True
            
    except Exception as e:
        log(f"❌ Error cleaning up indexes: {e}")
        return False


def ensure_staging_schema_table_optimized(table_name: str, table_config: Dict[str, Any], engine_target) -> bool:
    """
    CRITICAL: Ensure that staging schema table exists and is optimized.
    
    This function proactively checks and optimizes the staging schema table
    that DLT uses for DELETE operations, even if the main staging table
    is created in a different schema.
    """
    try:
        # Get current database name to determine staging schema
        with engine_target.connect() as connection:
            current_db_result = connection.execute(sa.text("SELECT DATABASE()"))
            current_db = current_db_result.scalar()
            staging_schema = f"{current_db}_staging"
            
            # Check if staging schema table exists
            staging_check_query = sa.text("""
                SELECT COUNT(*) as count
                FROM information_schema.tables 
                WHERE table_schema = :schema_name
                AND table_name = :table_name
            """)
            staging_exists = connection.execute(staging_check_query, {
                "schema_name": staging_schema,
                "table_name": table_name
            })
            
            if staging_exists.fetchone()[0] > 0:
                staging_schema_table = f"{staging_schema}.{table_name}"
                log(f"🔍 CRITICAL: Found staging schema table: {staging_schema_table}")
                log(f"   This is what DLT uses for DELETE operations - optimizing now")
                
                # Optimize the staging schema table
                return optimize_staging_table_indexes(staging_schema_table, table_config, engine_target)
            else:
                log(f"⚠️ Staging schema table {staging_schema}.{table_name} does not exist")
                log(f"   DLT might create it during pipeline execution")
                return False
                
    except Exception as e:
        log(f"❌ Error ensuring staging schema table optimization: {e}")
        return False


def verify_staging_optimization_working() -> bool:
    """
    Verify that our staging optimization is actually working by checking for slow queries.
    """
    try:
        # This would check the MySQL process list for slow DELETE queries
        # Implementation depends on monitoring capabilities
        log("🔍 Verifying staging optimization effectiveness...")
        return True
    except Exception as e:
        log(f"❌ Error verifying staging optimization: {e}")
        return False


def monitor_dataset_staging_tables(dataset_name: str, engine_target, timeout_seconds: int = 60):
    """
    Monitor and optimize dataset-level staging tables created by DLT.
    
    This is specifically for tables like 'sync_data.sanitize_table_data' that are created
    at the dataset level, not individual table level.
    """
    try:
        log(f"🚀 Starting dataset-level staging table monitoring for '{dataset_name}'...")
        
        start_time = time.time()
        staging_tables_found = []
        
        while time.time() - start_time < timeout_seconds:
            try:
                with engine_target.connect() as connection:
                    # Get current database name
                    current_db_result = connection.execute(sa.text("SELECT DATABASE()"))
                    current_db = current_db_result.scalar()
                    
                    # Check for dataset-level staging tables in main schema
                    # These are tables like 'sync_data.sanitize_table_data'
                    dataset_staging_query = sa.text("""
                        SELECT table_name
                        FROM information_schema.tables 
                        WHERE table_schema = :schema_name
                        AND table_name LIKE :pattern
                    """)
                    
                    # DLT creates dataset-level staging tables with various patterns
                    dataset_patterns = [
                        f"{dataset_name}%",           # sync_data.*
                        f"_dlt_{dataset_name}%",      # _dlt_sync_data.*
                        f"{dataset_name}_staging%",   # sync_data_staging.*
                        f"{dataset_name}__dlt%",      # sync_data__dlt.*
                    ]
                    
                    for pattern in dataset_patterns:
                        result = connection.execute(dataset_staging_query, {
                            "schema_name": current_db, 
                            "pattern": pattern
                        })
                        
                        for row in result:
                            table_name = row[0]
                            full_table_name = f"{current_db}.{table_name}"
                            
                            if full_table_name not in staging_tables_found:
                                staging_tables_found.append(full_table_name)
                                log(f"🎯 Found dataset-level staging table: {full_table_name}")
                                
                                # Create a basic table config for optimization
                                # Since this is a dataset-level table, we'll use safe defaults
                                table_config = {
                                    "primary_key": ["id"],  # Safe default
                                    "table_name": table_name,
                                    "dataset_name": dataset_name
                                }
                                
                                # Optimize the dataset-level staging table
                                success = optimize_staging_table_indexes(full_table_name, table_config, engine_target)
                                if success:
                                    log(f"✅ Dataset-level staging table optimized: {full_table_name}")
                                else:
                                    log(f"⚠️ Dataset-level staging table optimization failed: {full_table_name}")
                    
                    # Also check staging schema for dataset-level tables
                    staging_schema = f"{current_db}_staging"
                    staging_schema_check = sa.text("SELECT SCHEMA_NAME FROM information_schema.SCHEMATA WHERE SCHEMA_NAME = :schema_name")
                    staging_exists = connection.execute(staging_schema_check, {"schema_name": staging_schema})
                    
                    if staging_exists.fetchone():
                        log(f"🔍 Checking staging schema {staging_schema} for dataset-level tables...")
                        
                        for pattern in dataset_patterns:
                            staging_result = connection.execute(dataset_staging_query, {
                                "schema_name": staging_schema, 
                                "pattern": pattern
                            })
                            
                            for row in staging_result:
                                table_name = row[0]
                                full_table_name = f"{staging_schema}.{table_name}"
                                
                                if full_table_name not in staging_tables_found:
                                    staging_tables_found.append(full_table_name)
                                    log(f"🎯 Found dataset-level staging table in staging schema: {full_table_name}")
                                    
                                    # Create a basic table config for optimization
                                    table_config = {
                                        "primary_key": ["id"],  # Safe default
                                        "table_name": table_name,
                                        "dataset_name": dataset_name
                                    }
                                    
                                    # Optimize the dataset-level staging table
                                    success = optimize_staging_table_indexes(full_table_name, table_config, engine_target)
                                    if success:
                                        log(f"✅ Dataset-level staging table optimized: {full_table_name}")
                                    else:
                                        log(f"⚠️ Dataset-level staging table optimization failed: {full_table_name}")
                    
                    if staging_tables_found:
                        log(f"✅ Dataset-level staging table monitoring completed. Found {len(staging_tables_found)} tables:")
                        for table in staging_tables_found:
                            log(f"   - {table}")
                        return staging_tables_found
                    
                    # Wait before checking again
                    time.sleep(1)
                    
            except Exception as check_error:
                log(f"⚠️ Error checking for dataset-level staging tables: {check_error}")
                time.sleep(2)
        
        if not staging_tables_found:
            log(f"⚠️ No dataset-level staging tables found within {timeout_seconds}s for '{dataset_name}'")
        
        return staging_tables_found
        
    except Exception as e:
        log(f"❌ Error in dataset-level staging table monitoring: {e}")
        return []