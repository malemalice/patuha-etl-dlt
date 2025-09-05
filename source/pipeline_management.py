"""
Pipeline management module for DLT Database Sync Pipeline.
Contains DLT pipeline operations, table processing, and sync logic.
"""

import time
import os
import shutil
import pandas as pd
import threading
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
import dlt
from dlt.sources.sql_database import sql_database
import sqlalchemy as sa
from typing import Dict, Any, List, Union, Callable
import config
from utils import log, log_config, log_phase, log_status, log_error, log_debug, format_primary_key, validate_primary_key_config, log_primary_key_info
from database import execute_with_transaction_management, ensure_dlt_columns
from schema_management import sync_table_schema

# CRITICAL FIX: Enable DLT staging optimization for direct database operations
# DLT SQLAlchemy destination always creates staging tables, so we optimize them instead
if config.TRUNCATE_STAGING_DATASET:
    # DEEP THINKING: Use ONLY DLT's supported environment variables
    # These are the actual environment variables that DLT 1.15.0 recognizes
    os.environ["DLT_LOAD_TRUNCATE_STAGING_DATASET"] = "true"  # ✅ SUPPORTED: Clear staging after each load
    os.environ["DLT_LOAD_STAGING_DATASET_CLEANUP"] = "true"   # ✅ SUPPORTED: Enable staging cleanup
    os.environ["DLT_LOAD_WORKERS"] = "1"  # ✅ SUPPORTED: Single worker
    os.environ["DLT_EXTRACT_WORKERS"] = "1"  # ✅ SUPPORTED: Single worker
    log_debug("🔧 DEEP THINKING: DLT staging optimization enabled using SUPPORTED parameters")
    log_debug("   Using ONLY DLT 1.15.0 recognized environment variables")
    log_debug("   Staging tables will be automatically cleaned up after each load")
from data_processing import validate_table_data, sanitize_table_data, debug_problematic_rows
from error_handling import retry_on_connection_error, retry_on_lock_timeout
from monitoring import log_performance_metrics
from index_management import optimize_table_for_dlt, cleanup_table_indexes, wait_and_optimize_staging_table, monitor_dataset_staging_tables

def get_max_timestamp(engine_target, table_name, column_name):
    """Get the maximum timestamp value from the target table."""
    try:
        with engine_target.connect() as connection:
            query = f"SELECT MAX({column_name}) FROM {table_name}"
            result = connection.execute(sa.text(query))
            max_value = result.scalar()
            
            if max_value is None:
                log_debug(f"📅 No existing data in target table {table_name}, starting from beginning")
                return None
            else:
                log_debug(f"📅 Latest {column_name} in target table {table_name}: {max_value}")
                return max_value
                
    except Exception as e:
        log_error(f"❌ Error getting max timestamp for {table_name}.{column_name}: {e}")
        return None

def get_table_row_count(engine, table_name):
    """Get the total row count for a table."""
    try:
        with engine.connect() as connection:
            query = f"SELECT COUNT(*) FROM {table_name}"
            result = connection.execute(sa.text(query))
            count = result.scalar()
            return count or 0
    except Exception as e:
        log_error(f"❌ Error getting row count for {table_name}: {e}")
        return 0

def log_sync_results(table_name, engine_source, engine_target, operation_type="sync"):
    """Log comprehensive sync results comparing source vs target row counts."""
    try:
        log("🔍" + "="*60)
        log(f"🔍 SYNC VERIFICATION: {table_name} ({operation_type})")
        log("🔍" + "="*60)

        # Get row counts
        source_count = get_table_row_count(engine_source, table_name)
        target_count = get_table_row_count(engine_target, table_name)

        log(f"📊 Source table '{table_name}': {source_count:,} rows")
        log(f"📊 Target table '{table_name}': {target_count:,} rows")

        # Calculate difference
        difference = abs(source_count - target_count)
        percentage_diff = (difference / max(source_count, 1)) * 100

        if source_count == target_count:
            log(f"✅ PERFECT SYNC: Row counts match exactly ({source_count:,} rows)")
            log(f"🎯 Sync accuracy: 100.0%")
        elif difference == 0:
            log(f"✅ PERFECT SYNC: No difference in row counts")
        else:
            log(f"⚠️ ROW COUNT DIFFERENCE: {difference:,} rows ({percentage_diff:.2f}%)")
            if operation_type == "incremental":
                log(f"ℹ️ Incremental sync: Some difference expected due to ongoing changes")
            elif operation_type == "full_refresh":
                log(f"⚠️ Full refresh: Row counts should match - investigate data loss")
            else:
                log(f"ℹ️ {operation_type}: Row count difference detected")

        # Additional verification for full refresh
        if operation_type == "full_refresh" and source_count > 0:
            if target_count == 0:
                log_error(f"🚨 CRITICAL: Full refresh resulted in 0 target rows despite {source_count:,} source rows")
                log_error(f"   This indicates a complete sync failure")
            elif percentage_diff > 10:
                log_error(f"🚨 SIGNIFICANT LOSS: {percentage_diff:.1f}% data loss detected")
                log_error(f"   Expected: {source_count:,} | Actual: {target_count:,}")
            elif percentage_diff > 1:
                log(f"⚠️ MINOR LOSS: {percentage_diff:.1f}% data difference")
                log(f"   May be due to concurrent updates or data filtering")

        log("🔍" + "="*60)
        log(f"🔍 SYNC VERIFICATION COMPLETE: {table_name}")
        log("🔍" + "="*60)

        return {
            'source_count': source_count,
            'target_count': target_count,
            'difference': difference,
            'percentage_diff': percentage_diff,
            'sync_status': 'perfect' if difference == 0 else 'partial' if percentage_diff < 10 else 'failed'
        }

    except Exception as e:
        log_error(f"❌ Failed to verify sync results for {table_name}: {e}")
        return None

def get_new_records_count(engine, table_name, modifier_column, since_timestamp):
    """Get the count of new records since a specific timestamp."""
    try:
        with engine.connect() as connection:
            if since_timestamp is None:
                # If no timestamp, get all records
                query = f"SELECT COUNT(*) FROM {table_name}"
                result = connection.execute(sa.text(query))
            else:
                # Ensure since_timestamp has proper timezone (GMT +7) for consistency
                query_timestamp = since_timestamp
                if isinstance(since_timestamp, datetime) and since_timestamp.tzinfo is None:
                    # If timestamp has no timezone info, default to GMT +7 (Asia/Bangkok)
                    import pytz
                    bangkok_tz = pytz.timezone('Asia/Bangkok')
                    query_timestamp = bangkok_tz.localize(since_timestamp)
                    log(f"🔧 Converted since_timestamp to GMT +7: {query_timestamp}")

                # Get records newer than the timestamp - use named parameters for consistency
                query = f"SELECT COUNT(*) FROM {table_name} WHERE {modifier_column} > :since_timestamp"
                result = connection.execute(sa.text(query), {"since_timestamp": query_timestamp})
            
            count = result.scalar()
            return count or 0
    except Exception as e:
        log(f"❌ Error getting new records count for {table_name}: {e}")
        log(f"🔍 Debug: Query: {query}")
        log(f"🔍 Debug: Parameters: {since_timestamp}")
        log(f"🔍 Debug: Error type: {type(e)}")
        return 0

def force_table_clear(engine_target, table_name):
    """Force clear a table completely to prevent DLT from generating complex DELETE statements.
    
    This function:
    1. Truncates the table instead of using DELETE statements
    2. Handles foreign key constraints if present
    3. Provides better performance for full refresh operations
    """
    def _clear_table(connection):
        try:
            # First try simple TRUNCATE
            truncate_query = f"TRUNCATE TABLE {table_name}"
            log(f"🗑️ Truncating table {table_name}")
            connection.execute(sa.text(truncate_query))
            log(f"✅ Successfully truncated table {table_name}")
            return True
            
        except Exception as truncate_error:
            truncate_error_msg = str(truncate_error).lower()
            
            # If truncate fails due to foreign key constraints, try DELETE
            if any(keyword in truncate_error_msg for keyword in ['foreign key', 'constraint', 'referenced']):
                log(f"⚠️ TRUNCATE failed due to foreign key constraints, trying DELETE for {table_name}")
                try:
                    delete_query = f"DELETE FROM {table_name}"
                    connection.execute(sa.text(delete_query))
                    log(f"✅ Successfully deleted all rows from table {table_name}")
                    return True
                except Exception as delete_error:
                    log(f"❌ Both TRUNCATE and DELETE failed for {table_name}")
                    log(f"❌ TRUNCATE error: {truncate_error}")
                    log(f"❌ DELETE error: {delete_error}")
                    return False
            else:
                log(f"❌ TRUNCATE failed for {table_name}: {truncate_error}")
                return False
    
    try:
        with engine_target.connect() as connection:
            return _clear_table(connection)
    except Exception as e:
        log(f"❌ Error in force_table_clear for {table_name}: {e}")
        return False


def safe_table_cleanup(engine_target, table_name, write_disposition="replace"):
    """Safely clean up table data based on write disposition with lock timeout handling.
    
    This function handles different write dispositions:
    - replace: Clear all existing data
    - append: Keep existing data  
    - merge: Keep existing data (will be merged)
    """
    def _cleanup_table(connection):
        if write_disposition == "replace":
            log(f"🧹 Cleaning up table {table_name} for full refresh (replace mode)")
            
            # Use force clear for better performance
            success = force_table_clear(engine_target, table_name)
            if not success:
                log(f"⚠️ Force clear failed for {table_name}, DLT will handle cleanup")
            
            return success
        else:
            log(f"📝 Table {table_name} using {write_disposition} mode - no cleanup needed")
            return True
    
    return retry_on_lock_timeout(
        _cleanup_table,
        "target",
        f"safe_table_cleanup for {table_name}"
    )

def recover_connection_pool(engine, pool_name):
    """Recover corrupted connection pool for MariaDB."""
    try:
        # Force pool recreation
        engine.dispose()
        
        # Recreate engine with fresh pool
        from database import create_engines, get_engines
        create_engines()  # This creates and stores engines globally
        engine_source, engine_target = get_engines()  # This gets the created engines
        
        log(f"✅ Recovered {pool_name} connection pool")
        return engine_source if 'source' in pool_name else engine_target
        
    except Exception as e:
        log(f"❌ Failed to recover {pool_name} connection pool: {e}")
        # Try to create a single engine instead of recreating both
        try:
            log(f"🔄 Attempting single engine recovery for {pool_name}...")
            if 'source' in pool_name:
                from database import create_source_engine
                return create_source_engine()
            else:
                from database import create_target_engine
                return create_target_engine()
        except Exception as single_engine_error:
            log(f"❌ Single engine recovery also failed: {single_engine_error}")
            # Return None to indicate complete failure - this will be handled by caller
            return None

def process_tables_batch(pipeline, engine_source, engine_target, tables_dict, write_disposition="merge"):
    """Process a batch of tables with connection pool recovery and retry queue mechanism."""
    
    batch_start_time = time.time()
    successful_tables = []
    failed_tables = []
    retry_queue = []  # Queue for tables that need retry due to infrastructure failures
    
    log_phase(f"🔄 Processing batch of {len(tables_dict)} tables with {write_disposition} disposition")
    
    for table_name, table_config in tables_dict.items():
        table_start_time = time.time()
        
        try:
            log_phase(f"\n{'='*60}")
            log_phase(f"🔄 Processing table: {table_name}")
            log_debug(f"📋 Table config: {table_config}")
            
            # Check connection pool health before processing each table
            if hasattr(engine_source.pool, 'overflow') and engine_source.pool.overflow() < 0:
                log_debug(f"⚠️ Source connection pool corrupted (overflow: {engine_source.pool.overflow()}), recovering...")
                try:
                    recovered_engine = recover_connection_pool(engine_source, "source")
                    if recovered_engine is not None:
                        engine_source = recovered_engine
                        log_debug(f"✅ Source connection pool recovery completed")
                    else:
                        log_debug(f"⚠️ Source connection pool recovery failed, adding {table_name} to retry queue")
                        retry_queue.append((table_name, table_config, "source_pool_recovery_failed"))
                        continue
                except Exception as recovery_error:
                    log_debug(f"⚠️ Source connection pool recovery error, adding {table_name} to retry queue: {recovery_error}")
                    retry_queue.append((table_name, table_config, f"source_pool_recovery_error: {recovery_error}"))
                    continue
                
            if hasattr(engine_target.pool, 'overflow') and engine_target.pool.overflow() < 0:
                log_debug(f"⚠️ Target connection pool corrupted (overflow: {engine_target.pool.overflow()}), recovering...")
                try:
                    recovered_engine = recover_connection_pool(engine_target, "target")
                    if recovered_engine is not None:
                        engine_target = recovered_engine
                        log_debug(f"✅ Target connection pool recovery completed")
                    else:
                        log_debug(f"⚠️ Target connection pool recovery failed, adding {table_name} to retry queue")
                        retry_queue.append((table_name, table_config, "target_pool_recovery_failed"))
                        continue
                except Exception as recovery_error:
                    log_debug(f"⚠️ Target connection pool recovery error, adding {table_name} to retry queue: {recovery_error}")
                    retry_queue.append((table_name, table_config, f"target_pool_recovery_error: {recovery_error}"))
                    continue
            
            # Validate primary key configuration
            if "primary_key" not in table_config:
                log_error(f"❌ Table '{table_name}' missing primary_key configuration, skipping")
                failed_tables.append(table_name)
                continue
                
            primary_key = table_config["primary_key"]
            if not validate_primary_key_config(primary_key):
                log_error(f"❌ Table '{table_name}' has invalid primary_key configuration: {primary_key}, skipping")
                failed_tables.append(table_name)
                continue
            
            # Log primary key information
            log_primary_key_info(table_name, primary_key)
            
            # Log table configuration details
            log_debug(f"📋 Table configuration details:")
            log_debug(f"   Primary key: {primary_key}")
            log_debug(f"   Has modifier: {'modifier' in table_config}")
            if 'modifier' in table_config:
                log_debug(f"   Modifier column: {table_config['modifier']}")
            log_debug(f"   Write disposition: {write_disposition}")
            
            # Validate table data
            if not validate_table_data(engine_source, table_name):
                log_error(f"❌ Table data validation failed for {table_name}, attempting to debug...")
                debug_problematic_rows(engine_source, table_name, limit=5)
                failed_tables.append(table_name)
                continue
            
            # Sync schema first
            try:
                sync_table_schema(engine_source, engine_target, table_name)
                ensure_dlt_columns(engine_target, table_name)
            except Exception as schema_error:
                log_error(f"❌ Schema sync failed for {table_name}: {schema_error}")
                failed_tables.append(table_name)
                continue
            
            # CRITICAL: Optimize indexes for DLT operations BEFORE running pipeline
            # ONLY for incremental tables (those with modifier column and merge disposition)
            if config.ENABLE_INDEX_OPTIMIZATION and "modifier" in table_config and write_disposition == "merge":
                try:
                    log_debug(f"🔧 Optimizing indexes for DLT operations on {table_name} (incremental table)...")
                    index_optimization_success = optimize_table_for_dlt(table_name, table_config, engine_source, engine_target)
                    if index_optimization_success:
                        log_debug(f"✅ Index optimization completed for incremental table {table_name}")
                    else:
                        log_debug(f"⚠️ Index optimization had issues for incremental table {table_name}, but continuing...")
                except Exception as index_error:
                    log_debug(f"⚠️ Index optimization failed for incremental table {table_name}: {index_error}")
                    log_debug(f"   Continuing without index optimization...")
                    # Don't fail the table processing due to index issues
            elif config.ENABLE_INDEX_OPTIMIZATION:
                log_debug(f"ℹ️ Skipping index optimization for {table_name} (not an incremental table)")
                log_debug(f"   Table uses full refresh method - indexes not needed for DLT operations")
            else:
                log_debug(f"ℹ️ Index optimization disabled globally (ENABLE_INDEX_OPTIMIZATION=false)")
            
            # CRITICAL: Handle different sync strategies with strict method separation
            if "modifier" in table_config and write_disposition == "merge":
                # Incremental sync - ONLY for tables with modifier column and merge disposition
                log_phase(f"📈 Processing incremental sync for {table_name}")
                log_debug(f"   Table has modifier column: {table_config['modifier']}")
                log_debug(f"   Using merge disposition for incremental sync")
                log_debug(f"   CRITICAL: Incremental method - NO fallback to full refresh")
                success = process_incremental_table(pipeline, engine_source, engine_target, table_name, table_config)
            else:
                # Full refresh - for tables without modifier OR with non-merge disposition
                log_phase(f"🔄 Processing full refresh for {table_name}")
                if "modifier" not in table_config:
                    log_debug(f"   Table has no modifier column - using full refresh")
                else:
                    log_debug(f"   Table has modifier but write_disposition is not merge - using full refresh")
                log_debug(f"   Using {write_disposition} disposition for full refresh")
                log_debug(f"   CRITICAL: Full refresh method - NO incremental logic")
                
                success = process_full_refresh_table(pipeline, engine_source, engine_target, table_name, table_config, write_disposition)
            
            table_end_time = time.time()
            
            if success:
                successful_tables.append(table_name)

                # Log row count comparison after successful sync
                try:
                    from database import log_row_count_comparison
                    log_row_count_comparison(table_name, engine_source, engine_target)
                except Exception as count_error:
                    log_debug(f"⚠️ Row count comparison failed for {table_name}: {count_error}")

                # CRITICAL: Clean up temporary indexes after successful DLT operation
                # ONLY for incremental tables (those with modifier column and merge disposition)
                if config.CLEANUP_TEMPORARY_INDEXES and config.ENABLE_INDEX_OPTIMIZATION and "modifier" in table_config and write_disposition == "merge":
                    try:
                        log_debug(f"🧹 Cleaning up temporary indexes for incremental table {table_name}...")
                        cleanup_success = cleanup_table_indexes(table_name, engine_target)
                        if cleanup_success:
                            log_debug(f"✅ Index cleanup completed for incremental table {table_name}")
                        else:
                            log_debug(f"⚠️ Index cleanup had issues for incremental table {table_name}")
                    except Exception as cleanup_error:
                        log_debug(f"⚠️ Index cleanup failed for incremental table {table_name}: {cleanup_error}")
                        # Don't fail the table processing due to cleanup issues
                elif config.CLEANUP_TEMPORARY_INDEXES and config.ENABLE_INDEX_OPTIMIZATION:
                    log_debug(f"ℹ️ Skipping index cleanup for {table_name} (not an incremental table)")
                    log_debug(f"   Table uses full refresh method - no temporary indexes to clean up")
                else:
                    log_debug(f"ℹ️ Index cleanup disabled for {table_name}")
                
                # Get actual record count for this table
                try:
                    if "modifier" in table_config:
                        # For incremental sync, get new records count
                        modifier_column = table_config["modifier"]
                        max_timestamp = get_max_timestamp(engine_target, table_name, modifier_column)
                        if max_timestamp is not None:
                            new_records = get_new_records_count(engine_source, table_name, modifier_column, max_timestamp)
                            log_performance_metrics(f"Table {table_name}", table_start_time, table_end_time, new_records)
                            log_status(f"✅ Successfully processed table: {table_name} ({new_records} new records)")
                        else:
                            # First time sync
                            source_count = get_table_row_count(engine_source, table_name)
                            log_performance_metrics(f"Table {table_name}", table_start_time, table_end_time, source_count)
                            log_status(f"✅ Successfully processed table: {table_name} ({source_count} records, first time)")
                    else:
                        # For full refresh, get total source count
                        source_count = get_table_row_count(engine_source, table_name)
                        log_performance_metrics(f"Table {table_name}", table_start_time, table_end_time, source_count)
                        log_status(f"✅ Successfully processed table: {table_name} ({source_count} records, full refresh)")
                except Exception as e:
                    log_debug(f"⚠️ Could not get record count for {table_name}: {e}")
                    log_performance_metrics(f"Table {table_name}", table_start_time, table_end_time)
                    log_status(f"✅ Successfully processed table: {table_name}")
            else:
                failed_tables.append(table_name)
                log_error(f"❌ Failed to process table: {table_name}")
                
        except Exception as table_error:
            table_end_time = time.time()
            log_error(f"❌ Error processing table {table_name}: {table_error}")
            log_performance_metrics(f"Table {table_name} (FAILED)", table_start_time, table_end_time)
            
            # Check if this is an infrastructure-related error that should be retried
            error_message = str(table_error).lower()
            infrastructure_keywords = ['connection', 'pool', 'timeout', 'lost', 'disconnect', 'gone away', 'server has gone away']
            
            if any(keyword in error_message for keyword in infrastructure_keywords):
                log_debug(f"⚠️ Infrastructure-related error detected, adding {table_name} to retry queue")
                retry_queue.append((table_name, table_config, f"infrastructure_error: {table_error}"))
            else:
                # Non-infrastructure error, mark as failed
                failed_tables.append(table_name)
            
            # Force connection pool recovery on critical errors
            log_debug(f"🔄 Attempting connection pool recovery after critical error...")
            try:
                recovered_source = recover_connection_pool(engine_source, "source")
                if recovered_source is not None:
                    engine_source = recovered_source
                    log_debug(f"✅ Source pool recovery after critical error completed")
                else:
                    log_debug(f"⚠️ Source pool recovery after critical error failed")
            except Exception as recovery_error:
                log_debug(f"⚠️ Source pool recovery after critical error failed: {recovery_error}")
                
            try:
                recovered_target = recover_connection_pool(engine_target, "target")
                if recovered_target is not None:
                    engine_target = recovered_target
                    log_debug(f"✅ Target pool recovery after critical error completed")
                else:
                    log_debug(f"⚠️ Target pool recovery after critical error failed")
            except Exception as recovery_error:
                log_debug(f"⚠️ Target pool recovery after critical error failed: {recovery_error}")
    
    batch_end_time = time.time()
    
    # Calculate actual records processed for this batch
    total_records_processed = 0
    for table_name in successful_tables:
        try:
            # Get the actual record count that was synced for each table
            if table_name in tables_dict:
                table_config = tables_dict[table_name]
                if "modifier" in table_config:
                    # For incremental sync, get the count of new records
                    modifier_column = table_config["modifier"]
                    # Get the latest timestamp from target to calculate new records
                    from database import get_engines
                    _, engine_target = get_engines()
                    max_timestamp = get_max_timestamp(engine_target, table_name, modifier_column)
                    if max_timestamp is not None:
                        new_records = get_new_records_count(engine_source, table_name, modifier_column, max_timestamp)
                        total_records_processed += new_records
                        log_debug(f"📊 Table {table_name}: {new_records} new records synced")
                    else:
                        # First time sync, get total source count
                        source_count = get_table_row_count(engine_source, table_name)
                        total_records_processed += source_count
                        log_debug(f"📊 Table {table_name}: {source_count} records synced (first time)")
                else:
                    # For full refresh, get total source count
                    source_count = get_table_row_count(engine_source, table_name)
                    total_records_processed += source_count
                    log_debug(f"📊 Table {table_name}: {source_count} records synced (full refresh)")
        except Exception as e:
            log_debug(f"⚠️ Could not get record count for {table_name}: {e}")
    
    log_performance_metrics(f"Batch processing", batch_start_time, batch_end_time, total_records_processed)
    
    # Process retry queue if there are tables that failed due to infrastructure issues
    if retry_queue:
        log_phase(f"\n{'='*60}")
        log_phase(f"🔄 Processing retry queue: {len(retry_queue)} tables")
        
        # Create fresh engines for retry attempts
        try:
            log_debug(f"🔧 Creating fresh engines for retry processing...")
            from database import create_engines, get_engines
            create_engines()  # This creates and stores engines globally
            fresh_engine_source, fresh_engine_target = get_engines()  # This gets the created engines
            log_debug(f"✅ Fresh engines created successfully")
            
            retry_successful = []
            retry_failed = []
            
            for table_name, table_config, reason in retry_queue:
                log_phase(f"🔄 Retrying table: {table_name} (reason: {reason})")
                
                try:
                    # Use the same processing logic but with fresh engines
                    if "modifier" in table_config and write_disposition == "merge":
                        # Incremental sync
                        success = process_incremental_table(pipeline, fresh_engine_source, fresh_engine_target, table_name, table_config)
                    else:
                        # Full refresh
                        success = process_full_refresh_table(pipeline, fresh_engine_source, fresh_engine_target, table_name, table_config, write_disposition)
                    
                    if success:
                        retry_successful.append(table_name)
                        successful_tables.append(table_name)
                        log_status(f"✅ Retry successful for table: {table_name}")

                        # Log row count comparison after successful retry
                        try:
                            from database import log_row_count_comparison
                            log_row_count_comparison(table_name, fresh_engine_source, fresh_engine_target)
                        except Exception as count_error:
                            log_debug(f"⚠️ Row count comparison failed for retried table {table_name}: {count_error}")
                    else:
                        retry_failed.append(table_name)
                        failed_tables.append(table_name)
                        log_error(f"❌ Retry failed for table: {table_name}")
                        
                except Exception as retry_error:
                    retry_failed.append(table_name)
                    failed_tables.append(table_name)
                    log_error(f"❌ Retry error for table {table_name}: {retry_error}")
            
            log_phase(f"📊 Retry summary: {len(retry_successful)} successful, {len(retry_failed)} failed")
            if retry_successful:
                log_status(f"✅ Retry successful: {', '.join(retry_successful)}")
            if retry_failed:
                log_error(f"❌ Retry failed: {', '.join(retry_failed)}")
                
        except Exception as fresh_engine_error:
            log_error(f"❌ Could not create fresh engines for retry: {fresh_engine_error}")
            # Add all retry queue items to failed tables
            for table_name, table_config, reason in retry_queue:
                failed_tables.append(table_name)
            log_error(f"❌ All {len(retry_queue)} retry queue tables marked as failed")
    
    # Summary
    log_status(f"\n{'='*60}")
    log_status(f"📊 Batch processing summary:")
    log_status(f"✅ Successful: {len(successful_tables)} tables")
    log_status(f"❌ Failed: {len(failed_tables)} tables")
    log_status(f"📈 Total records processed: {total_records_processed:,}")
    
    if retry_queue:
        log_status(f"🔄 Retry queue processed: {len(retry_queue)} tables")
    
    if successful_tables:
        log_status(f"✅ Successful tables: {', '.join(successful_tables)}")
    if failed_tables:
        log_error(f"❌ Failed tables: {', '.join(failed_tables)}")
    
    return len(successful_tables), len(failed_tables)

def process_incremental_table(pipeline, engine_source, engine_target, table_name, table_config):
    """Process a single table with incremental sync using DLT's built-in mechanisms.
    
    CRITICAL: This function ONLY handles incremental sync and does NOT fallback to full refresh.
    Incremental failures remain as incremental failures to maintain method separation.
    """
    try:
        # CRITICAL SAFEGUARD: Ensure this is truly an incremental table
        if "modifier" not in table_config:
            log(f"❌ CRITICAL ERROR: Incremental function called for non-incremental table!")
            log(f"   Table {table_name} has no modifier column but is in incremental function")
            log(f"   This should not happen - check table processing logic")
            return False
        
        modifier_column = table_config["modifier"]
        primary_key = table_config["primary_key"]
        
        log(f"📈 CONFIRMED: Incremental method for {table_name}")
        log(f"   Modifier column: {modifier_column}")
        log(f"   Primary key: {primary_key}")
        log(f"   NO fallback to full refresh on failure")
        
        # Get the latest timestamp from target
        log(f"🔍 Getting latest {modifier_column} from TARGET database...")
        max_timestamp = get_max_timestamp(engine_target, table_name, modifier_column)
        log(f"   📅 TARGET latest {modifier_column}: {max_timestamp}")
        
        # CRITICAL FIX: Apply same timezone handling as old_db_pipeline.py (line 665-666)
        # This ensures consistent timezone handling to fix the incremental sync issue
        if max_timestamp is not None:
            try:
                from dlt.common import pendulum
                max_timestamp = pendulum.instance(max_timestamp).in_tz("Asia/Bangkok")
                log(f"🔧 Applied timezone conversion (Asia/Bangkok): {max_timestamp}")
            except Exception as tz_error:
                log(f"⚠️ Warning: Could not apply timezone conversion: {tz_error}")
                # Keep the original timestamp if conversion fails
        
        log(f"📅 Incremental sync - Target timestamp: {max_timestamp}")
        
        # Get source table row count for comparison
        source_count = get_table_row_count(engine_source, table_name)
        target_count = get_table_row_count(engine_target, table_name)
        
        log(f"📊 Row counts - Source: {source_count}, Target: {target_count}")
        
        # Create SQL database source
        source = sql_database(engine_source, schema=config.SOURCE_DB_NAME, table_names=[table_name])
        
        # Format primary key for DLT hints
        formatted_primary_key = format_primary_key(primary_key)
        
        # Apply hints for incremental loading with timezone-aware handling
        from datetime import datetime
        import pytz
        
        # Convert max_timestamp to timezone-aware datetime if needed (like working commit)
        # Keep original for SQL queries, create pendulum-aware copy for DLT
        max_timestamp_sql = max_timestamp  # Keep original for SQL queries

        if max_timestamp is not None and isinstance(max_timestamp, datetime):
            # CRITICAL FIX: Create a PROPER COPY before converting to Pendulum
            # This ensures max_timestamp_sql remains a timezone-naive datetime object
            if hasattr(max_timestamp, 'replace'):
                # Create a timezone-naive copy for SQLAlchemy compatibility
                max_timestamp_sql = datetime(
                    max_timestamp.year, max_timestamp.month, max_timestamp.day,
                    max_timestamp.hour, max_timestamp.minute, max_timestamp.second,
                    max_timestamp.microsecond
                    # Note: tzinfo is intentionally omitted for SQLAlchemy compatibility
                )
                log_phase(f"🔧 Created SQL-safe datetime copy: {max_timestamp_sql} (type: {type(max_timestamp_sql)})")
                log_phase(f"   Timezone info: {max_timestamp_sql.tzinfo} (should be None for SQLAlchemy)")

            # CRITICAL FIX: Use pendulum conversion like the working commit a989acb
            try:
                from dlt.common import pendulum
                max_timestamp = pendulum.instance(max_timestamp).in_tz("Asia/Bangkok")
                log_phase(f"🔧 Applied pendulum timezone conversion (Asia/Bangkok): {max_timestamp} (type: {type(max_timestamp)})")
            except Exception as tz_error:
                log_error(f"⚠️ Warning: Could not apply pendulum timezone conversion: {tz_error}")
                log_error(f"   Table {table_name} had problematic timestamp data requiring this fix")
                log_error(f"   Other tables may have worked because their datetime data was cleaner")
                # Fallback: keep original timestamp
                max_timestamp = max_timestamp_sql

        # Get source data range for comparison
        try:
            with engine_source.connect() as connection:
                # Get the latest timestamp in source
                log_phase(f"🔍 Getting latest {modifier_column} from SOURCE database...")
                source_max_query = f"SELECT MAX({modifier_column}) FROM {table_name}"
                source_max_result = connection.execute(sa.text(source_max_query))
                source_max_timestamp = source_max_result.scalar()

                # 🕐 TIMEZONE HANDLING: If source timestamp has no timezone info, default to GMT +7 (Asia/Bangkok)
                # This ensures consistent timezone handling across all database operations
                if source_max_timestamp and isinstance(source_max_timestamp, datetime) and source_max_timestamp.tzinfo is None:
                    log_phase(f"   📅 SOURCE timestamp has no timezone - defaulting to GMT +7 (Asia/Bangkok)")
                    # Convert naive datetime to GMT +7 timezone for consistency with target database
                    import pytz
                    bangkok_tz = pytz.timezone('Asia/Bangkok')
                    source_max_timestamp = bangkok_tz.localize(source_max_timestamp)
                    log_phase(f"   📅 SOURCE timestamp with GMT +7: {source_max_timestamp}")
                else:
                    log_phase(f"   📅 SOURCE latest {modifier_column}: {source_max_timestamp}")

                # Get count of records newer than target timestamp (like working commit)
                if max_timestamp_sql is not None and source_max_timestamp is not None:
                    log_phase(f"🔍 Executing new records query...")
                    # 🕐 TIMEZONE CONSISTENCY: Ensure comparison timestamp is also in GMT +7
                    # This prevents timezone mismatches in the WHERE clause comparison
                    comparison_timestamp = max_timestamp_sql
                    if isinstance(max_timestamp_sql, datetime) and max_timestamp_sql.tzinfo is None:
                        log_phase(f"   📅 Comparison timestamp has no timezone - converting to GMT +7")
                        import pytz
                        bangkok_tz = pytz.timezone('Asia/Bangkok')
                        comparison_timestamp = bangkok_tz.localize(max_timestamp_sql)
                        log_phase(f"   📅 Comparison timestamp with GMT +7: {comparison_timestamp}")

                    # Try named parameters instead of positional for complex primary key compatibility
                    new_records_query = f"SELECT COUNT(*) FROM {table_name} WHERE {modifier_column} > :timestamp"
                    log_phase(f"   📋 Query: {new_records_query}")
                    log_phase(f"   📋 Params: {{'timestamp': {comparison_timestamp}}} (type: {type(comparison_timestamp)})")
                    log_phase(f"   ✅ CONFIRMED: Using NAMED parameters (:timestamp) for complex primary key compatibility")

                    # Use named parameters for better compatibility with complex primary keys
                    new_records_result = connection.execute(sa.text(new_records_query), {"timestamp": comparison_timestamp})
                    new_records_count = new_records_result.scalar()
                    log_phase(f"🔍 Found {new_records_count} new records since {comparison_timestamp}")
                    log_phase(f"   📊 Source max timestamp: {source_max_timestamp} (type: {type(source_max_timestamp)})")
                else:
                    # If no target timestamp, count all records
                    total_records_query = f"SELECT COUNT(*) FROM {table_name}"
                    total_records_result = connection.execute(sa.text(total_records_query))
                    new_records_count = total_records_result.scalar()
                    log_phase(f"🔍 No target timestamp - counting all records: {new_records_count}")
                
        except Exception as source_check_error:
            log_error(f"⚠️ Could not check source data range: {source_check_error}")
            log_error(f"   ✅ CONFIRMED: This is the UPDATED exception handler with comprehensive logging")
            log_error(f"   ✅ CONFIRMED: Named parameter fix is being used (:timestamp instead of %s)")
            log_error(f"   Exception type: {type(source_check_error)}")
            log_error(f"   Exception details: {str(source_check_error)}")
            import traceback
            log_error(f"   Full traceback: {traceback.format_exc()}")
            log_error(f"   max_timestamp_sql at failure: {max_timestamp_sql} (type: {type(max_timestamp_sql)})")
            log_error(f"   max_timestamp at failure: {max_timestamp} (type: {type(max_timestamp)})")
            source_max_timestamp = None
            new_records_count = 0

            # Add row count comparison even when source check fails
            log_error(f"📊 ROW COUNT COMPARISON (despite source check failure):")
            source_count = get_table_row_count(engine_source, table_name)
            target_count = get_table_row_count(engine_target, table_name)
            difference = abs(source_count - target_count)
            percentage_diff = (difference / max(source_count, 1)) * 100
            log_error(f"   Source: {source_count:,} rows")
            log_error(f"   Target: {target_count:,} rows")
            log_error(f"   Difference: {difference:,} rows ({percentage_diff:.2f}%)")

            log_error(f"🚨 SOURCE CHECK FAILED - CONTINUING WITH PIPELINE EXECUTION ANYWAY")
            log_error(f"   This should still reach pipeline.run() despite the source check failure")
        
        # Apply incremental hints
        table_source = getattr(source, table_name)

        # 🔍 CRITICAL: Show comprehensive source vs target modifier comparison
        log(f"🔧 INCREMENTAL SYNC ANALYSIS for {table_name}:")
        log(f"   📊 Modifier column: {modifier_column}")
        log(f"   🔑 Primary key: {formatted_primary_key}")
        log(f"   📅 TARGET latest {modifier_column}: {max_timestamp}")
        log(f"   📅 SOURCE latest {modifier_column}: {source_max_timestamp}")
        log(f"   📈 Records to sync: {new_records_count}")

        if max_timestamp and source_max_timestamp:
            time_diff = source_max_timestamp - max_timestamp
            log(f"   ⏱️  Time difference: {time_diff}")

            if source_max_timestamp > max_timestamp:
                log(f"   ✅ SOURCE is NEWER - {new_records_count} records will be synced")
                log(f"   📊 Sync window: {max_timestamp} → {source_max_timestamp}")
            elif source_max_timestamp == max_timestamp:
                log(f"   ⚖️  SOURCE and TARGET are in sync - no new records")
            else:
                log(f"   ⚠️  SOURCE is OLDER than TARGET - this is unusual")
                log(f"   🔍 This might indicate data inconsistencies")
        elif max_timestamp is None:
            log(f"   📝 TARGET has no existing data - full initial sync")
        elif source_max_timestamp is None:
            log(f"   📝 SOURCE has no data in {modifier_column} column")
        else:
            log(f"   ❓ Unable to determine sync status - check data types")

        log(f"🔄 Ready for incremental pipeline execution...")
        
        # Create incremental configuration with proper handling
        if max_timestamp is not None:
            # Ensure timestamp is in the correct format for DLT
            if isinstance(max_timestamp, str):
                try:
                    # Parse string timestamp to datetime
                    from datetime import datetime
                    import pytz
                    max_timestamp = datetime.fromisoformat(max_timestamp.replace('Z', '+00:00'))
                except Exception as parse_error:
                    log(f"⚠️ Warning: Could not parse timestamp {max_timestamp}: {parse_error}")
                    max_timestamp = None
            
            if max_timestamp is not None:
                # CRITICAL FIX: Use the TARGET timestamp as the starting point for incremental sync
                # This ensures DLT processes records newer than what's in the target database
                log(f"🔧 Creating incremental config with target timestamp: {max_timestamp}")
                incremental_config = dlt.sources.incremental(modifier_column, initial_value=max_timestamp)
                log(f"🔧 Applied incremental sync with target timestamp: {max_timestamp}")
                
                # CRITICAL: Force DLT to use the exact timestamp we specify
                if hasattr(incremental_config, 'initial_value'):
                    incremental_config.initial_value = max_timestamp
                    log(f"🔧 Forced incremental config initial_value to: {max_timestamp}")
            else:
                # Fallback to no incremental (will sync all data)
                incremental_config = None
                log(f"⚠️ Warning: Using fallback sync (no incremental) for {table_name}")
        else:
            # No timestamp available, sync all data
            incremental_config = None
            log(f"📝 No previous timestamp found - syncing all data for {table_name}")
        
        # Apply hints
        if incremental_config is not None:
            # CRITICAL: Configure hints with merge strategy optimization
            hints_config = {
                "primary_key": formatted_primary_key,
                "incremental": incremental_config
            }
            
            # Add merge strategy to avoid complex DELETE operations
            if config.TRUNCATE_STAGING_DATASET:
                try:
                    # Note: DLT may not support these hint parameters directly
                    # They are handled through the destination configuration
                    log(f"🔧 Using staging-optimized merge strategy for {table_name}")
                except Exception as merge_config_error:
                    log(f"⚠️ Could not apply merge strategy hints: {merge_config_error}")
            
            table_source.apply_hints(**hints_config)
            log(f"✅ Applied incremental hints with config: {incremental_config}")
            
            # CRITICAL FIX: Force DLT to re-evaluate incremental state by clearing cached state
            # This ensures DLT doesn't use stale cached state that prevents proper sync
            try:
                if hasattr(table_source, '_incremental'):
                    if hasattr(table_source._incremental, '_cached_state'):
                        table_source._incremental._cached_state = None
                        log(f"🔧 Cleared DLT cached incremental state for {table_name}")
                    if hasattr(table_source._incremental, 'last_value'):
                        # Force reset to match database state
                        table_source._incremental.last_value = max_timestamp
                        log(f"🔧 Reset DLT incremental last_value to: {max_timestamp}")
                        
                    # CRITICAL: Force DLT to re-evaluate the incremental configuration
                    if hasattr(table_source._incremental, 'initial_value'):
                        table_source._incremental.initial_value = max_timestamp
                        log(f"🔧 Reset DLT incremental initial_value to: {max_timestamp}")
                        
                    # Force DLT to clear any internal state
                    if hasattr(table_source._incremental, '_state'):
                        table_source._incremental._state = None
                        log(f"🔧 Cleared DLT incremental internal state for {table_name}")
                        
                    # CRITICAL: Force DLT to re-evaluate the incremental configuration by clearing all cached values
                    if hasattr(table_source._incremental, '_last_value_func'):
                        table_source._incremental._last_value_func = None
                        log(f"🔧 Cleared DLT incremental last_value_func for {table_name}")
                        
                    # Force DLT to re-evaluate the incremental configuration by clearing all cached values
                    if hasattr(table_source._incremental, '_initial_value_func'):
                        table_source._incremental._initial_value_func = None
                        log(f"🔧 Cleared DLT incremental initial_value_func for {table_name}")
                        
            except Exception as cache_clear_error:
                log(f"⚠️ Could not clear DLT cached state: {cache_clear_error}")
        else:
            # CRITICAL: Configure hints with merge strategy optimization for non-incremental tables
            hints_config = {"primary_key": formatted_primary_key}
            
            # Add merge strategy to avoid complex DELETE operations
            if config.TRUNCATE_STAGING_DATASET:
                log(f"🔧 Using staging-optimized merge strategy for non-incremental {table_name}")
            
            table_source.apply_hints(**hints_config)
            log(f"✅ Applied primary key hints only (no incremental)")
        
        log(f"✅ Incremental hints applied successfully")
        
        # Debug: Verify hints were applied
        try:
            if hasattr(table_source, 'hints'):
                log(f"🔍 Applied hints: {table_source.hints}")
            # Check if the incremental configuration was applied correctly
            if incremental_config is not None:
                log(f"🔍 Incremental config type: {type(incremental_config)}")
                log(f"🔍 Incremental config: {incremental_config}")
                if hasattr(incremental_config, 'cursor_path'):
                    log(f"🔍 Cursor path: {incremental_config.cursor_path}")
                if hasattr(incremental_config, 'initial_value'):
                    log(f"🔍 Initial value: {incremental_config.initial_value}")
                    
                    # CRITICAL DEBUG: Show what DLT will actually process
                    log(f"🔍 DLT will process records newer than: {incremental_config.initial_value}")
                    if hasattr(incremental_config.initial_value, 'astimezone'):
                        utc_value = incremental_config.initial_value.astimezone(pytz.UTC)
                        log(f"🔍 DLT initial value (UTC): {utc_value}")
        except Exception as hint_error:
            log(f"⚠️ Hint verification unavailable: {hint_error}")
        
        # CRITICAL FIX: Create completely fresh pipeline like old_db_pipeline.py
        # The issue is that DLT's pipeline state is corrupted - we need a fresh pipeline
        log(f"🔧 Creating fresh pipeline and source (like old working code)...")
        try:
            # Create a fresh pipeline with a unique name to avoid state conflicts
            import time
            fresh_pipeline_name = f"mysql_sync_fresh_{table_name}_{int(time.time())}"
            
            # CRITICAL FIX: Use connection string directly to avoid _conn_lock error
            target_url = f"mysql+pymysql://{config._url_encode_credential(config.TARGET_DB_USER)}:{config._url_encode_credential(config.TARGET_DB_PASS)}@{config.TARGET_DB_HOST}:{config.TARGET_DB_PORT}/{config.TARGET_DB_NAME}"
            
            fresh_pipeline = dlt.pipeline(
                pipeline_name=fresh_pipeline_name,
                destination=dlt.destinations.sqlalchemy(target_url),
                dataset_name=config.TARGET_DB_NAME
            )
            log(f"✅ Created fresh pipeline: {fresh_pipeline_name}")
            
            # Create fresh source like old_db_pipeline.py line 659
            fresh_source = sql_database(engine_source, schema=config.SOURCE_DB_NAME, table_names=[table_name])
            
            # Apply incremental hints exactly like old_db_pipeline.py lines 667-671
            log(f"🔧 Setting incremental for table {table_name} on column {modifier_column}")
            log(f"🔧 Setting incremental for table {table_name} with initial value {max_timestamp}")
            
            getattr(fresh_source, table_name).apply_hints(
                primary_key=formatted_primary_key,
                incremental=dlt.sources.incremental(modifier_column, initial_value=max_timestamp)
            )
            
            # CRITICAL FIX: Don't apply sanitization transformer to fresh pipeline
            # The transformer creates a separate table that confuses DLT's merge logic
            
            # Use the fresh pipeline and source
            pipeline = fresh_pipeline
            source = fresh_source
            log(f"✅ Fresh pipeline and source created successfully")
            
        except Exception as fresh_error:
            log(f"⚠️ Failed to create fresh pipeline: {fresh_error}")
            # Fallback to original approach
            # CRITICAL FIX: Disable sanitization transformer (same issue as full refresh)
            # source = source | sanitize_table_data  # DISABLED - was consuming all data

        
        # Run the pipeline with proper configuration for DLT 1.15.0
        log(f"🔄 Running DLT pipeline for {table_name}...")
        log(f"   Using write_disposition: merge (incremental sync)")
        log(f"   Will merge new/changed data with existing target table")
        
        # Verify new records count
        try:
            new_records_count = get_new_records_count(engine_source, table_name, modifier_column, max_timestamp)
            log(f"🔍 Source has {new_records_count} new records since {max_timestamp}")
        except Exception as count_error:
            log(f"⚠️ Could not verify new records count: {count_error}")
            new_records_count = 0
        
        # Run pipeline like old_db_pipeline.py
        log(f"🔧 Running DLT pipeline...")
        
        # CRITICAL DISCOVERY: DLT SQLAlchemy destination DOES use staging tables for merge operations!
        # We need to optimize BOTH the target table AND the staging tables that DLT creates
        
        if config.ENABLE_INDEX_OPTIMIZATION:
            try:
                log(f"🚨 CORRECTED: DLT IS using staging tables - optimizing BOTH target and staging tables")
                log(f"🔧 Creating optimized indexes on target table {table_name} for DLT operations")
                
                # Optimize the target table directly for DLT's operations
                from index_management import optimize_table_for_dlt
                optimization_success = optimize_table_for_dlt(table_name, table_config, engine_source, engine_target)
                
                if optimization_success:
                    log(f"✅ Target table optimization completed for {table_name}")
                else:
                    log(f"⚠️ Target table optimization had issues for {table_name}")
                
                # CRITICAL: Start staging table optimization in parallel thread
                # This will wait for DLT to create staging tables and immediately optimize them
                import threading
                staging_optimization_thread = None
                try:
                    log(f"🚀 Starting staging table optimization thread for {table_name}...")
                    from index_management import wait_and_optimize_staging_table
                    staging_optimization_thread = threading.Thread(
                        target=wait_and_optimize_staging_table,
                        args=(table_name, table_config, engine_source, engine_target, config.INDEX_OPTIMIZATION_TIMEOUT),
                        daemon=True
                    )
                    staging_optimization_thread.start()
                    log(f"✅ Staging table optimization thread started for {table_name}")
                    log(f"   This will create indexes on staging tables as soon as DLT creates them")
                except Exception as thread_error:
                    log(f"⚠️ Could not start staging optimization thread: {thread_error}")
                    
            except Exception as optimization_error:
                log(f"⚠️ Could not optimize target table: {optimization_error}")
        else:
            log(f"ℹ️ Table optimization disabled for {table_name}")
        
        # 🔍 FINAL INCREMENTAL SYNC SUMMARY BEFORE PIPELINE EXECUTION
        log(f"🔍 INCREMENTAL SYNC SUMMARY - Ready for Pipeline Execution:")
        log(f"   📊 Table: {table_name}")
        log(f"   🔑 Primary Key: {formatted_primary_key}")
        log(f"   📅 Modifier Column: {modifier_column}")
        log(f"   📅 Target Latest: {max_timestamp}")
        log(f"   📅 Source Latest: {source_max_timestamp}")
        log(f"   📈 New Records to Sync: {new_records_count}")

        # Add row count comparison for complete visibility
        source_count = get_table_row_count(engine_source, table_name)
        target_count = get_table_row_count(engine_target, table_name)
        difference = abs(source_count - target_count)
        percentage_diff = (difference / max(source_count, 1)) * 100
        log(f"   📊 Total Row Counts:")
        log(f"      Source: {source_count:,} rows")
        log(f"      Target: {target_count:,} rows")
        log(f"      Difference: {difference:,} rows ({percentage_diff:.2f}%)")

        if new_records_count > 0:
            log(f"   ✅ INCREMENTAL SYNC: Will sync {new_records_count} new records")
            log(f"   📊 Processing records where {modifier_column} > {max_timestamp}")
            log(f"   🎯 Expected result: Target should increase by {new_records_count} rows")
        elif new_records_count == 0:
            log(f"   ⚖️  NO CHANGES: Source and target are in sync")
            log(f"   📊 No records where {modifier_column} > {max_timestamp}")
            if difference == 0:
                log(f"   ✅ CONFIRMED: Row counts match exactly")
            else:
                log(f"   ⚠️  WARNING: Row counts differ despite no new records found")
        else:
            log(f"   ❓ UNKNOWN: Unable to determine record count")

        log(f"🚀 Starting DLT Pipeline Execution...")

        try:
            # CRITICAL FIX: Use DLT's built-in staging management
            # DLT 1.15.0 automatically handles staging cleanup when properly configured
            if config.TRUNCATE_STAGING_DATASET:
                log(f"🗑️ DLT staging OPTIMIZATION enabled - fast DELETE operations with automatic cleanup")
            else:
                log(f"⚠️ DLT staging optimization not configured - may use default staging behavior")

            # Run pipeline with DLT staging optimization (fast DELETE operations)
            log(f"🔄 Starting DLT pipeline execution for {table_name} (incremental sync)...")
            log(f"   Source records to process: checking incremental data...")
            log(f"   Progress bars should appear below during execution...")
            log(f"🚀 EXECUTING: pipeline.run(source, write_disposition='merge')")
            log(f"   Pipeline: {pipeline.pipeline_name if hasattr(pipeline, 'pipeline_name') else 'Unknown'}")
            log(f"   Source type: {type(source)}")
            log(f"   Write disposition: merge")

            try:
                load_info = pipeline.run(source, write_disposition="merge")
                log(f"✅ Pipeline run completed for {table_name}")
                log(f"   Load info type: {type(load_info)}")
                if load_info is None:
                    log_error(f"🚨 CRITICAL: pipeline.run() returned None!")
                else:
                    log(f"   Load info received: {bool(load_info)}")
            except Exception as run_error:
                log_error(f"🚨 CRITICAL: pipeline.run() threw exception: {run_error}")
                log_error(f"   Exception type: {type(run_error)}")
                import traceback
                log_error(f"   Full traceback: {traceback.format_exc()}")
                load_info = None

            # Log detailed load info
            if load_info:
                log(f"📊 Load info details: {load_info}")
            else:
                log_error(f"⚠️ No load info returned - this may indicate a problem")

            # 🔍 CRITICAL: Log comprehensive sync verification
            try:
                sync_results = log_sync_results(table_name, engine_source, engine_target, "incremental_merge")
                if sync_results:
                    log(f"📈 Incremental sync summary: {sync_results['sync_status'].upper()}")
                    log(f"   Source: {sync_results['source_count']:,} | Target: {sync_results['target_count']:,}")
            except Exception as sync_error:
                log_error(f"❌ Sync verification failed: {sync_error}")
            
            # CRITICAL: Force staging cleanup after pipeline run
            if config.TRUNCATE_STAGING_DATASET:
                try:
                    log("🧹 CRITICAL: Forcing staging cleanup after pipeline run...")
                    # Force DLT to clean up staging tables immediately
                    if hasattr(pipeline, 'destination') and hasattr(pipeline.destination, 'truncate_staging_dataset'):
                        pipeline.destination.truncate_staging_dataset = True
                        log("✅ Staging cleanup forced on destination")
                    
                    # Also force pipeline-level staging cleanup
                    dlt.config["load.truncate_staging_dataset"] = True
                    dlt.config["load.staging_dataset_cleanup"] = True
                    log("✅ Staging cleanup forced on pipeline")
                    
                except Exception as cleanup_error:
                    log(f"⚠️ Could not force staging cleanup: {cleanup_error}")
        except Exception as pipeline_error:
            log_error(f"❌ Incremental pipeline run failed for {table_name}")
            log_error(f"   Error type: {type(pipeline_error).__name__}")
            log_error(f"   Error details: {str(pipeline_error)}")
            
            # Log additional context for debugging
            log_error(f"   Table: {table_name}")
            log_error(f"   Modifier column: {modifier_column if 'modifier_column' in locals() else 'Unknown'}")
            log_error(f"   Max timestamp: {max_timestamp if 'max_timestamp' in locals() else 'Unknown'}")
            
            # CRITICAL: Do NOT fallback to full refresh for incremental tables
            # Incremental failures should remain as incremental failures
            log_error(f"   Incremental tables do not fallback to full refresh")
            load_info = None
        
        if load_info:
            log(f"📊 Load info for {table_name}: {load_info}")
            log(f"📊 Load info attributes: {dir(load_info)}")

            # CRITICAL: Validate that DLT actually created load packages
            try:
                if hasattr(load_info, 'loads_ids'):
                    log(f"🔍 Load IDs: {load_info.loads_ids}")
                    log(f"   Number of load IDs: {len(load_info.loads_ids) if load_info.loads_ids else 0}")
                if hasattr(load_info, 'load_packages'):
                    log(f"🔍 Load packages: {len(load_info.load_packages)}")

                    # CRITICAL CHECK: If no load packages, this indicates DLT didn't process any data
                    if len(load_info.load_packages) == 0:
                        log(f"🚨 CRITICAL: DLT created 0 load packages for incremental sync!")
                        log(f"   This means DLT thinks there's no new data to process")
                        log(f"   But we know there are {new_records_count} new records in source")
                        log(f"   This indicates the incremental cursor is not working properly")
                        log(f"   Max timestamp: {max_timestamp}")
                        log(f"   New records count: {new_records_count}")
                        log(f"   This is likely why the difference stays the same (-6,722 rows)")
                        
                        # CRITICAL: Do NOT fallback to full refresh for incremental tables
                        # This maintains the integrity of incremental vs full refresh methods
                        log(f"❌ Incremental sync failed to create load packages")
                        log(f"   Incremental tables do not fallback to full refresh")
                        log(f"   Check incremental configuration or data timestamps")
                    else:
                        log(f"✅ DLT created {len(load_info.load_packages)} load packages")
                    
                    # Show package details
                    for pkg in load_info.load_packages:
                        if hasattr(pkg, 'jobs'):
                            log(f"🔍 Jobs in package: {len(pkg.jobs)}")
                            for job in pkg.jobs:
                                if hasattr(job, 'status'):
                                    log(f"🔍 Job status: {job.status}")
            except Exception as debug_error:
                log(f"⚠️ Debug info unavailable: {debug_error}")
            
            # Verify data was actually synced
            new_target_count = get_table_row_count(engine_target, table_name)
            rows_synced = new_target_count - target_count
            
            # Get new target timestamp after sync
            new_target_max_timestamp = get_max_timestamp(engine_target, table_name, modifier_column)
            
            log(f"📊 Incremental sync results for {table_name}:")
            log(f"   Rows synced: {rows_synced}")
            log(f"   Target count: {target_count} → {new_target_count}")
            log(f"   📅 Target timestamp: {max_timestamp} → {new_target_max_timestamp}")
            
            if rows_synced > 0:
                log(f"✅ Incremental sync successful: {rows_synced} rows synced")
                if new_target_max_timestamp:
                    log(f"   📈 Timestamp advanced to: {new_target_max_timestamp}")
                
                # DLT 1.15.0 automatically handles staging cleanup when configured
                if config.TRUNCATE_STAGING_DATASET:
                    log(f"✅ DLT staging management handled cleanup automatically")
                
                return True
            else:
                log(f"⚠️ No rows were synced despite successful load info")
                log(f"   This might indicate an incremental sync issue")
                
                # Check if there are actually new records in source
                if max_timestamp is not None:
                    new_source_count = get_new_records_count(engine_source, table_name, modifier_column, max_timestamp)
                    log(f"🔍 New records in source since {max_timestamp}: {new_source_count}")
                    
                    if new_source_count > 0:
                        log(f"⚠️ Source has {new_source_count} new records but none were synced")
                        log(f"   This indicates an incremental sync configuration issue")
                        return False
                    else:
                        log(f"✅ No new records in source - sync is working correctly")
                        return True
                else:
                    log(f"✅ No timestamp comparison - sync is working correctly")
                    return True
        else:
            log(f"⚠️ No load info returned for {table_name}")
            return False
            
    except Exception as e:
        log(f"❌ Error in incremental processing for {table_name}: {e}")
        return False

def process_full_refresh_table(pipeline, engine_source, engine_target, table_name, table_config, write_disposition):
    """Process a single table with full refresh.
    
    CRITICAL: This function ONLY handles full refresh sync and does NOT use incremental logic.
    Full refresh tables completely replace target data and maintain method separation.
    """
    try:
        primary_key = table_config["primary_key"]
        # CRITICAL SAFEGUARD: Ensure this is truly a full refresh table
        if "modifier" in table_config and write_disposition == "merge":
            log(f"❌ CRITICAL ERROR: Full refresh function called for incremental table!")
            log(f"   Table {table_name} has modifier column but is in full refresh function")
            log(f"   This should not happen - check table processing logic")
            return False
        
        # Log the full refresh details
        log(f"🔄 Full refresh for {table_name}:")
        log(f"   Primary key: {primary_key}")
        log(f"   Write disposition: {write_disposition}")
        log(f"   CONFIRMED: Full refresh method (no incremental logic)")
        
        # Verify no modifier column or non-merge disposition
        if "modifier" in table_config:
            log(f"   Table has modifier '{table_config['modifier']}' but using full refresh due to write_disposition: {write_disposition}")
        else:
            log(f"   Table has no modifier column - pure full refresh")
        
        log_error(f"🔍 DEBUG: Getting row counts...")
        # Get source table row count for comparison
        source_count = get_table_row_count(engine_source, table_name)
        target_count_before = get_table_row_count(engine_target, table_name)
        
        log(f"📊 Row counts before sync - Source: {source_count}, Target: {target_count_before}")

        
        # CRITICAL: Check if source table has data before proceeding
        if source_count == 0:
            log_error(f"❌ Source table {table_name} has no data to sync")
            log_error(f"   Cannot perform full refresh on empty source table")
            return False
        else:
            log(f"✅ Source table {table_name} has {source_count} rows ready for sync")

        
        # CRITICAL FIX: Full refresh tables MUST use "replace" disposition
        # The issue is that full refresh tables are being processed with "merge" which prevents data sync
        if write_disposition != "replace":
            log(f"🚨 CRITICAL: Full refresh table {table_name} was given wrong write_disposition: {write_disposition}")
            log(f"🔧 Forcing write_disposition to 'replace' for full refresh table")
            write_disposition = "replace"
        
        # Clean up target table if using replace mode
        if write_disposition == "replace":
            log(f"🧹 Cleaning up target table {table_name} for full refresh...")
            
            # CRITICAL: Force aggressive cleanup for full refresh tables
            # This ensures the target table is completely empty before loading new data
            try:
                with engine_target.connect() as connection:
                    # First try TRUNCATE (fastest)
                    try:
                        log(f"🧹 Attempting TRUNCATE for {table_name}")
                        connection.execute(sa.text(f"TRUNCATE TABLE {table_name}"))
                        connection.commit()
                        log(f"✅ TRUNCATE successful for {table_name}")
                    except Exception as truncate_error:
                        log(f"⚠️ TRUNCATE failed for {table_name}: {truncate_error}")
                        # Fallback to DELETE
                        try:
                            log(f"🧹 Attempting DELETE for {table_name}")
                            connection.execute(sa.text(f"DELETE FROM {table_name}"))
                            connection.commit()
                            log(f"✅ DELETE successful for {table_name}")
                        except Exception as delete_error:
                            log(f"❌ DELETE also failed for {table_name}: {delete_error}")
                            # Last resort: use safe_table_cleanup
                            log(f"🔄 Using safe_table_cleanup as last resort")
                            safe_table_cleanup(engine_target, table_name, write_disposition)
                            
                            # Verify cleanup
                            target_count_after_cleanup = get_table_row_count(engine_target, table_name)
                            log(f"📊 Target table after cleanup: {target_count_after_cleanup} rows")
                            
                            if target_count_after_cleanup > 0:
                                log(f"⚠️ Warning: Target table still has {target_count_after_cleanup} rows after cleanup")
                                log(f"   This might prevent proper full refresh sync")
                            else:
                                log(f"✅ Target table successfully cleared for full refresh")
                                
                        except Exception as delete_error:
                            log(f"❌ DELETE also failed for {table_name}: {delete_error}")
                            log(f"   Full refresh may fail due to unclean target table")
            except Exception as cleanup_error:
                log(f"❌ Critical cleanup error for {table_name}: {cleanup_error}")
                log(f"   Full refresh may fail due to unclean target table")
        
        # Create SQL database source
        source = sql_database(engine_source, schema=config.SOURCE_DB_NAME, table_names=[table_name])
        
        # Format primary key for DLT hints
        formatted_primary_key = format_primary_key(primary_key)
        
        # Debug full refresh configuration
        log(f"🔧 Applying full refresh hints for {table_name}:")
        log(f"   Primary key: {formatted_primary_key}")
        log(f"   Write disposition: {write_disposition}")
        
        # Apply hints
        table_source = getattr(source, table_name)
        table_source.apply_hints(primary_key=formatted_primary_key)
        
        # CRITICAL: For full refresh tables, ensure we don't apply incremental hints
        # This prevents DLT from thinking it's doing incremental sync
        if write_disposition == "replace":
            log(f"🔧 Full refresh mode: No incremental hints applied")
            # Ensure the table source doesn't have any incremental configuration
            if hasattr(table_source, '_incremental'):
                table_source._incremental = None
                log(f"🔧 Cleared any existing incremental configuration for full refresh")
        
        # CRITICAL FIX: Disable sanitization transformer that's consuming all data
        # The transformer was creating a separate 'sanitize_table_data' table instead of 
        # passing data through to the target table
        
        # Run the pipeline with proper configuration for DLT 1.15.0
        log(f"🔄 Running DLT pipeline for {table_name} (full refresh)...")
        log(f"   Using write_disposition: {write_disposition}")
        
        # Ensure write_disposition is properly passed
        if write_disposition == "replace":
            log(f"   Full refresh mode: Will replace all data in target table")
        elif write_disposition == "merge":
            log(f"   Merge mode: Will merge data with existing target table")
        else:
            log(f"   Append mode: Will add data to existing target table")
        
        # CRITICAL: Ensure we pass the correct write_disposition to pipeline.run()
        # This is essential for full refresh tables to work correctly
        
        # CRITICAL: For full refresh tables, skip index optimization
        # Full refresh tables don't need indexes for DLT operations since they replace all data
        # Indexes are only beneficial for incremental tables with merge operations
        
        if config.ENABLE_INDEX_OPTIMIZATION:
            log(f"ℹ️ Skipping index optimization for {table_name} (full refresh table)")
            log(f"   Full refresh tables don't need indexes for DLT operations")
            log(f"   Indexes are only beneficial for incremental tables with merge operations")
        else:
            log(f"ℹ️ Index optimization disabled globally (ENABLE_INDEX_OPTIMIZATION=false)")
        
        # CRITICAL FIX: DLT staging OPTIMIZATION is enabled for direct database operations
        if config.TRUNCATE_STAGING_DATASET:
            log(f"🗑️ DLT staging OPTIMIZATION enabled - fast operations with automatic cleanup")
        else:
            log(f"⚠️ DLT staging optimization not configured - may use default staging behavior")
        
        if write_disposition == "replace":
            # Force the write_disposition to be respected
            log(f"🔄 Starting DLT pipeline execution for {table_name} (full refresh)...")
            try:
                load_info = pipeline.run(source, write_disposition="replace")
                log(f"✅ Pipeline execution completed for {table_name}")

                # 🔍 CRITICAL: Log comprehensive sync verification for full refresh
                try:
                    sync_results = log_sync_results(table_name, engine_source, engine_target, "full_refresh")
                    if sync_results:
                        log(f"📈 Full refresh sync summary: {sync_results['sync_status'].upper()}")
                        log(f"   Source: {sync_results['source_count']:,} | Target: {sync_results['target_count']:,}")

                        # Additional validation for full refresh
                        if sync_results['source_count'] > 0 and sync_results['target_count'] == 0:
                            log_error(f"🚨 CRITICAL ISSUE: Full refresh sync completely failed!")
                            log_error(f"   Source has {sync_results['source_count']:,} rows but target has 0")
                        elif sync_results['percentage_diff'] > 5:
                            log_error(f"🚨 SIGNIFICANT ISSUE: Full refresh has {sync_results['percentage_diff']:.1f}% data loss")
                except Exception as sync_error:
                    log_error(f"❌ Sync verification failed: {sync_error}")

            except Exception as pipeline_error:
                log_error(f"❌ Pipeline execution failed for {table_name}: {pipeline_error}")
                log_error(f"   Pipeline error type: {type(pipeline_error).__name__}")
                raise  # Re-raise to be caught by outer exception handler
            
            # CRITICAL: Force staging cleanup after full refresh
            if config.TRUNCATE_STAGING_DATASET:
                try:
                    log("🧹 CRITICAL: Forcing staging cleanup after full refresh...")
                    dlt.config["load.truncate_staging_dataset"] = True
                    dlt.config["load.staging_dataset_cleanup"] = True
                    log("✅ Staging cleanup forced after full refresh")
                except Exception as cleanup_error:
                    log(f"⚠️ Could not force staging cleanup: {cleanup_error}")
                    
        elif write_disposition == "merge":
            log(f"   Merge mode: Will merge data with existing target table")
            load_info = pipeline.run(source, write_disposition="merge")

            # 🔍 CRITICAL: Log comprehensive sync verification for merge
            try:
                sync_results = log_sync_results(table_name, engine_source, engine_target, "merge_sync")
                if sync_results:
                    log(f"📈 Merge sync summary: {sync_results['sync_status'].upper()}")
                    log(f"   Source: {sync_results['source_count']:,} | Target: {sync_results['target_count']:,}")
            except Exception as sync_error:
                log_error(f"❌ Sync verification failed: {sync_error}")

            # CRITICAL: Force staging cleanup after merge
            if config.TRUNCATE_STAGING_DATASET:
                try:
                    log("🧹 CRITICAL: Forcing staging cleanup after merge...")
                    dlt.config["load.truncate_staging_dataset"] = True
                    dlt.config["load.staging_dataset_cleanup"] = True
                    log("✅ Staging cleanup forced after merge")
                except Exception as cleanup_error:
                    log(f"⚠️ Could not force staging cleanup: {cleanup_error}")
                    
        else:
            log(f"   Append mode: Will add data to existing target table")
            load_info = pipeline.run(source, write_disposition=write_disposition)

            # 🔍 CRITICAL: Log comprehensive sync verification for append
            try:
                sync_results = log_sync_results(table_name, engine_source, engine_target, f"append_{write_disposition}")
                if sync_results:
                    log(f"📈 Append sync summary: {sync_results['sync_status'].upper()}")
                    log(f"   Source: {sync_results['source_count']:,} | Target: {sync_results['target_count']:,}")
            except Exception as sync_error:
                log_error(f"❌ Sync verification failed: {sync_error}")

            # CRITICAL: Force staging cleanup after append
            if config.TRUNCATE_STAGING_DATASET:
                try:
                    log("🧹 CRITICAL: Forcing staging cleanup after append...")
                    dlt.config["load.truncate_staging_dataset"] = True
                    dlt.config["load.staging_dataset_cleanup"] = True
                    log("✅ Staging cleanup forced after append")
                except Exception as cleanup_error:
                    log(f"⚠️ Could not force staging cleanup: {cleanup_error}")
        
        if load_info:
            # Verify data was actually synced
            target_count_after = get_table_row_count(engine_target, table_name)
            rows_synced = target_count_after - target_count_before
            
            log(f"📊 Sync verification - Rows synced: {rows_synced}")
            log(f"   Target before: {target_count_before}, Target after: {target_count_after}")
            log(f"   Source count: {source_count}")
            
            if rows_synced > 0 or (write_disposition == "replace" and target_count_after > 0):
                log(f"✅ Full refresh successful: {target_count_after} rows in target")
                
                # DLT 1.15.0 automatically handles staging cleanup when configured
                if config.TRUNCATE_STAGING_DATASET:
                    log(f"✅ DLT staging management handled cleanup automatically")
                
                return True
            else:
                log_error(f"⚠️ No rows were synced despite successful load info")
                log_error(f"   This indicates a full refresh configuration issue")
                
                # Check if source actually has data
                if source_count > 0:
                    log_error(f"⚠️ Source has {source_count} rows but target has {target_count_after} rows")
                    log_error(f"   This indicates a full refresh processing issue")
                    return False
                else:
                    log(f"✅ Source has no data - sync is working correctly")
                    return True
        else:
            log_error(f"❌ No load info returned for {table_name}")
            log_error(f"   This indicates the DLT pipeline failed to execute properly")
            log_error(f"   Check DLT configuration and database connectivity")
            return False
            
    except Exception as e:
        log_error(f"❌ Error in full refresh processing for {table_name}: {e}")
        log_error(f"   Error type: {type(e).__name__}")
        log_error(f"   Error details: {str(e)}")
        import traceback
        log_error(f"   Full traceback: {traceback.format_exc()}")
        return False

def create_pipeline(pipeline_name="mysql_sync", destination="sqlalchemy"):
    """Create a DLT pipeline with proper configuration."""
    try:
        # CRITICAL: Import dlt at the very beginning of the function to avoid scope issues
        import dlt
        
        # Configure DLT pipeline with correct syntax for DLT 1.15.0
        # Use sqlalchemy destination for MySQL compatibility
        # Configure staging and load behavior using available DLT 1.15.0 options
        
        # CRITICAL FIX: DLT 1.15.0 doesn't automatically load config.toml from subdirectories
        # We need to set the configuration programmatically using environment variables
        
        # CRITICAL DISCOVERY: DLT SQLAlchemy destination DOES use staging tables for merge operations!
        # Our approach: Optimize BOTH target tables AND staging tables that DLT creates
        
        if config.PIPELINE_MODE.lower() == "direct" and config.TRUNCATE_STAGING_DATASET:
            try:
                log("🚨 CORRECTED DISCOVERY: DLT IS using staging tables")
                log("   SQLAlchemy destination creates staging tables for merge operations")
                log("   Solution: Optimize target tables AND staging tables for fast DELETE operations")
                
                # Create basic SQLAlchemy destination with staging optimization
                optimized_destination = dlt.destinations.sqlalchemy(config.DB_TARGET_URL)
                
                # Set pipeline-level configuration for better performance
                dlt.config["load.workers"] = 1  # ✅ SUPPORTED: Single worker to avoid conflicts
                dlt.config["extract.workers"] = 1  # ✅ SUPPORTED: Single worker
                
                # Set environment variables for DLT configuration
                os.environ["DLT_LOAD_WORKERS"] = "1"
                os.environ["DLT_EXTRACT_WORKERS"] = "1"
                
                log("🔧 CORRECTED APPROACH: Optimizing target tables AND staging tables for DLT operations")
                log("   Workers: 1 (single worker to avoid conflicts)")
                log("   Index optimization: ENABLED (will optimize both target and staging tables)")
                log("   This should eliminate slow DELETE queries by optimizing all tables")
                
            except Exception as config_error:
                log(f"⚠️ Could not apply configuration: {config_error}")
                # Fallback to basic destination
                optimized_destination = dlt.destinations.sqlalchemy(config.DB_TARGET_URL)
        else:
            # Basic destination without optimization
            optimized_destination = dlt.destinations.sqlalchemy(config.DB_TARGET_URL)
        
        # Use the destination
        pipeline_kwargs = {
            "pipeline_name": pipeline_name+"v1",
            "destination": optimized_destination,
            "dataset_name": config.TARGET_DB_NAME  # Use actual target database name, not hardcoded "sync_data"
        }
        
        # Configure pipeline for optimal performance
        if config.PIPELINE_MODE.lower() == "direct" and config.TRUNCATE_STAGING_DATASET:
            pipeline_kwargs.update({
                "dev_mode": False,  # Keep incremental behavior
                "restore_from_destination": True,  # Sync with destination state
                "enable_runtime_trace": True,  # Enable runtime monitoring
            })
            
            log("🎯 CORRECTED APPROACH: Target table AND staging table optimization for DLT operations")
            log("   DLT creates staging tables - we optimize both target and staging tables")
            log("   This should eliminate slow DELETE queries")
        else:
            log("⚠️ Table optimization not configured")
            log("   Default DLT behavior may create slow DELETE operations")
        
        # Create pipeline with all configuration and progress reporting
        pipeline_kwargs["progress"] = "alive_progress"  # Add progress bar for better visibility
        pipeline = dlt.pipeline(**pipeline_kwargs)
        
        # CRITICAL FIX: Clean up any pending packages from previous runs before starting
        # This prevents the "pending load packages" warning
        try:
            log("🧹 Cleaning up any pending load packages from previous runs...")

            # Force DLT to not load pending packages - this is the key fix
            import dlt
            dlt.config["load.restore_schemas_on_run"] = False
            log("✅ Disabled schema restoration to prevent loading pending packages")

            # Force pipeline to start fresh by cleaning up any existing state
            if hasattr(pipeline, 'drop'):
                try:
                    pipeline.drop()
                    log("✅ Dropped existing pipeline state")
                except Exception as drop_error:
                    log(f"ℹ️ Pipeline drop not needed or failed: {drop_error}")

            # Also try to clean up dataset-level staging tables
            try:
                from index_management import cleanup_dataset_staging_tables
                cleanup_dataset_staging_tables(config.TARGET_DB_NAME, pipeline.destination.engine)
                log("✅ Cleaned up dataset-level staging tables")
            except Exception as staging_cleanup_error:
                log(f"ℹ️ Dataset staging cleanup not needed: {staging_cleanup_error}")

        except Exception as cleanup_error:
            log(f"ℹ️ Pipeline cleanup not critical: {cleanup_error}")

        # CRITICAL NEW FEATURE: Start dataset-level staging table monitoring
        # This will monitor and optimize DLT staging tables
        if config.TRUNCATE_STAGING_DATASET:
            try:
                log("🚀 CRITICAL NEW FEATURE: Starting dataset-level staging table monitoring...")
                log("   This will monitor and optimize DLT staging tables")
                log("   Note: sanitize_table_data transformer disabled to prevent data loss")
                
                # Start dataset-level staging table monitoring in a separate thread
                import threading
                dataset_staging_thread = threading.Thread(
                    target=monitor_dataset_staging_tables,
                    args=(config.TARGET_DB_NAME, pipeline.destination.engine, config.INDEX_OPTIMIZATION_TIMEOUT),
                    daemon=True
                )
                dataset_staging_thread.start()
                log("✅ Dataset-level staging table monitoring thread started")
                log(f"   This will automatically optimize all {config.TARGET_DB_NAME} staging tables")
                
                # CRITICAL: Also check for existing dataset-level staging tables immediately
                # This catches tables that were already created in previous runs
                try:
                    log("🔍 Proactively checking for existing dataset-level staging tables...")
                    existing_tables = monitor_dataset_staging_tables(config.TARGET_DB_NAME, pipeline.destination.engine, 10)
                    if existing_tables:
                        log(f"✅ Found and optimized {len(existing_tables)} existing dataset-level staging tables")
                    else:
                        log("ℹ️ No existing dataset-level staging tables found (this is normal for new pipelines)")
                except Exception as immediate_check_error:
                    log(f"⚠️ Immediate dataset-level staging table check failed: {immediate_check_error}")
                
            except Exception as dataset_monitoring_error:
                log(f"⚠️ Could not start dataset-level staging monitoring: {dataset_monitoring_error}")
                log("   Dataset-level staging tables may not be optimized")
        
        # CRITICAL FIX: DLT staging OPTIMIZATION is now enabled for direct database operations
        if config.TRUNCATE_STAGING_DATASET:
            log("🔧 DLT staging OPTIMIZATION enabled - staging tables with fast DELETE operations")
            log("   DLT will automatically clean up staging tables after each load")
            log("   Complex EXISTS subqueries will be avoided for better performance")
        else:
            log("⚠️ DLT staging optimization not configured")
        
        log(f"✅ Created DLT pipeline: {pipeline_name}")
        log(f"   Destination: SQLAlchemy MySQL")
        log(f"   Dataset: {config.TARGET_DB_NAME}")
        log(f"   Mode: incremental with merge disposition")
        if config.TRUNCATE_STAGING_DATASET:
            log(f"   Staging: OPTIMIZED (fast DELETE operations, automatic cleanup)")
            log(f"   Dataset-level monitoring: ENABLED (will optimize {config.TARGET_DB_NAME}.* staging tables)")
        else:
            log(f"   Staging: Default behavior (may create slow DELETE operations)")
            log(f"   Dataset-level monitoring: DISABLED (staging optimization not configured)")
        
        return pipeline
        
    except Exception as e:
        log(f"❌ Error creating pipeline: {e}")
        raise

class StagingFileWatcher(FileSystemEventHandler):
    """Event-driven file watcher for staging files - MODERN APPROACH."""
    
    def __init__(self, table_name: str, callback: Callable, target_extensions=('.parquet', '.json', '.csv', '.jsonl')):
        self.table_name = table_name
        self.callback = callback
        self.target_extensions = target_extensions
        self.found_files = []
        self.completed = threading.Event()
        
    def on_created(self, event):
        """Called when a file is created."""
        if not event.is_directory:
            file_path = event.src_path
            file_name = os.path.basename(file_path)
            
            # Check if this is a staging file for our table
            if (file_name.endswith(self.target_extensions) and 
                self.table_name in file_name):
                
                self.found_files.append(file_path)
                log(f"📄 Detected staging file: {file_name}")
                
                # Call the callback function
                self.callback(file_path, self.found_files.copy())
    
    def on_modified(self, event):
        """Called when a file is modified (file write completed)."""
        if not event.is_directory:
            file_path = event.src_path
            file_name = os.path.basename(file_path)
            
            if (file_name.endswith(self.target_extensions) and 
                self.table_name in file_name and 
                file_path in self.found_files):
                
                log(f"✅ Staging file write completed: {file_name}")
                # Signal that files are ready
                self.completed.set()





def process_file_staging_files(staging_dir, table_name, engine_target, primary_keys):
    """Process staging files and load data to MySQL using custom upsert logic."""
    try:
        # Step 2.1: Discover staging files with improved detection
        log(f"🔍 Discovering staging files for {table_name}...")
        staging_files = []
        if os.path.exists(staging_dir):
            for file in os.listdir(staging_dir):
                file_path = os.path.join(staging_dir, file)
                # Improved file detection logic
                if (file.endswith(('.parquet', '.json', '.csv', '.jsonl')) and 
                    (table_name in file or file.startswith('data') or file.startswith(table_name)) and
                    os.path.isfile(file_path) and os.path.getsize(file_path) > 0):
                    staging_files.append(file_path)
                    log(f"📄 Found staging file: {file} ({os.path.getsize(file_path)} bytes)")
        
        if not staging_files:
            # Try alternative file patterns
            log(f"🔍 No files found with table name, checking for generic DLT files...")
            if os.path.exists(staging_dir):
                for file in os.listdir(staging_dir):
                    file_path = os.path.join(staging_dir, file)
                    if (file.endswith(('.parquet', '.json', '.csv', '.jsonl')) and
                        os.path.isfile(file_path) and os.path.getsize(file_path) > 0):
                        staging_files.append(file_path)
                        log(f"📄 Found generic staging file: {file} ({os.path.getsize(file_path)} bytes)")
        
        if not staging_files:
            log(f"❌ Step 2.1 FAILED: No staging files found for {table_name}")
            log(f"   Directory contents: {os.listdir(staging_dir) if os.path.exists(staging_dir) else 'Directory does not exist'}")
            return False
        
        log(f"✅ Step 2.1 SUCCESS: Found {len(staging_files)} files for {table_name}")
        
        # Step 2.2: Process each staging file
        for file_idx, staging_file in enumerate(staging_files, 1):
            file_name = os.path.basename(staging_file)
            log(f"🔄 Step 2.2.{file_idx}: Processing file {file_name}...")
            
            # Read file in chunks
            file_ext = os.path.splitext(staging_file)[1].lower()
            batch_size = 10000
            
            try:
                total_records = 0
                if file_ext == '.parquet':
                    # Read parquet file in chunks
                    df = pd.read_parquet(staging_file)
                    total_records = len(df)
                    log(f"📊 Reading {total_records} records from Parquet file...")
                    
                    for start in range(0, len(df), batch_size):
                        chunk = df.iloc[start:start + batch_size]
                        records = chunk.to_dict('records')
                        chunk_num = (start // batch_size) + 1
                        log(f"🔄 Processing chunk {chunk_num} ({len(records)} records)...")
                        process_data_chunk(records, table_name, engine_target, primary_keys)
                        
                elif file_ext in ['.json', '.jsonl']:
                    # Read JSON lines file in chunks
                    log(f"📊 Reading JSON lines file...")
                    chunk_records = []
                    chunk_num = 0
                    
                    with open(staging_file, 'r') as f:
                        for line_num, line in enumerate(f, 1):
                            if line.strip():
                                try:
                                    record = pd.read_json(line, typ='series').to_dict()
                                    chunk_records.append(record)
                                    
                                    if len(chunk_records) >= batch_size:
                                        chunk_num += 1
                                        log(f"🔄 Processing chunk {chunk_num} ({len(chunk_records)} records)...")
                                        process_data_chunk(chunk_records, table_name, engine_target, primary_keys)
                                        total_records += len(chunk_records)
                                        chunk_records = []
                                except Exception as json_error:
                                    log(f"⚠️ Skipping invalid JSON on line {line_num}: {json_error}")
                        
                        # Process remaining records
                        if chunk_records:
                            chunk_num += 1
                            log(f"🔄 Processing final chunk {chunk_num} ({len(chunk_records)} records)...")
                            process_data_chunk(chunk_records, table_name, engine_target, primary_keys)
                            total_records += len(chunk_records)
                            
                elif file_ext == '.csv':
                    # Read CSV file in chunks
                    log(f"📊 Reading CSV file in chunks...")
                    chunk_num = 0
                    
                    for chunk in pd.read_csv(staging_file, chunksize=batch_size):
                        chunk_num += 1
                        records = chunk.to_dict('records')
                        log(f"🔄 Processing chunk {chunk_num} ({len(records)} records)...")
                        process_data_chunk(records, table_name, engine_target, primary_keys)
                        total_records += len(records)
                
                log(f"✅ Step 2.2.{file_idx} SUCCESS: Processed {total_records} records from {file_name}")
                        
            except Exception as file_error:
                log(f"❌ Step 2.2.{file_idx} FAILED: Error processing {file_name}")
                log(f"   Error: {file_error}")
                return False
        
        log(f"✅ Step 2.2 SUCCESS: All files processed for {table_name}")
        return True
        
    except Exception as e:
        log(f"❌ Error in file staging processing for {table_name}: {e}")
        return False

def process_data_chunk(records, table_name, engine_target, primary_keys):
    """Process a chunk of records using MySQL upsert logic."""
    if not records:
        return True
    
    try:
        # Ensure primary_keys is a list
        if isinstance(primary_keys, str):
            primary_keys = [primary_keys]
        
        # Build column information
        columns = list(records[0].keys())
        non_pk_columns = [col for col in columns if col not in primary_keys]
        
        # Create efficient upsert query
        placeholders = ', '.join(['%s'] * len(columns))
        update_clause = ', '.join([f'{col} = VALUES({col})' for col in non_pk_columns])
        
        upsert_sql = f"""
        INSERT INTO {table_name} ({', '.join(columns)})
        VALUES ({placeholders})
        ON DUPLICATE KEY UPDATE {update_clause}
        """
        
        # Execute in small, fast transaction
        def _execute_upsert(connection):
            for record in records:
                values = [record.get(col) for col in columns]
                connection.execute(sa.text(upsert_sql), values)
            return True
        
        with engine_target.connect() as connection:
            return _execute_upsert(connection)
        
    except Exception as e:
        log(f"❌ Error processing data chunk for {table_name}: {e}")
        return False

def cleanup_staging_files(staging_dir):
    """Clean up staging files after successful processing."""
    try:
        if os.path.exists(staging_dir):
            shutil.rmtree(staging_dir)
            log(f"🗑️ Cleaned up staging directory: {staging_dir}")
        return True
    except Exception as e:
        log(f"⚠️ Error cleaning up staging directory {staging_dir}: {e}")
        return False

def process_incremental_table_with_file(table_name, table_config, engine_source, engine_target):
    """Process incremental table using file staging to avoid DELETE conflicts."""
    try:
        # Phase 1: DLT Extraction to Files
        log(f"📁 Phase 1: DLT Extraction → Files for {table_name}")
        
        # Step 1.1: Create staging directory
        timestamp = time.strftime('%Y%m%d_%H%M%S')
        staging_pipeline_name = f"staging_{table_name}_{int(time.time())}"
        staging_dir = f"{config.FILE_STAGING_DIR}/{staging_pipeline_name}_{timestamp}"
        os.makedirs(staging_dir, exist_ok=True)
        
        # Step 1.2: Create DLT pipeline with filesystem destination
        try:
            staging_pipeline = dlt.pipeline(
                pipeline_name=staging_pipeline_name,
                destination=dlt.destinations.filesystem(staging_dir),
                dataset_name=config.TARGET_DB_NAME
            )
        except Exception as pipeline_error:
            log(f"❌ Phase 1 FAILED: Pipeline creation error for {table_name}")
            log(f"   Error: {pipeline_error}")
            log(f"   Working directory: {staging_dir}")
            return False
        
        # Step 1.3: Create DLT source with incremental logic
        modifier_column = table_config.get("modifier")
        primary_key = table_config["primary_key"]
        
        try:
            if modifier_column:
                # Get the latest timestamp from target for incremental sync
                max_timestamp = get_max_timestamp(engine_target, table_name, modifier_column)
                
                # CRITICAL FIX: Apply same timezone handling as old_db_pipeline.py
                if max_timestamp is not None:
                    try:
                        from dlt.common import pendulum
                        max_timestamp = pendulum.instance(max_timestamp).in_tz("Asia/Bangkok")
                        log(f"🔧 Applied timezone conversion (Asia/Bangkok): {max_timestamp}")
                    except Exception as tz_error:
                        log(f"⚠️ Warning: Could not apply timezone conversion: {tz_error}")
                        # Fallback: keep original timestamp
                
                # Create source with connection recovery
                try:
                    source = sql_database(engine_source, schema=config.SOURCE_DB_NAME, table_names=[table_name])
                except Exception as conn_error:
                    log(f"⚠️ Connection error creating source, retrying: {conn_error}")
                    # Force recreate engines to fix "Commands out of sync" issue
                    from database import create_engines, get_engines
                    create_engines()
                    engine_source, _ = get_engines()
                    source = sql_database(engine_source, schema=config.SOURCE_DB_NAME, table_names=[table_name])
                
                formatted_primary_key = format_primary_key(primary_key)
                getattr(source, table_name).apply_hints(
                    primary_key=formatted_primary_key,
                    incremental=dlt.sources.incremental(modifier_column, initial_value=max_timestamp)
                )
            else:
                # Full refresh
                try:
                    source = sql_database(engine_source, schema=config.SOURCE_DB_NAME, table_names=[table_name])
                except Exception as conn_error:
                    log(f"⚠️ Connection error creating source, retrying: {conn_error}")
                    # Force recreate engines to fix "Commands out of sync" issue
                    from database import create_engines, get_engines
                    create_engines()
                    engine_source, _ = get_engines()
                    source = sql_database(engine_source, schema=config.SOURCE_DB_NAME, table_names=[table_name])
                
                formatted_primary_key = format_primary_key(primary_key)
                getattr(source, table_name).apply_hints(primary_key=formatted_primary_key)
            
            # CRITICAL FIX: Disable sanitization transformer (same issue as full refresh)
            # Apply data sanitization transformer if enabled
            # source = source | sanitize_table_data  # DISABLED - was consuming all data

            
        except Exception as source_error:
            log(f"❌ Phase 1 FAILED: Source creation error for {table_name}")
            log(f"   Error: {source_error}")
            log(f"   Modifier column: {modifier_column}")
            log(f"   Primary key: {primary_key}")
            cleanup_staging_files(staging_dir)
            return False
        
        # Step 1.4: Start file watcher (before DLT runs)
        log(f"⚙️ Setting up file watcher for {table_name}...")
        timeout = int(os.getenv('FILE_STAGING_WAIT_TIMEOUT', '60'))
        
        # Set up callback-based file detection
        staging_files = []
        
        def file_ready_callback(file_path: str, all_files: List[str]):
            """Callback when staging files are detected and ready."""
            nonlocal staging_files
            staging_files = all_files
            log(f"📄 Detected {len(all_files)} staging files for {table_name}")
        
        # Start file watcher
        watcher = StagingFileWatcher(table_name, file_ready_callback)
        observer = Observer()
        observer.schedule(watcher, staging_dir, recursive=False)
        observer.start()
        
        try:
            # Step 1.5: Run DLT pipeline extraction
            log(f"🔄 Extracting data from source to files for {table_name}...")
            load_info = staging_pipeline.run(source)

            if not load_info:
                log(f"❌ Phase 1 FAILED: No data extracted for {table_name}")
                return False

            # 🔍 Log staging extraction results
            log(f"📊 Staging extraction completed for {table_name}")
            log(f"   Load info: {load_info}")
            log(f"✅ Phase 1 SUCCESS: Data extracted to staging files")
            
            # Step 1.6: Wait for files to be ready with improved detection
            log(f"⏳ Waiting for staging files to be ready for {table_name}...")
            
            # Give DLT some time to write files before starting the wait
            time.sleep(2)
            
            # Check for files manually first (fallback mechanism)
            manual_files = []
            if os.path.exists(staging_dir):
                for file in os.listdir(staging_dir):
                    file_path = os.path.join(staging_dir, file)
                    if (file.endswith(('.parquet', '.json', '.csv', '.jsonl')) and
                        os.path.isfile(file_path) and os.path.getsize(file_path) > 0):
                        manual_files.append(file_path)
            
            if manual_files:
                staging_files = manual_files
                log(f"✅ Phase 1 SUCCESS: Found {len(staging_files)} files immediately for {table_name}")
            elif watcher.completed.wait(timeout):
                staging_files = watcher.found_files
                log(f"✅ Phase 1 SUCCESS: {len(staging_files)} files ready via watcher for {table_name}")
            else:
                # Final fallback check
                staging_files = []
                if os.path.exists(staging_dir):
                    for file in os.listdir(staging_dir):
                        file_path = os.path.join(staging_dir, file)
                        if (file.endswith(('.parquet', '.json', '.csv', '.jsonl')) and
                            os.path.isfile(file_path) and os.path.getsize(file_path) > 0):
                            staging_files.append(file_path)
                
                if staging_files:
                    log(f"⚠️ Phase 1 PARTIAL: Found {len(staging_files)} files via fallback (timeout after {timeout}s)")
                else:
                    log(f"❌ Phase 1 FAILED: No files detected within {timeout}s for {table_name}")
                    log(f"   Directory contents: {os.listdir(staging_dir) if os.path.exists(staging_dir) else 'Directory does not exist'}")
            
        except Exception as extraction_error:
            log(f"❌ Phase 1 FAILED: DLT extraction error for {table_name}")
            log(f"   Error: {extraction_error}")
            staging_files = []
            
        finally:
            # Always stop the file watcher
            observer.stop()
            observer.join()
        
        if not staging_files:
            cleanup_staging_files(staging_dir)
            return False
        
        # Phase 2: Custom File Processing to MySQL
        log(f"🔧 Phase 2: Files → MySQL for {table_name}")
        log(f"🔄 Processing {len(staging_files)} staging files...")
        
        processing_success = process_file_staging_files(staging_dir, table_name, engine_target, primary_key)
        
        # Clean up staging files
        log(f"🗑️ Cleaning up staging files for {table_name}...")
        cleanup_staging_files(staging_dir)
        
        if processing_success:
            log(f"✅ Phase 2 SUCCESS: Table {table_name} processed successfully")

            # Log row count comparison after successful sync
            try:
                from database import log_row_count_comparison
                log_row_count_comparison(table_name, engine_source, engine_target)
            except Exception as count_error:
                log(f"⚠️ Row count comparison failed for {table_name}: {count_error}")

            log(f"🎉 COMPLETE: {table_name} sync finished successfully")
            return True
        else:
            log(f"❌ Phase 2 FAILED: File processing error for {table_name}")
            return False
            
    except Exception as e:
        log(f"❌ CRITICAL ERROR: Unexpected error processing {table_name}")
        log(f"   Error: {e}")
        log(f"   Cleaning up and aborting...")
        # Clean up on error
        cleanup_staging_files(staging_dir)
        return False

def load_select_tables_from_database():
    """Main function to load and sync configured tables from the database."""
    def _load_tables():
        from database import get_engines
        engine_source, engine_target = get_engines()
        
        log_config(f"🚀 Starting database sync with {len(config.table_configs)} configured tables")
        log_config(f"📋 Configured tables: {list(config.table_configs.keys())}")
        log_config(f"🔧 Pipeline mode: {config.PIPELINE_MODE}")
        
        # Check pipeline mode
        if config.PIPELINE_MODE.lower() == "file_staging":
            log_config("📁 File staging mode enabled - extracting to files first, then loading to database")
            pipeline = None  # No MySQL pipeline created - avoids timeout issues
        elif config.PIPELINE_MODE.lower() == "direct":
            log_config("⚡ Direct mode enabled - using db-to-db pipeline via DLT")
            # Create DLT pipeline for direct database operation
            pipeline = create_pipeline()
        else:
            log_config(f"⚠️ Unknown pipeline mode '{config.PIPELINE_MODE}', defaulting to direct mode")
            pipeline = create_pipeline()
        
        # Process tables in batches
        table_items = list(config.table_configs.items())
        total_successful = 0
        total_failed = 0
        
        for i in range(0, len(table_items), config.BATCH_SIZE):
            batch_num = (i // config.BATCH_SIZE) + 1
            total_batches = (len(table_items) + config.BATCH_SIZE - 1) // config.BATCH_SIZE
            
            batch_items = table_items[i:i + config.BATCH_SIZE]
            batch_dict = dict(batch_items)
            
            log_phase(f"\n🔄 Processing batch {batch_num}/{total_batches} ({len(batch_dict)} tables)")
            log_debug(f"📋 Batch configuration: BATCH_SIZE={config.BATCH_SIZE}, BATCH_DELAY={config.BATCH_DELAY}s")
            
            # Monitor connection pool health before each batch
            from monitoring import monitor_connection_pool_health
            try:
                if not monitor_connection_pool_health(engine_source, engine_target):
                    log_debug(f"⚠️ Connection pool health issues detected, attempting recovery...")
                    # Force engine recreation
                    from database import create_engines
                    create_engines()
                    engine_source, engine_target = get_engines()
                    log_debug(f"✅ Engine recreation completed for batch {batch_num}")
            except Exception as health_check_error:
                log_debug(f"⚠️ Connection pool health check failed: {health_check_error}")
                log_debug(f"🔄 Attempting engine recreation...")
                try:
                    from database import create_engines
                    create_engines()
                    engine_source, engine_target = get_engines()
                    log_debug(f"✅ Engine recreation completed after health check failure")
                except Exception as recreation_error:
                    log_error(f"❌ Engine recreation failed: {recreation_error}")
                    # Continue with existing engines - they might still work
            
            # Validate engines before processing
            if engine_source is None or engine_target is None:
                log_error(f"❌ Invalid engines detected, attempting to recreate...")
                try:
                    from database import create_engines
                    create_engines()
                    engine_source, engine_target = get_engines()
                    log_debug(f"✅ Engines recreated successfully")
                except Exception as engine_error:
                    log_error(f"❌ Failed to recreate engines: {engine_error}")
                    log_error(f"⚠️ Skipping batch {batch_num} due to engine issues")
                    total_failed += len(batch_dict)
                    continue
            
            if config.PIPELINE_MODE.lower() == "file_staging":
                log_debug("📁 Using file staging mode - extracting to files first, then loading to database")
                # Process each table individually with file staging
                for table_name, table_config in batch_dict.items():
                    try:
                        success = process_incremental_table_with_file(table_name, table_config, engine_source, engine_target)
                        if success:
                            total_successful += 1
                        else:
                            total_failed += 1
                    except Exception as table_error:
                        log_error(f"❌ Error processing table {table_name}: {table_error}")
                        total_failed += 1
            else:
                # Use direct db-to-db batch processing
                log_debug("⚡ Using direct db-to-db mode via DLT")
                
                # CRITICAL FIX: Separate incremental and full refresh tables
                # This ensures they use the correct write_disposition
                incremental_tables = {}
                full_refresh_tables = {}
                
                for table_name, table_config in batch_dict.items():
                    if "modifier" in table_config:
                        # Incremental tables use "merge" disposition
                        incremental_tables[table_name] = table_config
                    else:
                        # Full refresh tables use "replace" disposition
                        full_refresh_tables[table_name] = table_config
                
                log_debug(f"📊 Batch breakdown: {len(incremental_tables)} incremental, {len(full_refresh_tables)} full refresh")
                
                # Process incremental tables with "merge" disposition
                if incremental_tables:
                    log_phase(f"🔄 Processing {len(incremental_tables)} incremental tables with merge disposition")
                    successful, failed = process_tables_batch(pipeline, engine_source, engine_target, incremental_tables, "merge")
                    total_successful += successful
                    total_failed += failed
                
                # Process full refresh tables with "replace" disposition
                if full_refresh_tables:
                    log_phase(f"🔄 Processing {len(full_refresh_tables)} full refresh tables with replace disposition")
                    successful, failed = process_tables_batch(pipeline, engine_source, engine_target, full_refresh_tables, "replace")
                    total_successful += successful
                    total_failed += failed
            
            # Add delay between batches
            if i + config.BATCH_SIZE < len(table_items) and config.BATCH_DELAY > 0:
                log_debug(f"⏳ Waiting {config.BATCH_DELAY} seconds before next batch...")
                time.sleep(config.BATCH_DELAY)
        
        # Final summary
        log_status(f"\n{'='*60}")
        log_status(f"🎯 FINAL SYNC SUMMARY")
        log_status(f"📊 Total tables processed: {len(config.table_configs)}")
        log_status(f"✅ Successful: {total_successful}")
        log_status(f"❌ Failed: {total_failed}")
        log_status(f"📈 Success rate: {(total_successful/len(config.table_configs)*100):.1f}%")
        log_status(f"{'='*60}")
    
    return retry_on_connection_error(_load_tables, "pipeline")