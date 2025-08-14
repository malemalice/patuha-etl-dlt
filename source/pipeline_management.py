"""
Pipeline management module for DLT Database Sync Pipeline.
Contains DLT pipeline operations, table processing, and sync logic.
"""

import time
import dlt
from dlt.sources.sql_database import sql_database
import sqlalchemy as sa
from typing import Dict, Any, List, Union
import config
from utils import log, format_primary_key, validate_primary_key_config, log_primary_key_info
from database import execute_with_transaction_management, ensure_dlt_columns
from schema_management import sync_table_schema
from data_processing import validate_table_data, sanitize_table_data, debug_problematic_rows
from error_handling import retry_on_connection_error, retry_on_lock_timeout
from monitoring import log_performance_metrics

def get_max_timestamp(engine_target, table_name, column_name):
    """Get the maximum timestamp value from the target table with lock timeout handling."""
    def _get_timestamp(connection):
        try:
            query = f"SELECT MAX({column_name}) FROM {table_name}"
            result = connection.execute(sa.text(query))
            max_value = result.scalar()
            
            if max_value is None:
                log(f"📅 No existing data in target table {table_name}, starting from beginning")
                return None
            else:
                log(f"📅 Latest {column_name} in target table {table_name}: {max_value}")
                return max_value
                
        except Exception as e:
            log(f"❌ Error getting max timestamp for {table_name}.{column_name}: {e}")
            return None
    
    return retry_on_lock_timeout(
        execute_with_transaction_management,
        "target",
        f"get_max_timestamp for {table_name}.{column_name}",
        engine_target,
        f"get_max_timestamp for {table_name}.{column_name}",
        _get_timestamp
    )

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
    
    return retry_on_lock_timeout(
        execute_with_transaction_management,
        "target",
        f"force_table_clear for {table_name}",
        engine_target,
        f"force_table_clear for {table_name}",
        _clear_table
    )

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

def process_tables_batch(pipeline, engine_source, engine_target, tables_dict, write_disposition="merge"):
    """Process a batch of tables with proper connection management and lock timeout handling."""
    
    batch_start_time = time.time()
    successful_tables = []
    failed_tables = []
    
    log(f"🔄 Processing batch of {len(tables_dict)} tables with {write_disposition} disposition")
    
    for table_name, table_config in tables_dict.items():
        table_start_time = time.time()
        
        try:
            log(f"\n{'='*60}")
            log(f"🔄 Processing table: {table_name}")
            log(f"📋 Table config: {table_config}")
            
            # Validate primary key configuration
            if "primary_key" not in table_config:
                log(f"❌ Table '{table_name}' missing primary_key configuration, skipping")
                failed_tables.append(table_name)
                continue
                
            primary_key = table_config["primary_key"]
            if not validate_primary_key_config(primary_key):
                log(f"❌ Table '{table_name}' has invalid primary_key configuration: {primary_key}, skipping")
                failed_tables.append(table_name)
                continue
            
            # Log primary key information
            log_primary_key_info(table_name, primary_key)
            
            # Validate table data
            if not validate_table_data(engine_source, table_name):
                log(f"❌ Table data validation failed for {table_name}, attempting to debug...")
                debug_problematic_rows(engine_source, table_name, limit=5)
                failed_tables.append(table_name)
                continue
            
            # Sync schema first
            try:
                sync_table_schema(engine_source, engine_target, table_name)
                ensure_dlt_columns(engine_target, table_name)
            except Exception as schema_error:
                log(f"❌ Schema sync failed for {table_name}: {schema_error}")
                failed_tables.append(table_name)
                continue
            
            # Handle different sync strategies
            if "modifier" in table_config and write_disposition == "merge":
                # Incremental sync
                log(f"📈 Processing incremental sync for {table_name}")
                success = process_incremental_table(pipeline, engine_source, engine_target, table_name, table_config)
            else:
                # Full refresh
                log(f"🔄 Processing full refresh for {table_name}")
                success = process_full_refresh_table(pipeline, engine_source, engine_target, table_name, table_config, write_disposition)
            
            table_end_time = time.time()
            
            if success:
                successful_tables.append(table_name)
                log_performance_metrics(f"Table {table_name}", table_start_time, table_end_time)
                log(f"✅ Successfully processed table: {table_name}")
            else:
                failed_tables.append(table_name)
                log(f"❌ Failed to process table: {table_name}")
                
        except Exception as table_error:
            table_end_time = time.time()
            failed_tables.append(table_name)
            log(f"❌ Error processing table {table_name}: {table_error}")
            log_performance_metrics(f"Table {table_name} (FAILED)", table_start_time, table_end_time)
    
    batch_end_time = time.time()
    log_performance_metrics(f"Batch processing", batch_start_time, batch_end_time, len(tables_dict))
    
    # Summary
    log(f"\n{'='*60}")
    log(f"📊 Batch processing summary:")
    log(f"✅ Successful: {len(successful_tables)} tables")
    log(f"❌ Failed: {len(failed_tables)} tables")
    
    if successful_tables:
        log(f"✅ Successful tables: {', '.join(successful_tables)}")
    if failed_tables:
        log(f"❌ Failed tables: {', '.join(failed_tables)}")
    
    return len(successful_tables), len(failed_tables)

def process_incremental_table(pipeline, engine_source, engine_target, table_name, table_config):
    """Process a single table with incremental sync."""
    try:
        modifier_column = table_config["modifier"]
        primary_key = table_config["primary_key"]
        
        # Get the latest timestamp from target
        max_timestamp = get_max_timestamp(engine_target, table_name, modifier_column)
        
        # Create SQL database source
        source = sql_database(engine_source).with_resources(table_name)
        
        # Format primary key for DLT hints
        formatted_primary_key = format_primary_key(primary_key)
        
        # Apply hints for incremental loading
        getattr(source, table_name).apply_hints(
            primary_key=formatted_primary_key,
            incremental=dlt.sources.incremental(modifier_column, initial_value=max_timestamp)
        )
        
        # Apply data sanitization transformer if enabled
        source = source | sanitize_table_data
        
        # Run the pipeline
        load_info = pipeline.run(source, write_disposition="merge")
        
        if load_info:
            log(f"📊 Load info for {table_name}: {load_info}")
            return True
        else:
            log(f"⚠️ No load info returned for {table_name}")
            return False
            
    except Exception as e:
        log(f"❌ Error in incremental processing for {table_name}: {e}")
        return False

def process_full_refresh_table(pipeline, engine_source, engine_target, table_name, table_config, write_disposition):
    """Process a single table with full refresh."""
    try:
        primary_key = table_config["primary_key"]
        
        # Clean up target table if using replace mode
        if write_disposition == "replace":
            safe_table_cleanup(engine_target, table_name, write_disposition)
        
        # Create SQL database source
        source = sql_database(engine_source).with_resources(table_name)
        
        # Format primary key for DLT hints
        formatted_primary_key = format_primary_key(primary_key)
        
        # Apply hints
        getattr(source, table_name).apply_hints(primary_key=formatted_primary_key)
        
        # Apply data sanitization transformer if enabled
        source = source | sanitize_table_data
        
        # Run the pipeline
        load_info = pipeline.run(source, write_disposition=write_disposition)
        
        if load_info:
            log(f"📊 Load info for {table_name}: {load_info}")
            return True
        else:
            log(f"⚠️ No load info returned for {table_name}")
            return False
            
    except Exception as e:
        log(f"❌ Error in full refresh processing for {table_name}: {e}")
        return False

def create_pipeline(pipeline_name="mysql_sync", destination="mysql"):
    """Create a DLT pipeline with proper configuration."""
    try:
        # Configure DLT pipeline
        pipeline_config = {
            "pipeline_name": pipeline_name,
            "destination": destination,
            "dataset_name": "sync_data"
        }
        
        if config.PRESERVE_COLUMN_NAMES:
            pipeline_config["config"] = {"normalize": {"naming": "direct"}}
        
        pipeline = dlt.pipeline(**pipeline_config)
        log(f"✅ Created DLT pipeline: {pipeline_name}")
        return pipeline
        
    except Exception as e:
        log(f"❌ Error creating pipeline: {e}")
        raise

def load_select_tables_from_database():
    """Main function to load and sync configured tables from the database."""
    def _load_tables():
        from database import get_engines
        engine_source, engine_target = get_engines()
        
        # Create DLT pipeline
        pipeline = create_pipeline()
        
        log(f"🚀 Starting database sync with {len(config.table_configs)} configured tables")
        log(f"📋 Configured tables: {list(config.table_configs.keys())}")
        
        # Process tables in batches
        table_items = list(config.table_configs.items())
        total_successful = 0
        total_failed = 0
        
        for i in range(0, len(table_items), config.BATCH_SIZE):
            batch_num = (i // config.BATCH_SIZE) + 1
            total_batches = (len(table_items) + config.BATCH_SIZE - 1) // config.BATCH_SIZE
            
            batch_items = table_items[i:i + config.BATCH_SIZE]
            batch_dict = dict(batch_items)
            
            log(f"\n🔄 Processing batch {batch_num}/{total_batches} ({len(batch_dict)} tables)")
            
            successful, failed = process_tables_batch(pipeline, engine_source, engine_target, batch_dict)
            total_successful += successful
            total_failed += failed
            
            # Add delay between batches
            if i + config.BATCH_SIZE < len(table_items) and config.BATCH_DELAY > 0:
                log(f"⏳ Waiting {config.BATCH_DELAY} seconds before next batch...")
                time.sleep(config.BATCH_DELAY)
        
        # Final summary
        log(f"\n{'='*60}")
        log(f"🎯 FINAL SYNC SUMMARY")
        log(f"📊 Total tables processed: {len(config.table_configs)}")
        log(f"✅ Successful: {total_successful}")
        log(f"❌ Failed: {total_failed}")
        log(f"📈 Success rate: {(total_successful/len(config.table_configs)*100):.1f}%")
        log(f"{'='*60}")
    
    return retry_on_connection_error(_load_tables, "pipeline")