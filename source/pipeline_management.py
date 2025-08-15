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
        source = sql_database(engine_source, schema=config.SOURCE_DB_NAME, table_names=[table_name])
        
        # Format primary key for DLT hints
        formatted_primary_key = format_primary_key(primary_key)
        
        # Apply hints for incremental loading with timezone-aware handling
        from datetime import datetime
        import pytz
        
        # Convert max_timestamp to timezone-aware datetime if needed
        if max_timestamp is not None and isinstance(max_timestamp, datetime):
            if max_timestamp.tzinfo is None:
                # Add UTC timezone to naive datetime to prevent DLT warnings
                max_timestamp = pytz.UTC.localize(max_timestamp)
        
        getattr(source, table_name).apply_hints(
            primary_key=formatted_primary_key,
            incremental=dlt.sources.incremental(modifier_column, initial_value=max_timestamp)
        )
        
        # Apply data sanitization transformer if enabled
        source = source | sanitize_table_data
        
        # Run the pipeline with proper configuration for DLT 1.15.0
        # Handle truncate_staging_dataset through write_disposition and table replacement
        if config.PIPELINE_MODE.lower() == "direct" and config.TRUNCATE_STAGING_DATASET:
            # For truncate_staging_dataset, we need to use replace disposition
            # This will clear the table before loading new data
            load_info = pipeline.run(source, write_disposition="replace")
            log("🗑️ Using replace disposition to clear staging dataset")
        else:
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
        source = sql_database(engine_source, schema=config.SOURCE_DB_NAME, table_names=[table_name])
        
        # Format primary key for DLT hints
        formatted_primary_key = format_primary_key(primary_key)
        
        # Apply hints
        getattr(source, table_name).apply_hints(primary_key=formatted_primary_key)
        
        # Apply data sanitization transformer if enabled
        source = source | sanitize_table_data
        
        # Run the pipeline with proper configuration for DLT 1.15.0
        # Handle truncate_staging_dataset through write_disposition
        if config.PIPELINE_MODE.lower() == "direct" and config.TRUNCATE_STAGING_DATASET:
            # For truncate_staging_dataset, we need to use replace disposition
            # This will clear the table before loading new data
            load_info = pipeline.run(source, write_disposition="replace")
            log("🗑️ Using replace disposition to clear staging dataset")
        else:
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

def create_pipeline(pipeline_name="mysql_sync", destination="sqlalchemy"):
    """Create a DLT pipeline with proper configuration."""
    try:
        # Configure DLT pipeline with correct syntax for DLT 1.15.0
        # Use sqlalchemy destination for MySQL compatibility
        pipeline_kwargs = {
            "pipeline_name": pipeline_name,
            "destination": dlt.destinations.sqlalchemy(config.DB_TARGET_URL),
            "dataset_name": "sync_data"
        }
        
        # Create pipeline with basic parameters first
        pipeline = dlt.pipeline(**pipeline_kwargs)
        
        # Apply configuration after pipeline creation if needed
        # Note: DLT 1.15.0 handles normalization and load settings differently
        if config.PIPELINE_MODE.lower() == "direct" and config.TRUNCATE_STAGING_DATASET:
            log("🗑️ Staging dataset truncation enabled for direct mode")
            # The truncate_staging_dataset will be handled in the pipeline.run() calls
        
        log(f"✅ Created DLT pipeline: {pipeline_name}")
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
        
        return execute_with_transaction_management(
            engine_target,
            f"upsert_chunk for {table_name}",
            _execute_upsert
        )
        
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
                
                # Convert max_timestamp to timezone-aware datetime if needed
                from datetime import datetime
                import pytz
                
                if max_timestamp is not None and isinstance(max_timestamp, datetime):
                    if max_timestamp.tzinfo is None:
                        # Add UTC timezone to naive datetime to prevent DLT warnings
                        max_timestamp = pytz.UTC.localize(max_timestamp)
                
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
            
            # Apply data sanitization transformer if enabled
            source = source | sanitize_table_data
            
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
        
        log(f"🚀 Starting database sync with {len(config.table_configs)} configured tables")
        log(f"📋 Configured tables: {list(config.table_configs.keys())}")
        log(f"🔧 Pipeline mode: {config.PIPELINE_MODE}")
        
        # Check pipeline mode
        if config.PIPELINE_MODE.lower() == "file_staging":
            log("📁 File staging mode enabled - extracting to files first, then loading to database")
            pipeline = None  # No MySQL pipeline created - avoids timeout issues
        elif config.PIPELINE_MODE.lower() == "direct":
            log("⚡ Direct mode enabled - using db-to-db pipeline via DLT")
            # Create DLT pipeline for direct database operation
            pipeline = create_pipeline()
        else:
            log(f"⚠️ Unknown pipeline mode '{config.PIPELINE_MODE}', defaulting to direct mode")
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
            
            log(f"\n🔄 Processing batch {batch_num}/{total_batches} ({len(batch_dict)} tables)")
            
            if config.PIPELINE_MODE.lower() == "file_staging":
                log("📁 Using file staging mode - extracting to files first, then loading to database")
                # Process each table individually with file staging
                for table_name, table_config in batch_dict.items():
                    try:
                        success = process_incremental_table_with_file(table_name, table_config, engine_source, engine_target)
                        if success:
                            total_successful += 1
                        else:
                            total_failed += 1
                    except Exception as table_error:
                        log(f"❌ Error processing table {table_name}: {table_error}")
                        total_failed += 1
            else:
                # Use direct db-to-db batch processing
                log("⚡ Using direct db-to-db mode via DLT")
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