#!/usr/bin/env python3
"""
DLT Database Sync Pipeline - Main Entry Point

This is the main entry point for the DLT Database Sync Pipeline.
It orchestrates the entire sync process using modular components.

Key Features:
- Modular architecture with separated concerns
- Advanced error handling and retry mechanisms  
- Connection pool management and monitoring
- Incremental and full refresh sync modes
- File-based staging support
- Health monitoring and HTTP endpoints
- Comprehensive logging and debugging

Usage:
    python db_pipeline.py

Environment Variables:
    See config.py for all available configuration options.
"""

# flake8: noqa
import threading
import time
import signal
import sys
from typing import Any, Union, List

# Import configuration first
import config

# Import utilities
from utils import log, log_config, log_phase, log_status, log_error

# Import core modules
from database import create_engines, cleanup_engines, get_engines
from pipeline_management import load_select_tables_from_database
from monitoring import run_http_server, periodic_connection_monitoring
from error_handling import retry_on_connection_error

def validate_table_configurations():
    """Validate all table configurations for proper primary key setup."""
    from utils import validate_primary_key_config, log_primary_key_info
    
    log_config("🔍 Validating table configurations...")
    
    for table_name, table_config in config.table_configs.items():
        # Check if primary_key exists
        if "primary_key" not in table_config:
            log_error(f"❌ Table '{table_name}' missing primary_key configuration")
            continue
            
        primary_key = table_config["primary_key"]
        
        # Validate primary key configuration
        if not validate_primary_key_config(primary_key):
            log_error(f"❌ Table '{table_name}' has invalid primary_key configuration: {primary_key}")
            continue
            
        # Log primary key information
        log_primary_key_info(table_name, primary_key)
        
        # Check if modifier exists for incremental sync
        if "modifier" in table_config:
            log_config(f"📅 Table '{table_name}' configured for incremental sync using column: {table_config['modifier']}")
        else:
            log_config(f"🔄 Table '{table_name}' configured for full refresh sync")
    
    log_config(f"✅ Table configuration validation completed for {len(config.table_configs)} tables")

def run_pipeline():
    """Main pipeline execution function."""
    if config.INTERVAL > 0:
        log_phase(f"🔄 Starting continuous sync mode (interval: {config.INTERVAL}s)")
        while True:
            try:
                log_phase(f"\n🚀 Starting sync cycle at {time.strftime('%H:%M:%S')}")
                
                # Run the main sync process
                load_select_tables_from_database()
                
                log_status(f"✅ Sync cycle completed - waiting {config.INTERVAL}s until next cycle")
                time.sleep(config.INTERVAL)
                
            except KeyboardInterrupt:
                log_phase("🛑 Shutdown signal received - stopping gracefully")
                break
            except Exception as e:
                log_error(f"❌ FAILED: Sync cycle error")
                log_error(f"   Error: {e}")
                log_status(f"   Retrying in {config.INTERVAL}s...")
                time.sleep(config.INTERVAL)
    else:
        log_phase("🔄 Starting single execution mode")
        try:
            load_select_tables_from_database()
            log_status("✅ Single execution completed successfully")
        except Exception as e:
            log_error(f"❌ FAILED: Single execution error")
            log_error(f"   Error: {e}")

def signal_handler(signum, frame):
    """Handle shutdown signals gracefully."""
    log_phase(f"🛑 Received signal {signum}, shutting down gracefully...")
    cleanup_engines()
    sys.exit(0)

def main():
    """Main entry point for the pipeline."""
    try:
        # Set up signal handlers
        signal.signal(signal.SIGINT, signal_handler)
        signal.signal(signal.SIGTERM, signal_handler)
        
        log_config("🚀 DLT Database Sync Pipeline Starting")
        log_config(f"📋 Tables configured: {len(config.table_configs)}")
        log_config(f"🔧 Pipeline mode: {config.PIPELINE_MODE}")
        
        if config.PIPELINE_MODE.lower() == "file_staging":
            log_config("📁 Using file staging mode - extract to files first, then load to database")
        elif config.PIPELINE_MODE.lower() == "direct":
            log_config("⚡ Using direct mode - db-to-db pipeline via DLT")
        else:
            log_config(f"⚠️ Unknown pipeline mode '{config.PIPELINE_MODE}', will default to direct mode")
        
        log_config("🔄 Validating configurations...")
        
        # Validate table configurations
        validate_table_configurations()
        
        # Create database engines
        create_engines()
        
        log_config("🔄 Starting background services...")
        
        # Start HTTP server in background
        http_thread = threading.Thread(target=run_http_server, daemon=True)
        http_thread.start()
        
        # Start connection monitoring in background
        monitor_thread = threading.Thread(target=periodic_connection_monitoring, daemon=True)
        monitor_thread.start()
        
        log_config("🔄 Starting pipeline execution...")
        
        # Run the main pipeline
        run_pipeline()
        
    except KeyboardInterrupt:
        log_phase("🛑 Keyboard interrupt - shutting down")
    except Exception as e:
        log_error(f"❌ FATAL ERROR: Pipeline failure")
        log_error(f"   Error: {e}")
    finally:
        log_phase("🧹 Cleaning up...")
        cleanup_engines()
        log_phase("👋 Shutdown complete")

if __name__ == "__main__":
    main()