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
from utils import log

# Import core modules
from database import create_engines, cleanup_engines, get_engines
from pipeline_management import load_select_tables_from_database
from monitoring import run_http_server, periodic_connection_monitoring
from error_handling import retry_on_connection_error

def validate_table_configurations():
    """Validate all table configurations for proper primary key setup."""
    from utils import validate_primary_key_config, log_primary_key_info
    
    log("🔍 Validating table configurations...")
    
    for table_name, table_config in config.table_configs.items():
        # Check if primary_key exists
        if "primary_key" not in table_config:
            log(f"❌ Table '{table_name}' missing primary_key configuration")
            continue
            
        primary_key = table_config["primary_key"]
        
        # Validate primary key configuration
        if not validate_primary_key_config(primary_key):
            log(f"❌ Table '{table_name}' has invalid primary_key configuration: {primary_key}")
            continue
            
        # Log primary key information
        log_primary_key_info(table_name, primary_key)
        
        # Check if modifier exists for incremental sync
        if "modifier" in table_config:
            log(f"📅 Table '{table_name}' configured for incremental sync using column: {table_config['modifier']}")
        else:
            log(f"🔄 Table '{table_name}' configured for full refresh sync")
    
    log(f"✅ Table configuration validation completed for {len(config.table_configs)} tables")

def run_pipeline():
    """Main pipeline execution function."""
    if config.INTERVAL > 0:
        log(f"🔄 Running pipeline in continuous mode (interval: {config.INTERVAL} seconds)")
        while True:
            try:
                log(f"\n{'='*80}")
                log(f"🚀 Starting sync cycle at {time.strftime('%Y-%m-%d %H:%M:%S')}")
                
                # Run the main sync process
                load_select_tables_from_database()
                
                log(f"✅ Sync cycle completed at {time.strftime('%Y-%m-%d %H:%M:%S')}")
                log(f"⏳ Waiting {config.INTERVAL} seconds until next sync...")
                
                time.sleep(config.INTERVAL)
                
            except KeyboardInterrupt:
                log("🛑 Received interrupt signal, shutting down gracefully...")
                break
            except Exception as e:
                log(f"❌ Error in sync cycle: {e}")
                log(f"⏳ Waiting {config.INTERVAL} seconds before retry...")
                time.sleep(config.INTERVAL)
    else:
        log("🔄 Running pipeline in single execution mode")
        try:
            load_select_tables_from_database()
            log("✅ Single execution completed successfully")
        except Exception as e:
            log(f"❌ Error in single execution: {e}")
            sys.exit(1)

def signal_handler(signum, frame):
    """Handle shutdown signals gracefully."""
    log(f"🛑 Received signal {signum}, shutting down gracefully...")
    cleanup_engines()
    sys.exit(0)

def main():
    """Main function that orchestrates the entire pipeline."""
    try:
        # Set up signal handlers
        signal.signal(signal.SIGINT, signal_handler)
        signal.signal(signal.SIGTERM, signal_handler)
        
        log("🚀 Starting DLT Database Sync Pipeline")
        log(f"🐛 Debug mode: {'ON' if config.DEBUG_MODE else 'OFF'}")
        log(f"📊 Tables configured: {len(config.table_configs)}")
        
        # Validate table configurations
        validate_table_configurations()
        
        # Initialize database engines
        log("🔧 Initializing database connections...")
        create_engines()
        
        # Get engines for monitoring setup
        engine_source, engine_target = get_engines()
        
        # Start HTTP health check server in background thread
        log("🌐 Starting health check server...")
        http_thread = threading.Thread(target=run_http_server, daemon=True)
        http_thread.start()
        
        # Start connection monitoring in background thread
        log("🔍 Starting connection monitoring...")
        periodic_connection_monitoring(engine_target, interval_seconds=60)
        
        # Give background services time to start
        time.sleep(2)
        
        # Run the main pipeline
        log("🚀 Starting main pipeline execution...")
        run_pipeline()
        
    except KeyboardInterrupt:
        log("🛑 Received keyboard interrupt, shutting down...")
    except Exception as e:
        log(f"❌ Fatal error in main: {e}")
        raise
    finally:
        # Clean up resources
        log("🧹 Cleaning up resources...")
        cleanup_engines()
        log("👋 Pipeline shutdown complete")

if __name__ == "__main__":
    main()