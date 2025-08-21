#!/usr/bin/env python3
"""
Index Management Monitor for DLT Database Sync Pipeline.

A simple utility to monitor and display index optimization status.
Can be run standalone or imported to get optimization statistics.
"""

import sys
import json
import argparse
from typing import Dict, Any

# Add the source directory to Python path for imports
import os
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

def get_optimization_status() -> Dict[str, Any]:
    """Get current index optimization status."""
    try:
        from index_management import get_optimization_summary
        return get_optimization_summary()
    except ImportError as e:
        return {"error": f"Could not import index management: {e}"}
    except Exception as e:
        return {"error": f"Error getting optimization status: {e}"}

def display_optimization_status(status: Dict[str, Any]):
    """Display optimization status in a human-readable format."""
    print("🔧 DLT Index Optimization Status")
    print("=" * 50)
    
    if "error" in status:
        print(f"❌ Error: {status['error']}")
        return
    
    tables_optimized = status.get('tables_optimized', 0)
    total_indexes = status.get('total_indexes_created', 0)
    active_indexes = status.get('active_temporary_indexes', {})
    
    print(f"📊 Tables optimized: {tables_optimized}")
    print(f"📊 Total indexes created: {total_indexes}")
    print(f"📊 Active temporary indexes: {len(active_indexes)}")
    
    if active_indexes:
        print("\n🔍 Active temporary indexes by table:")
        for table_name, indexes in active_indexes.items():
            print(f"  • {table_name}: {len(indexes)} indexes")
            for index in indexes:
                print(f"    - {index}")
    else:
        print("\n✅ No active temporary indexes")

def check_dlt_configuration() -> Dict[str, Any]:
    """Check DLT and index optimization configuration."""
    try:
        import config
        
        config_status = {
            "index_optimization_enabled": getattr(config, 'ENABLE_INDEX_OPTIMIZATION', False),
            "index_optimization_timeout": getattr(config, 'INDEX_OPTIMIZATION_TIMEOUT', 45),
            "cleanup_temporary_indexes": getattr(config, 'CLEANUP_TEMPORARY_INDEXES', True),
            "truncate_staging_dataset": getattr(config, 'TRUNCATE_STAGING_DATASET', True),
            "pipeline_mode": getattr(config, 'PIPELINE_MODE', 'direct')
        }
        
        return config_status
    except ImportError as e:
        return {"error": f"Could not import config: {e}"}
    except Exception as e:
        return {"error": f"Error checking configuration: {e}"}

def display_configuration(config_status: Dict[str, Any]):
    """Display configuration status."""
    print("\n⚙️ Configuration Status")
    print("=" * 50)
    
    if "error" in config_status:
        print(f"❌ Error: {config_status['error']}")
        return
    
    index_opt_enabled = config_status.get('index_optimization_enabled', False)
    print(f"🔧 Index Optimization: {'✅ ENABLED' if index_opt_enabled else '❌ DISABLED'}")
    
    if index_opt_enabled:
        timeout = config_status.get('index_optimization_timeout', 45)
        cleanup = config_status.get('cleanup_temporary_indexes', True)
        print(f"⏱️  Optimization Timeout: {timeout}s")
        print(f"🧹 Cleanup Temporary Indexes: {'✅ YES' if cleanup else '❌ NO'}")
    
    staging_opt = config_status.get('truncate_staging_dataset', True)
    print(f"🗑️ DLT Staging Optimization: {'✅ ENABLED' if staging_opt else '❌ DISABLED'}")
    
    pipeline_mode = config_status.get('pipeline_mode', 'direct')
    print(f"🚀 Pipeline Mode: {pipeline_mode.upper()}")

def main():
    """Main function for the monitor utility."""
    parser = argparse.ArgumentParser(description='Monitor DLT Index Optimization')
    parser.add_argument('--json', action='store_true', help='Output in JSON format')
    parser.add_argument('--config-only', action='store_true', help='Show only configuration')
    parser.add_argument('--status-only', action='store_true', help='Show only optimization status')
    
    args = parser.parse_args()
    
    if args.json:
        # JSON output
        result = {}
        
        if not args.status_only:
            result['configuration'] = check_dlt_configuration()
        
        if not args.config_only:
            result['optimization_status'] = get_optimization_status()
        
        print(json.dumps(result, indent=2))
    else:
        # Human-readable output
        if not args.status_only:
            config_status = check_dlt_configuration()
            display_configuration(config_status)
        
        if not args.config_only:
            opt_status = get_optimization_status()
            display_optimization_status(opt_status)

if __name__ == "__main__":
    main()
