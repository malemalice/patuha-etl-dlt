"""
Monitoring and health check module for DLT Database Sync Pipeline.
Contains health monitoring, connection monitoring, and performance tracking functions.
"""

import threading
import time
import json
from datetime import datetime
from http.server import SimpleHTTPRequestHandler, HTTPServer
import sqlalchemy as sa
import config
from utils import log

class SimpleHandler(SimpleHTTPRequestHandler):
    """Simple HTTP handler for health check endpoint."""
    def do_GET(self):
        try:
            status_data = {
                "status": "healthy",
                "timestamp": datetime.now().isoformat(),
                "message": "DLT Database Sync Pipeline is running"
            }
            
            # Check database connections if engines are available
            if config.ENGINE_SOURCE and config.ENGINE_TARGET:
                try:
                    with config.ENGINE_SOURCE.connect() as conn:
                        conn.execute(sa.text("SELECT 1"))
                    status_data["source_db"] = "connected"
                except Exception as e:
                    status_data["source_db"] = f"error: {str(e)}"
                    status_data["status"] = "degraded"
                
                try:
                    with config.ENGINE_TARGET.connect() as conn:
                        conn.execute(sa.text("SELECT 1"))
                    status_data["target_db"] = "connected"
                except Exception as e:
                    status_data["target_db"] = f"error: {str(e)}"
                    status_data["status"] = "degraded"
            
            self.send_response(200)
            self.send_header("Content-type", "application/json")
            self.end_headers()
            self.wfile.write(json.dumps(status_data, indent=2).encode())
            
        except Exception as e:
            self.send_response(500)
            self.send_header("Content-type", "application/json")
            self.end_headers()
            error_response = {"status": "error", "message": str(e)}
            self.wfile.write(json.dumps(error_response).encode())

def run_http_server():
    """Run the HTTP health check server."""
    server_address = ("", 8089)  # Serve on all interfaces, port 8089
    httpd = HTTPServer(server_address, SimpleHandler)
    log("🌐 Health check server starting on port 8089...")
    httpd.serve_forever()

def periodic_connection_monitoring(engine_target, interval_seconds=60):
    """Periodically monitor and clean up problematic connections and queries.
    
    This function runs in a separate thread and performs:
    1. Connection pool monitoring
    2. Long-running query detection and cleanup
    3. Resource usage monitoring
    """
    def _monitor():
        while True:
            try:
                # Monitor connection pool status
                if hasattr(engine_target.pool, 'size'):
                    pool_size = engine_target.pool.size()
                    checked_out = engine_target.pool.checkedout()
                    overflow = engine_target.pool.overflow()
                    
                    log(f"🔍 Connection Pool Status - Size: {pool_size}, Checked out: {checked_out}, Overflow: {overflow}")
                    
                    # Warning if pool utilization is high
                    utilization = (checked_out / (pool_size + overflow)) * 100 if (pool_size + overflow) > 0 else 0
                    if utilization > 80:
                        log(f"⚠️ High connection pool utilization: {utilization:.1f}%")
                
                # Monitor and kill long-running queries
                monitor_and_kill_long_queries(engine_target)
                
            except Exception as monitor_error:
                log(f"❌ Error in connection monitoring: {monitor_error}")
            
            time.sleep(interval_seconds)
    
    # Start monitoring thread
    monitor_thread = threading.Thread(target=_monitor, daemon=True)
    monitor_thread.start()
    log(f"🔍 Connection monitoring started (interval: {interval_seconds}s)")

def monitor_and_kill_long_queries(engine_target, timeout_seconds=300):
    """Monitor and kill long-running queries that might cause connection timeouts.
    
    This function identifies queries running longer than the specified timeout
    and kills them to prevent connection pool exhaustion.
    """
    def _monitor_queries(connection):
        try:
            # Get list of long-running queries
            query = """
            SELECT 
                ID, 
                USER, 
                HOST, 
                DB, 
                COMMAND, 
                TIME, 
                STATE, 
                LEFT(INFO, 100) as INFO_PREVIEW
            FROM INFORMATION_SCHEMA.PROCESSLIST 
            WHERE TIME > %s 
            AND COMMAND != 'Sleep'
            AND ID != CONNECTION_ID()
            ORDER BY TIME DESC
            """
            
            result = connection.execute(sa.text(query), [timeout_seconds])
            long_queries = result.fetchall()
            
            if long_queries:
                log(f"🔍 Found {len(long_queries)} long-running queries (>{timeout_seconds}s)")
                
                for query_info in long_queries:
                    query_id = query_info[0]
                    query_time = query_info[5]
                    query_preview = query_info[7] or "N/A"
                    
                    log(f"🔍 Long query ID {query_id}: {query_time}s - {query_preview}")
                    
                    # Kill queries running longer than 2x timeout
                    if query_time > timeout_seconds * 2:
                        try:
                            kill_query = f"KILL {query_id}"
                            connection.execute(sa.text(kill_query))
                            log(f"💀 Killed long-running query ID {query_id} ({query_time}s)")
                        except Exception as kill_error:
                            log(f"❌ Failed to kill query ID {query_id}: {kill_error}")
            
        except Exception as query_error:
            log(f"❌ Error monitoring queries: {query_error}")
    
    try:
        from database import execute_with_transaction_management
        execute_with_transaction_management(
            engine_target,
            "monitor_and_kill_long_queries",
            _monitor_queries
        )
    except Exception as e:
        log(f"❌ Error in query monitoring: {e}")

def get_connection_pool_status(engine):
    """Get detailed connection pool status."""
    try:
        if hasattr(engine.pool, 'size'):
            return {
                "size": engine.pool.size(),
                "checked_out": engine.pool.checkedout(),
                "overflow": engine.pool.overflow(),
                "invalid": getattr(engine.pool, 'invalid', 0)
            }
        else:
            return {"status": "pool_info_unavailable"}
    except Exception as e:
        return {"error": str(e)}

def log_performance_metrics(operation_name, start_time, end_time, record_count=None):
    """Log performance metrics for operations."""
    duration = end_time - start_time
    
    metrics_info = f"⏱️ {operation_name} completed in {duration:.2f}s"
    if record_count is not None:
        rate = record_count / duration if duration > 0 else 0
        metrics_info += f" ({record_count} records, {rate:.1f} records/sec)"
    
    log(metrics_info)