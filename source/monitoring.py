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
from utils import log, log_debug, log_error
import socket

class SimpleHandler(SimpleHTTPRequestHandler):
    """Simple HTTP handler for health check endpoint with enhanced error handling."""
    
    def handle_one_request(self):
        """Override to handle connection reset errors gracefully."""
        try:
            self.raw_requestline = self.rfile.readline(65537)
            if not self.raw_requestline:
                self.close_connection = True
                return
            if not self.parse_request():
                return
            self.do_request()
        except ConnectionResetError:
            # Handle connection reset gracefully
            self.close_connection = True
            return
        except BrokenPipeError:
            # Handle broken pipe errors
            self.close_connection = True
            return
        except Exception as e:
            # Log other errors but don't crash
            log_debug(f"⚠️ HTTP request error: {e}")
            self.close_connection = True
            return
    
    def do_request(self):
        """Handle the actual request processing."""
        try:
            if self.command == "GET":
                self.do_GET()
            else:
                self.send_error(405, "Method not allowed")
        except Exception as e:
            log_error(f"❌ Error processing {self.command} request: {e}")
            try:
                self.send_error(500, "Internal server error")
            except:
                pass  # Connection might already be closed
    
    def do_GET(self):
        """Handle GET requests with enhanced error handling."""
        try:
            # Simple health check for root path
            if self.path == "/":
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
                self.send_header("Connection", "close")  # Explicitly close connection
                self.end_headers()
                
                # Write response with error handling
                try:
                    response_data = json.dumps(status_data, indent=2).encode()
                    self.wfile.write(response_data)
                    self.wfile.flush()
                except (ConnectionResetError, BrokenPipeError):
                    # Client disconnected, ignore
                    pass
                except Exception as e:
                    log_debug(f"⚠️ Error writing response: {e}")
                
            # Simple health check for /health endpoint
            elif self.path == "/health":
                self.send_response(200)
                self.send_header("Content-type", "text/plain")
                self.send_header("Connection", "close")
                self.end_headers()
                try:
                    self.wfile.write(b"OK")
                    self.wfile.flush()
                except (ConnectionResetError, BrokenPipeError):
                    pass
                except Exception as e:
                    log_debug(f"⚠️ Error writing health response: {e}")
            
            # 404 for unknown paths
            else:
                self.send_error(404, "Not found")
                    
        except Exception as e:
            log_error(f"❌ Error in do_GET: {e}")
            try:
                self.send_error(500, "Internal server error")
            except:
                pass  # Connection might already be closed
    
    def log_message(self, format, *args):
        """Override to use our logging system."""
        log_debug(f"HTTP: {format % args}")
    
    def log_error(self, format, *args):
        """Override to use our logging system for errors."""
        log_error(f"HTTP Error: {format % args}")

def run_http_server():
    """Run the HTTP health check server."""
    try:
        server = HTTPServer((config.HTTP_SERVER_HOST, config.HTTP_SERVER_PORT), SimpleHandler)
        log_debug(f"🌐 HTTP server started on port {config.HTTP_SERVER_PORT}")
        
        # Set server timeout
        server.timeout = config.HTTP_SERVER_TIMEOUT
        
        while True:
            try:
                server.handle_request()
            except Exception as e:
                log_debug(f"⚠️ HTTP request handling error: {e}")
                continue
                
    except Exception as e:
        log_error(f"❌ HTTP server error: {e}")

def periodic_connection_monitoring():
    """Periodically monitor connection pool health."""
    while True:
        try:
            time.sleep(60)  # Check every minute
            
            if config.ENGINE_SOURCE and config.ENGINE_TARGET:
                monitor_connection_pool_health(config.ENGINE_SOURCE, config.ENGINE_TARGET)
                
        except Exception as e:
            log_debug(f"⚠️ Periodic connection monitoring error: {e}")
            time.sleep(60)  # Wait before retrying

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
            WHERE TIME > :timeout_seconds 
            AND COMMAND != 'Sleep'
            AND ID != CONNECTION_ID()
            ORDER BY TIME DESC
            """
            
            result = connection.execute(sa.text(query), {"timeout_seconds": timeout_seconds})
            long_queries = result.fetchall()
            
            if long_queries:
                log_debug(f"🔍 Found {len(long_queries)} long-running queries (>{timeout_seconds}s)")
                
                for query_info in long_queries:
                    # Handle both tuple and dictionary result formats
                    if isinstance(query_info, dict):
                        # Dictionary format (newer SQLAlchemy versions)
                        query_id = query_info.get('ID')
                        query_time = query_info.get('TIME')
                        query_preview = query_info.get('INFO_PREVIEW') or "N/A"
                    else:
                        # Tuple format (older SQLAlchemy versions or certain drivers)
                        query_id = query_info[0] if len(query_info) > 0 else None
                        query_time = query_info[5] if len(query_info) > 5 else None
                        query_preview = query_info[7] if len(query_info) > 7 else "N/A"
                    
                    # Validate we have the required data
                    if query_id is not None and query_time is not None:
                        log_debug(f"🔍 Long query ID {query_id}: {query_time}s - {query_preview}")
                        
                        # Kill queries running longer than 2x timeout
                        if query_time > timeout_seconds * 2:
                            try:
                                kill_query = f"KILL {query_id}"
                                connection.execute(sa.text(kill_query))
                                log_debug(f"💀 Killed long-running query ID {query_id} ({query_time}s)")
                            except Exception as kill_error:
                                log_error(f"❌ Failed to kill query ID {query_id}: {kill_error}")
                    else:
                        log_debug(f"⚠️ Skipping query info with missing data: {query_info}")
            
        except Exception as query_error:
            log_error(f"❌ Error monitoring queries: {query_error}")
            # Log additional debug info
            log_debug(f"🔍 Debug: Query error type: {type(query_error)}")
            log_debug(f"🔍 Debug: Query error details: {str(query_error)}")
    
    try:
        from database import execute_with_transaction_management
        execute_with_transaction_management(
            engine_target,
            "monitor_and_kill_long_queries",
            _monitor_queries
        )
    except Exception as e:
        log_error(f"❌ Error in query monitoring: {e}")

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

def monitor_connection_pool_health(engine_source, engine_target):
    """Monitor connection pool health for both engines."""
    try:
        if engine_source and hasattr(engine_source, 'pool'):
            source_pool = engine_source.pool
            source_status = {
                'size': source_pool.size(),
                'checked_out': source_pool.checkedout(),
                'overflow': source_pool.overflow(),
                'checked_in': source_pool.checkedin()
            }
            log_debug(f"Source pool status - Size: {source_status['size']}, Checked out: {source_status['checked_out']}")
        
        if engine_target and hasattr(engine_target, 'pool'):
            target_pool = engine_target.pool
            target_status = {
                'size': target_pool.size(),
                'checked_out': target_pool.checkedout(),
                'overflow': target_pool.overflow(),
                'checked_in': target_pool.checkedin()
            }
            log_debug(f"Target pool status - Size: {target_status['size']}, Checked out: {target_status['checked_out']}")
        
        return True
        
    except Exception as e:
        log_debug(f"⚠️ Connection pool monitoring error: {e}")
        return False

def log_performance_metrics(operation_name: str, start_time: float, end_time: float, records_processed: int = 0):
    """Log performance metrics for operations."""
    try:
        duration = end_time - start_time
        records_per_second = records_processed / duration if duration > 0 else 0
        
        log_debug(f"📊 Performance: {operation_name}")
        log_debug(f"   Duration: {duration:.2f}s")
        log_debug(f"   Records: {records_processed:,}")
        log_debug(f"   Rate: {records_per_second:.2f} records/sec")
        
    except Exception as e:
        log_debug(f"⚠️ Error logging performance metrics: {e}")