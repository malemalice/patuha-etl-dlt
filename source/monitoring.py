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
            log(f"⚠️ HTTP request error: {e}")
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
            log(f"❌ Error processing {self.command} request: {e}")
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
                    log(f"⚠️ Error writing response: {e}")
            
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
                    log(f"⚠️ Error writing health response: {e}")
            
            # 404 for unknown paths
            else:
                self.send_response(404)
                self.send_header("Content-type", "text/plain")
                self.send_header("Connection", "close")
                self.end_headers()
                try:
                    self.wfile.write(b"Not Found")
                    self.wfile.flush()
                except (ConnectionResetError, BrokenPipeError):
                    pass
                except Exception as e:
                    log(f"⚠️ Error writing 404 response: {e}")
                    
        except Exception as e:
            log(f"❌ Error in do_GET: {e}")
            try:
                self.send_response(500)
                self.send_header("Content-type", "application/json")
                self.send_header("Connection", "close")
                self.end_headers()
                error_response = {"status": "error", "message": str(e)}
                self.wfile.write(json.dumps(error_response).encode())
            except:
                pass  # Connection might already be closed
    
    def log_message(self, format, *args):
        """Override to use our logging system instead of stderr."""
        log(f"🌐 HTTP: {format % args}")
    
    def log_error(self, format, *args):
        """Override to use our logging system for errors."""
        log(f"❌ HTTP Error: {format % args}")

def run_http_server():
    """Run the HTTP health check server with enhanced error handling."""
    server_address = (config.HTTP_SERVER_HOST, config.HTTP_SERVER_PORT)
    
    try:
        httpd = HTTPServer(server_address, SimpleHandler)
        
        # Set socket options for better connection handling
        if config.HTTP_SERVER_ENABLE_REUSEADDR:
            httpd.socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        if config.HTTP_SERVER_ENABLE_KEEPALIVE:
            httpd.socket.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)
        
        # Set timeout for socket operations
        httpd.socket.settimeout(config.HTTP_SERVER_TIMEOUT)
        
        log(f"🌐 Health check server starting on {config.HTTP_SERVER_HOST or 'all interfaces'}:{config.HTTP_SERVER_PORT}...")
        
        request_count = 0
        
        # Enhanced serve_forever with error handling and request counting
        while True:
            try:
                httpd.handle_request()
                request_count += 1
                
                # Restart server after max requests to prevent memory leaks
                if request_count >= config.HTTP_SERVER_MAX_REQUESTS:
                    log(f"🔄 HTTP server restarting after {request_count} requests")
                    httpd.server_close()
                    httpd = HTTPServer(server_address, SimpleHandler)
                    if config.HTTP_SERVER_ENABLE_REUSEADDR:
                        httpd.socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
                    if config.HTTP_SERVER_ENABLE_KEEPALIVE:
                        httpd.socket.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)
                    httpd.socket.settimeout(config.HTTP_SERVER_TIMEOUT)
                    request_count = 0
                    log("✅ HTTP server restarted successfully")
                    
            except KeyboardInterrupt:
                log("🛑 HTTP server shutdown requested")
                break
            except Exception as e:
                log(f"⚠️ HTTP server error: {e}")
                # Continue serving other requests
                continue
                
    except Exception as e:
        log(f"❌ Failed to start HTTP server: {e}")
        # Don't crash the entire pipeline if HTTP server fails
        return

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