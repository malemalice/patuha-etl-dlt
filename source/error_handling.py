"""
Error handling and retry logic module for DLT Database Sync Pipeline.
Contains retry mechanisms, connection error handling, and recovery functions.
"""

import time
import random
from typing import Callable, Any
import config
from utils import log

def retry_on_connection_error(func: Callable, db_type: str = "unknown", *args, **kwargs) -> Any:
    """Enhanced retry function with lock timeout handling, deadlock detection, and exponential backoff."""
    max_retries = 5
    base_delay = 10  # Base delay in seconds
    max_delay = 300  # Maximum delay cap
    jitter_factor = 0.1  # 10% jitter
    
    for attempt in range(max_retries):
        try:
            return func(*args, **kwargs)
            
        except Exception as e:
            error_message = str(e).lower()
            error_type = "Unknown"
            
            # Classify error types
            if any(keyword in error_message for keyword in [
                'connection lost', 'server has gone away', 'connection was killed',
                'mysql server has gone away', 'lost connection to mysql server'
            ]):
                error_type = "Connection"
                
            elif any(keyword in error_message for keyword in [
                'lock wait timeout exceeded', 'deadlock found when trying to get lock',
                'try restarting transaction'
            ]):
                error_type = "Lock timeout"
                
            elif any(keyword in error_message for keyword in [
                'jsondecodeerror', 'orjson', 'json decode error', 'invalid json'
            ]):
                error_type = "JSON serialization"
                
            elif any(keyword in error_message for keyword in [
                'decompress_state', 'load_pipeline_state', 'state file corrupted',
                'pipeline state', 'dlt state'
            ]):
                error_type = "DLT state corruption"
                
            elif any(keyword in error_message for keyword in [
                'queuepool limit', 'connection pool', 'pool timeout',
                'connection pool exhausted'
            ]):
                error_type = "Connection pool"
                
            elif any(keyword in error_message for keyword in [
                'commands out of sync', 'you can\'t run this command now',
                'mysql protocol out of sync'
            ]):
                error_type = "MySQL protocol sync"
                
            else:
                log(f"❌ FAILED: Non-retryable error for {db_type}")
                log(f"   Error: {e}")
                raise
            
            if attempt < max_retries - 1:
                # Calculate delay with exponential backoff and jitter
                delay = min(base_delay * (2 ** attempt), max_delay)
                jitter = random.uniform(-jitter_factor * delay, jitter_factor * delay)
                total_delay = max(1, delay + jitter)
                
                log(f"🔄 {error_type} error - retrying in {total_delay:.1f}s (attempt {attempt + 2}/{max_retries})")
                time.sleep(total_delay)
            else:
                log(f"❌ FAILED: Max retries exceeded for {db_type}")
                log(f"   Error type: {error_type}")
                log(f"   Error: {e}")
                raise

def retry_on_lock_timeout(func: Callable, db_type: str = "unknown", operation_name: str = "operation", *args, **kwargs) -> Any:
    """Enhanced retry function specifically for lock timeout and deadlock errors with exponential backoff."""
    
    for attempt in range(config.LOCK_TIMEOUT_RETRIES):
        try:
            return func(*args, **kwargs)
            
        except Exception as e:
            error_message = str(e).lower()
            
            # Check for lock-related errors
            is_lock_error = any(keyword in error_message for keyword in [
                'lock wait timeout exceeded',
                'deadlock found when trying to get lock',
                'try restarting transaction',
                'connection was killed',
                'lock timeout'
            ])
            
            # Check for connection errors during lock operations
            is_connection_error = any(keyword in error_message for keyword in [
                'connection lost',
                'server has gone away', 
                'mysql server has gone away',
                'lost connection to mysql server'
            ])
            
            if is_lock_error or is_connection_error:
                error_type = "Lock timeout" if is_lock_error else "Connection"
                
                if attempt < config.LOCK_TIMEOUT_RETRIES - 1:
                    # Calculate delay with exponential backoff and jitter
                    delay = min(config.LOCK_TIMEOUT_BASE_DELAY * (2 ** attempt), config.LOCK_TIMEOUT_MAX_DELAY)
                    jitter = random.uniform(-config.LOCK_TIMEOUT_JITTER * delay, config.LOCK_TIMEOUT_JITTER * delay)
                    total_delay = max(1, delay + jitter)
                    
                    log(f"🔄 {error_type} error - retrying {operation_name} in {total_delay:.1f}s (attempt {attempt + 2}/{config.LOCK_TIMEOUT_RETRIES})")
                    time.sleep(total_delay)
                else:
                    log(f"❌ FAILED: Max retries exceeded for {operation_name} [{db_type}]")
                    log(f"   Error type: {error_type}")
                    log(f"   Error: {e}")
                    raise
            else:
                # Non-lock error, don't retry
                log(f"❌ FAILED: Non-retryable error for {operation_name} [{db_type}]")
                log(f"   Error: {e}")
                raise
    
    # This should never be reached, but just in case
    raise Exception(f"Unexpected end of retry loop for {operation_name} [{db_type}]")

def retry_on_connection_loss(func: Callable, db_type: str = "unknown", operation_name: str = "operation", *args, **kwargs) -> Any:
    """Enhanced retry function specifically for connection loss scenarios with intelligent recovery."""
    
    for attempt in range(config.CONNECTION_LOSS_RETRIES):
        try:
            return func(*args, **kwargs)
            
        except Exception as e:
            error_message = str(e).lower()
            
            # Check for connection loss errors
            is_connection_loss = any(keyword in error_message for keyword in [
                'connection lost',
                'server has gone away',
                'mysql server has gone away', 
                'lost connection to mysql server',
                'connection was killed',
                'broken pipe',
                'connection reset by peer',
                'connection timeout'
            ])
            
            if is_connection_loss:
                if attempt < config.CONNECTION_LOSS_RETRIES - 1:
                    # For connection loss, use a shorter delay but still with backoff
                    delay = min(5 * (2 ** attempt), 60)  # 5s, 10s, 20s, max 60s
                    jitter = random.uniform(-0.2 * delay, 0.2 * delay)  # 20% jitter
                    total_delay = max(2, delay + jitter)
                    
                    log(f"🔄 Connection lost - retrying {operation_name} in {total_delay:.1f}s (attempt {attempt + 2}/{config.CONNECTION_LOSS_RETRIES})")
                    time.sleep(total_delay)
                else:
                    log(f"❌ FAILED: Max connection retries exceeded for {operation_name} [{db_type}]")
                    log(f"   Error: {e}")
                    raise
            else:
                # Non-connection error, don't retry
                log(f"❌ FAILED: Non-connection error for {operation_name} [{db_type}]")
                log(f"   Error: {e}")
                raise
    
    # This should never be reached, but just in case
    raise Exception(f"Unexpected end of connection loss retry loop for {operation_name} [{db_type}]")

def execute_with_connection_loss_handling(engine, operation_name: str, operation_func: Callable, *args, **kwargs) -> Any:
    """Execute database operations with connection loss handling and automatic recovery."""
    def _wrapped_operation():
        return operation_func(engine, *args, **kwargs)
    
    return retry_on_connection_loss(
        _wrapped_operation,
        db_type="database",
        operation_name=operation_name
    )