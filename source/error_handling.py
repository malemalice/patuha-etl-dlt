"""
Error handling and retry mechanisms for DLT Database Sync Pipeline.
Contains retry logic for various database errors and connection issues.
"""

import time
import random
from typing import Callable, Any, TypeVar, Union
import sqlalchemy as sa
from sqlalchemy.exc import OperationalError, DisconnectionError, TimeoutError
import config
from utils import log, log_debug, log_error

T = TypeVar('T')

def retry_on_connection_error(func: Callable[[], T], operation_name: str = "operation", max_retries: int = None) -> T:
    """Retry function execution on connection-related errors."""
    if max_retries is None:
        max_retries = config.CONNECTION_LOSS_RETRIES
    
    for attempt in range(max_retries + 1):
        try:
            return func()
        except (OperationalError, DisconnectionError, TimeoutError) as e:
            error_type = type(e).__name__
            
            if attempt == max_retries:
                log_error(f"❌ FAILED: Max retries exceeded for {operation_name}")
                log_error(f"   Error type: {error_type}")
                log_error(f"   Error: {e}")
                raise
            
            # Calculate delay with exponential backoff and jitter
            base_delay = 2 ** attempt
            jitter = random.uniform(0.8, 1.2)
            total_delay = base_delay * jitter
            
            log_debug(f"🔄 {error_type} error - retrying in {total_delay:.1f}s (attempt {attempt + 2}/{max_retries + 1})")
            time.sleep(total_delay)
    
    # This should never be reached, but just in case
    raise RuntimeError(f"Unexpected retry loop exit for {operation_name}")

def retry_on_lock_timeout(func: Callable[[], T], operation_name: str = "operation", db_type: str = "database") -> T:
    """Retry function execution on lock timeout errors."""
    for attempt in range(config.LOCK_TIMEOUT_RETRIES):
        try:
            return func()
        except Exception as e:
            error_message = str(e).lower()
            
            # Check if it's a lock timeout error
            if any(keyword in error_message for keyword in ['lock', 'timeout', 'deadlock', 'innodb_lock_wait_timeout']):
                if attempt == config.LOCK_TIMEOUT_RETRIES - 1:
                    log_error(f"❌ FAILED: Max retries exceeded for {operation_name} [{db_type}]")
                    log_error(f"   Error type: Lock timeout")
                    log_error(f"   Error: {e}")
                    raise
                
                # Calculate delay with exponential backoff and jitter
                base_delay = config.LOCK_TIMEOUT_BASE_DELAY * (2 ** attempt)
                jitter = random.uniform(1 - config.LOCK_TIMEOUT_JITTER, 1 + config.LOCK_TIMEOUT_JITTER)
                total_delay = min(base_delay * jitter, config.LOCK_TIMEOUT_MAX_DELAY)
                
                log_debug(f"🔄 Lock timeout error - retrying {operation_name} in {total_delay:.1f}s (attempt {attempt + 2}/{config.LOCK_TIMEOUT_RETRIES})")
                time.sleep(total_delay)
            else:
                # Non-lock timeout error
                log_error(f"❌ FAILED: Non-retryable error for {operation_name} [{db_type}]")
                log_error(f"   Error: {e}")
                raise
    
    # This should never be reached
    raise RuntimeError(f"Unexpected retry loop exit for {operation_name}")

def retry_on_connection_loss(func: Callable[[], T], operation_name: str = "operation", db_type: str = "database") -> T:
    """Retry function execution on connection loss errors."""
    for attempt in range(config.CONNECTION_LOSS_RETRIES):
        try:
            return func()
        except Exception as e:
            error_message = str(e).lower()
            
            # Check if it's a connection loss error
            if any(keyword in error_message for keyword in ['connection', 'lost', 'disconnect', 'timeout', 'gone away']):
                if attempt == config.CONNECTION_LOSS_RETRIES - 1:
                    log_error(f"❌ FAILED: Max connection retries exceeded for {operation_name} [{db_type}]")
                    log_error(f"   Error: {e}")
                    raise
                
                # Calculate delay with exponential backoff
                base_delay = 5 * (2 ** attempt)
                jitter = random.uniform(0.8, 1.2)
                total_delay = base_delay * jitter
                
                log_debug(f"🔄 Connection lost - retrying {operation_name} in {total_delay:.1f}s (attempt {attempt + 2}/{config.CONNECTION_LOSS_RETRIES})")
                time.sleep(total_delay)
            else:
                # Non-connection error
                log_error(f"❌ FAILED: Non-connection error for {operation_name} [{db_type}]")
                log_error(f"   Error: {e}")
                raise
    
    # This should never be reached
    raise RuntimeError(f"Unexpected retry loop exit for {operation_name}")

def execute_with_connection_loss_handling(engine, operation_name: str, operation_func: Callable, *args, **kwargs) -> Any:
    """Execute database operations with connection loss handling and automatic recovery."""
    def _wrapped_operation():
        return operation_func(engine, *args, **kwargs)
    
    return retry_on_connection_loss(
        _wrapped_operation,
        db_type="database",
        operation_name=operation_name
    )