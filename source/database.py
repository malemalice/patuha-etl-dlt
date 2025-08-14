"""
Database connection and management module for DLT Database Sync Pipeline.
Contains engine creation, connection pooling, and basic database operations.
"""

import sqlalchemy as sa
import config
from utils import log

def create_engines():
    """Create SQLAlchemy engines with optimized connection pool settings and MySQL-specific parameters."""
    
    pool_settings = {
        'pool_size': 20,           # Configurable base pool size
        'max_overflow': 30,        # Configurable overflow limit  
        'pool_timeout': 60,        # Configurable timeout
        'pool_recycle': 3600,      # Configurable connection recycle time
        'pool_pre_ping': True,     # Validate connections before use
    }
    
    # MySQL-specific connection arguments to handle timeouts and connection stability
    mysql_connect_args = {
        'connect_timeout': 60,
        'read_timeout': 300,
        'write_timeout': 300,
        'autocommit': False,  # Better transaction control
        'charset': 'utf8mb4',
        'init_command': "SET sql_mode=(SELECT REPLACE(@@sql_mode,'ONLY_FULL_GROUP_BY','')); SET SESSION innodb_lock_wait_timeout=120; SET SESSION lock_wait_timeout=120",
        'use_unicode': True,
    }
    
    log("🔧 Creating database engines with optimized connection pool settings...")
    log(f"🔧 Pool settings: {pool_settings}")
    log(f"🔧 MySQL connection args: {mysql_connect_args}")
    
    try:
        config.ENGINE_SOURCE = sa.create_engine(
            config.DB_SOURCE_URL,
            connect_args=mysql_connect_args,
            **pool_settings
        )
        log(f"✅ Source engine created successfully")
        
        config.ENGINE_TARGET = sa.create_engine(
            config.DB_TARGET_URL,
            connect_args=mysql_connect_args,
            **pool_settings
        )
        log(f"✅ Target engine created successfully")
        
        # Test connections
        with config.ENGINE_SOURCE.connect() as conn:
            conn.execute(sa.text("SELECT 1"))
        log("✅ Source database connection test successful")
        
        with config.ENGINE_TARGET.connect() as conn:
            conn.execute(sa.text("SELECT 1"))
        log("✅ Target database connection test successful")
        
    except Exception as e:
        log(f"❌ Error creating database engines: {e}")
        raise

def cleanup_engines():
    """Clean up engine resources on shutdown."""
    try:
        if config.ENGINE_SOURCE:
            config.ENGINE_SOURCE.dispose()
            log("✅ Source engine disposed")
        if config.ENGINE_TARGET:
            config.ENGINE_TARGET.dispose()
            log("✅ Target engine disposed")
    except Exception as e:
        log(f"❌ Error disposing engines: {e}")

def get_engines():
    """Get the global database engines."""
    if config.ENGINE_SOURCE is None or config.ENGINE_TARGET is None:
        create_engines()
    
    return config.ENGINE_SOURCE, config.ENGINE_TARGET

def _execute_transaction(engine, operation_func, *args, **kwargs):
    """Internal function to execute operations within a transaction context."""
    with engine.begin() as connection:
        # Set transaction timeout
        connection.execute(sa.text(f"SET SESSION innodb_lock_wait_timeout = {config.TRANSACTION_TIMEOUT}"))
        connection.execute(sa.text(f"SET SESSION lock_wait_timeout = {config.TRANSACTION_TIMEOUT}"))
        return operation_func(connection, *args, **kwargs)

def execute_with_transaction_management(engine, operation_name, operation_func, *args, **kwargs):
    """Execute database operations with proper transaction management and lock timeout handling.
    
    This function:
    1. Acquires a transaction semaphore to limit concurrent transactions
    2. Executes the operation within a transaction context
    3. Handles lock timeouts and connection issues
    4. Releases the semaphore when done
    """
    log(f"🔄 Starting transaction for {operation_name}")
    
    # Acquire semaphore to limit concurrent transactions
    config.transaction_semaphore.acquire()
    
    try:
        result = _execute_transaction(engine, operation_func, *args, **kwargs)
        log(f"✅ Transaction completed successfully for {operation_name}")
        return result
        
    except Exception as e:
        log(f"❌ Transaction failed for {operation_name}: {e}")
        raise
    finally:
        # Always release the semaphore
        config.transaction_semaphore.release()
        log(f"🔄 Transaction semaphore released for {operation_name}")

def ensure_dlt_columns(engine_target, table_name):
    """Check if _dlt_load_id and _dlt_id exist in the target table, add them if not."""
    def _ensure_columns(connection):
        inspector = sa.inspect(engine_target)
        columns = [col["name"] for col in inspector.get_columns(table_name)]
        
        alter_statements = []
        if "_dlt_load_id" not in columns:
            alter_statements.append("ADD COLUMN `_dlt_load_id` VARCHAR(255)")
        if "_dlt_id" not in columns:
            alter_statements.append("ADD COLUMN `_dlt_id` VARCHAR(255)")
        
        if alter_statements:
            alter_query = f"ALTER TABLE {table_name} {', '.join(alter_statements)};"
            log(f"🔧 Adding DLT columns to {table_name}: {alter_query}")
            connection.execute(sa.text(alter_query))
            return True
        return False
    
    return execute_with_transaction_management(
        engine_target, 
        f"ensure_dlt_columns for {table_name}", 
        _ensure_columns
    )