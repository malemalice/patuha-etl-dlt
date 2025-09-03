"""
Database connection and management module for DLT Database Sync Pipeline.
Contains database engine creation, connection management, and utility functions.
"""

import os
import time
import sqlalchemy as sa
from sqlalchemy import create_engine, text
from sqlalchemy.pool import QueuePool
from typing import Tuple, Optional
import config
from utils import log, log_config, log_debug, log_error

# Global engine references
ENGINE_SOURCE = None
ENGINE_TARGET = None

def _get_mysql_connect_args():
    """Get MySQL-specific connection arguments based on available drivers."""
    mysql_connect_args = {}
    
    try:
        # Check if MySQLdb (mysqlclient) is available
        import MySQLdb
        log_config("🔧 Detected MySQLdb/mysqlclient driver")
        
        # MySQLdb-specific arguments
        mysql_connect_args.update({
            'charset': 'utf8mb4',
            'use_unicode': True,
            'autocommit': False,
            'sql_mode': 'TRADITIONAL',
            'init_command': "SET SESSION sql_mode='TRADITIONAL'",
            'connect_timeout': 60,
            'read_timeout': 60,
            'write_timeout': 60,
            'local_infile': False,
            'ssl': False
        })
        
    except ImportError:
        try:
            # Check if mysql-connector-python is available
            import mysql.connector
            log_config("🔧 Detected mysql-connector-python driver")
            
            # mysql-connector-python-specific arguments
            mysql_connect_args.update({
                'charset': 'utf8mb4',
                'use_unicode': True,
                'autocommit': False,
                'connect_timeout': 60,
                'ssl_disabled': True
            })
            
        except ImportError:
            try:
                # Check if pymysql is available
                import pymysql
                log_config("🔧 Detected pymysql driver")
                
                # pymysql-specific arguments
                mysql_connect_args.update({
                    'charset': 'utf8mb4',
                    'use_unicode': True,
                    'autocommit': False,
                    'sql_mode': 'TRADITIONAL',
                    'init_command': "SET SESSION sql_mode='TRADITIONAL'",
                    'connect_timeout': 60,
                    'read_timeout': 60,
                    'write_timeout': 60,
                    'local_infile': False,
                    'ssl': False
                })
                
            except ImportError:
                log_config("⚠️ No MySQL driver detected, using base arguments")
                # Base arguments for any MySQL driver
                mysql_connect_args.update({
                    'charset': 'utf8mb4',
                    'use_unicode': True,
                    'autocommit': False
                })
    
    return mysql_connect_args

def _configure_session(engine, db_type):
    """Configure database session with MariaDB-optimized settings."""
    try:
        with engine.connect() as connection:
            # Set session variables for MariaDB optimization
            session_commands = [
                "SET SESSION sql_mode='TRADITIONAL'",
                "SET SESSION innodb_lock_wait_timeout=120",
                "SET SESSION lock_wait_timeout=120",
                "SET SESSION wait_timeout=28800",
                "SET SESSION interactive_timeout=28800",
                "SET SESSION net_read_timeout=60",
                "SET SESSION net_write_timeout=60"
            ]
            
            for command in session_commands:
                try:
                    connection.execute(text(command))
                    connection.commit()
                except Exception as cmd_error:
                    log_debug(f"⚠️ Session command failed (non-critical): {command} - {cmd_error}")
                    
    except Exception as e:
        log_debug(f"⚠️ Session configuration failed (non-critical): {e}")

def _configure_database_session(connection):
    """Configure database session settings for a specific connection."""
    try:
        # Set session variables for MariaDB optimization
        session_commands = [
            "SET SESSION sql_mode='TRADITIONAL'",
            "SET SESSION innodb_lock_wait_timeout=120",
            "SET SESSION lock_wait_timeout=120",
            "SET SESSION wait_timeout=28800",
            "SET SESSION interactive_timeout=28800",
            "SET SESSION net_read_timeout=60",
            "SET SESSION net_write_timeout=60"
        ]
        
        for command in session_commands:
            try:
                connection.execute(text(command))
                connection.commit()
            except Exception as cmd_error:
                log_debug(f"⚠️ Session command failed (non-critical): {command} - {cmd_error}")
                
    except Exception as e:
        log_debug(f"⚠️ Session configuration failed (non-critical): {e}")

def create_engines():
    """Create database engines with connection pooling and MariaDB optimization."""
    global ENGINE_SOURCE, ENGINE_TARGET
    
    try:
        log_config("🔧 Creating database engines...")
        
        # Get MySQL-specific connection arguments
        mysql_connect_args = _get_mysql_connect_args()
        
        # Source database engine - use URL-encoded credentials from config
        source_url = f"mysql+pymysql://{config._url_encode_credential(config.SOURCE_DB_USER)}:{config._url_encode_credential(config.SOURCE_DB_PASS)}@{config.SOURCE_DB_HOST}:{config.SOURCE_DB_PORT}/{config.SOURCE_DB_NAME}"
        
        # Target database engine - use URL-encoded credentials from config
        target_url = f"mysql+pymysql://{config._url_encode_credential(config.TARGET_DB_USER)}:{config._url_encode_credential(config.TARGET_DB_PASS)}@{config.TARGET_DB_HOST}:{config.TARGET_DB_PORT}/{config.TARGET_DB_NAME}"
        
        # Connection pool configuration
        pool_config = {
            'poolclass': QueuePool,
            'pool_size': config.POOL_SIZE,
            'max_overflow': config.MAX_OVERFLOW,
            'pool_timeout': config.POOL_TIMEOUT,
            'pool_recycle': config.POOL_RECYCLE,
            'pool_pre_ping': config.POOL_PRE_PING,
            'echo': False
        }
        
        log_config(f"🔧 Using MySQL connection arguments: {list(mysql_connect_args.keys())}")
        
        # Create source engine
        log_config("🔄 Creating source database engine...")
        ENGINE_SOURCE = create_engine(
            source_url,
            **pool_config,
            connect_args=mysql_connect_args
        )
        
        # Create target engine
        log_config("🔄 Creating target database engine...")
        ENGINE_TARGET = create_engine(
            target_url,
            **pool_config,
            connect_args=mysql_connect_args
        )
        
        # Configure sessions
        _configure_session(ENGINE_SOURCE, "source")
        _configure_session(ENGINE_TARGET, "target")
        
        # Test connections
        log_config("🔄 Testing source database connection...")
        with ENGINE_SOURCE.connect() as conn:
            conn.execute(text("SELECT 1"))
        
        log_config("🔄 Testing target database connection...")
        with ENGINE_TARGET.connect() as conn:
            conn.execute(text("SELECT 1"))
        
        log_config("✅ Database engines created and tested successfully")
        
        # Store engines in config for global access
        config.ENGINE_SOURCE = ENGINE_SOURCE
        config.ENGINE_TARGET = ENGINE_TARGET
        
    except Exception as e:
        log_error(f"❌ FAILED: Database engine creation error")
        log_error(f"   Error: {e}")
        log_error(f"   Check database connectivity and credentials")
        raise

def get_engines() -> Tuple[Optional[sa.Engine], Optional[sa.Engine]]:
    """Get the current database engines."""
    return ENGINE_SOURCE, ENGINE_TARGET

def cleanup_engines():
    """Clean up database engines and close connections."""
    global ENGINE_SOURCE, ENGINE_TARGET
    
    log_config("🔄 Cleaning up database engines...")
    
    if ENGINE_SOURCE:
        try:
            ENGINE_SOURCE.dispose()
            ENGINE_SOURCE = None
        except Exception as e:
            log_debug(f"⚠️ Error disposing source engine: {e}")
    
    if ENGINE_TARGET:
        try:
            ENGINE_TARGET.dispose()
            ENGINE_TARGET = None
        except Exception as e:
            log_debug(f"⚠️ Error disposing target engine: {e}")
    
    # Clear from config
    config.ENGINE_SOURCE = None
    config.ENGINE_TARGET = None

def create_source_engine():
    """Create only the source database engine."""
    try:
        log("🔄 Creating source database engine only...")
        
        pool_settings = {
            'pool_size': config.POOL_SIZE,
            'max_overflow': config.MAX_OVERFLOW,
            'pool_timeout': config.POOL_TIMEOUT,
            'pool_recycle': config.POOL_RECYCLE,
            'pool_pre_ping': config.POOL_PRE_PING,
            'pool_reset_on_return': 'commit',
        }
        
        mysql_connect_args = _get_mysql_connect_args()
        
        engine = sa.create_engine(
            config.DB_SOURCE_URL,
            connect_args=mysql_connect_args,
            **pool_settings
        )
        
        # Test connection
        with engine.connect() as conn:
            conn.execute(sa.text("SELECT 1"))
            _configure_database_session(conn)
        
        log("✅ Source database engine created successfully")
        return engine
        
    except Exception as e:
        log(f"❌ Failed to create source engine: {e}")
        return None

def create_target_engine():
    """Create only the target database engine."""
    try:
        log("🔄 Creating target database engine only...")
        
        pool_settings = {
            'pool_size': config.POOL_SIZE,
            'max_overflow': config.MAX_OVERFLOW,
            'pool_timeout': config.POOL_TIMEOUT,
            'pool_recycle': config.POOL_RECYCLE,
            'pool_pre_ping': config.POOL_PRE_PING,
            'pool_reset_on_return': 'commit',
        }
        
        mysql_connect_args = _get_mysql_connect_args()
        
        engine = sa.create_engine(
            config.DB_TARGET_URL,
            connect_args=mysql_connect_args,
            **pool_settings
        )
        
        # Test connection
        with engine.connect() as conn:
            conn.execute(sa.text("SELECT 1"))
            _configure_database_session(conn)
        
        log("✅ Target database engine created successfully")
        return engine
        
    except Exception as e:
        log(f"❌ Failed to create target engine: {e}")
        return None

def check_connection_pool_health(engine, pool_name="database"):
    """Check if connection pool is healthy and not corrupted."""
    try:
        if not hasattr(engine, 'pool'):
            return False, "No pool attribute"
        
        pool = engine.pool
        
        # Check if pool has required methods
        if not hasattr(pool, 'size') or not hasattr(pool, 'overflow'):
            return False, "Pool missing required methods"
        
        # Check for negative overflow (indicates corruption)
        try:
            overflow = pool.overflow()
            if overflow < 0:
                return False, f"Pool corrupted (overflow: {overflow})"
        except Exception as e:
            return False, f"Error checking overflow: {e}"
        
        # Check pool size
        try:
            size = pool.size()
            if size <= 0:
                return False, f"Invalid pool size: {size}"
        except Exception as e:
            return False, f"Error checking pool size: {e}"
        
        # Test actual connection to ensure it's working
        try:
            with engine.connect() as conn:
                conn.execute(sa.text("SELECT 1"))
            return True, "Pool healthy"
        except Exception as conn_error:
            return False, f"Connection test failed: {conn_error}"
        
    except Exception as e:
        return False, f"Pool health check failed: {e}"

def get_configured_connection(engine, purpose="database"):
    """Get a database connection with session configuration applied."""
    try:
        # Test connection before using it
        if engine is None:
            raise Exception("Engine is None")
            
        connection = engine.connect()
        
        # Test if connection is actually working
        try:
            connection.execute(sa.text("SELECT 1"))
        except Exception as test_error:
            log(f"⚠️ Connection test failed: {test_error}")
            connection.close()
            raise Exception(f"Connection test failed: {test_error}")
        
        # Configure session settings
        _configure_database_session(connection)
        log(f"✅ {purpose} connection established and configured")
        return connection
    except Exception as e:
        log(f"❌ Failed to get configured {purpose} connection: {e}")
        raise

def _execute_transaction(engine, operation_func, *args, **kwargs):
    """Internal function to execute operations within a transaction context."""
    with engine.begin() as connection:
        # Configure session settings for this transaction
        _configure_database_session(connection)
        return operation_func(connection, *args, **kwargs)

def execute_with_transaction_management(engine, operation_name, operation_func, *args, **kwargs):
    """Execute database operations with proper transaction management and lock timeout handling."""
    
    # Acquire semaphore to limit concurrent transactions
    config.transaction_semaphore.acquire()
    
    try:
        result = _execute_transaction(engine, operation_func, *args, **kwargs)
        return result
        
    except Exception as e:
        log(f"❌ FAILED: Transaction error for {operation_name}")
        log(f"   Error: {e}")
        raise
    finally:
        # Always release the semaphore
        config.transaction_semaphore.release()

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