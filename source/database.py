"""
Database connection and management module for DLT Database Sync Pipeline.
Contains engine creation, connection pooling, and basic database operations.
"""

import sqlalchemy as sa
import config
from utils import log

def _get_mysql_connect_args():
    """Get MySQL connection arguments compatible with the available driver."""
    
    # Base connection arguments compatible with most MySQL drivers
    # Removed problematic init_command to avoid MariaDB syntax errors
    base_args = {
        'connect_timeout': 60,
        'autocommit': False,
        'charset': 'utf8mb4',
        'use_unicode': True,
        # init_command removed - will be handled after connection establishment
    }
    
    # Try to detect which MySQL driver is available and add driver-specific arguments
    try:
        # Check if MySQLdb (mysqlclient) is available
        import MySQLdb
        log("🔧 Detected MySQLdb/mysqlclient driver")
        # MySQLdb-specific arguments
        base_args.update({
            'read_timeout': 300,
            'write_timeout': 300,
        })
        return base_args
        
    except ImportError:
        try:
            # Check if mysql.connector (mysql-connector-python) is available
            import mysql.connector
            log("🔧 Detected mysql-connector-python driver")
            # mysql-connector-python specific arguments
            base_args.update({
                'use_pure': False,
                'buffered': True,
                'consume_results': True,
            })
            return base_args
            
        except ImportError:
            try:
                # Check if pymysql is available
                import pymysql
                log("🔧 Detected pymysql driver")
                # PyMySQL-specific arguments
                base_args.update({
                    'read_timeout': 300,
                    'write_timeout': 300,
                })
                return base_args
                
            except ImportError:
                log("⚠️ No MySQL driver detected, using base arguments")
                return base_args

def _configure_database_session(connection):
    """Configure database session with optimal settings after connection establishment."""
    try:
        # Execute session configuration commands individually to avoid syntax errors
        session_commands = [
            "SET sql_mode=(SELECT REPLACE(@@sql_mode,'ONLY_FULL_GROUP_BY',''))",
            "SET SESSION innodb_lock_wait_timeout=120",
            "SET SESSION lock_wait_timeout=120", 
            "SET SESSION wait_timeout=3600",
            "SET SESSION interactive_timeout=3600",
            "SET SESSION net_read_timeout=600",
            "SET SESSION net_write_timeout=600"
        ]
        
        for command in session_commands:
            try:
                connection.execute(sa.text(command))
                log(f"✅ Session configured: {command}")
            except Exception as cmd_error:
                # Log warning but don't fail - some commands might not be supported
                log(f"⚠️ Session command failed (non-critical): {command} - {cmd_error}")
                
    except Exception as e:
        log(f"⚠️ Session configuration failed (non-critical): {e}")
        # Don't raise - this is not critical for basic functionality

def create_engines():
    """Create SQLAlchemy engines with optimized connection pool settings and MySQL-specific parameters."""
    
    log("🔧 Creating database engines...")
    
    pool_settings = {
        'pool_size': 20,           # Configurable base pool size
        'max_overflow': 30,        # Configurable overflow limit  
        'pool_timeout': 60,        # Configurable timeout
        'pool_recycle': 3600,      # Configurable connection recycle time
        'pool_pre_ping': True,     # Validate connections before use
    }
    
    # Note: Session configuration will be done after connection establishment
    # This is more reliable than connection event listeners
    
    # Detect available MySQL driver and use appropriate connection arguments
    mysql_connect_args = _get_mysql_connect_args()
    log(f"🔧 Using MySQL connection arguments: {list(mysql_connect_args.keys())}")
    
    try:
        log("🔄 Creating source database engine...")
        config.ENGINE_SOURCE = sa.create_engine(
            config.DB_SOURCE_URL,
            connect_args=mysql_connect_args,
            **pool_settings
        )
        
        log("🔄 Creating target database engine...")
        config.ENGINE_TARGET = sa.create_engine(
            config.DB_TARGET_URL,
            connect_args=mysql_connect_args,
            **pool_settings
        )
        
        # Test connections and configure sessions
        log("🔄 Testing source database connection...")
        with config.ENGINE_SOURCE.connect() as conn:
            conn.execute(sa.text("SELECT 1"))
            # Configure session settings after successful connection
            _configure_database_session(conn)
        
        log("🔄 Testing target database connection...")
        with config.ENGINE_TARGET.connect() as conn:
            conn.execute(sa.text("SELECT 1"))
            # Configure session settings after successful connection
            _configure_database_session(conn)
        
        log("✅ Database engines created and tested successfully")
        
    except Exception as e:
        log(f"❌ FAILED: Database engine creation error")
        log(f"   Error: {e}")
        log(f"   Check database connectivity and credentials")
        raise

def cleanup_engines():
    """Clean up engine resources on shutdown."""
    log("🔄 Cleaning up database engines...")
    try:
        if config.ENGINE_SOURCE:
            config.ENGINE_SOURCE.dispose()
        if config.ENGINE_TARGET:
            config.ENGINE_TARGET.dispose()
        log("✅ Database engines cleaned up successfully")
    except Exception as e:
        log(f"❌ FAILED: Engine cleanup error")
        log(f"   Error: {e}")

def get_engines():
    """Get the global database engines."""
    if config.ENGINE_SOURCE is None or config.ENGINE_TARGET is None:
        create_engines()
    
    return config.ENGINE_SOURCE, config.ENGINE_TARGET

def get_configured_connection(engine, purpose="database"):
    """Get a database connection with session configuration applied."""
    try:
        connection = engine.connect()
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