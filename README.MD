# DLT Database Sync Pipeline

A robust, enterprise-grade data synchronization pipeline built with [dlt (data load tool)](https://dlthub.com/) that provides incremental and full refresh data synchronization between MySQL databases. The pipeline features a modular architecture with advanced error handling, connection pool management, file-based staging for large tables, and comprehensive monitoring capabilities.

**Latest Version**: v2.1 - Enhanced with Connection Pool Recovery and Data Validation
**Architecture**: Modular Design (97% code reduction from monolithic structure)
**Framework**: DLT 1.15.0+ with SQLAlchemy and PyMySQL

## Features

### Core Synchronization
- **Incremental Sync**: Sync data based on modifier columns (e.g., `updated_at`) for efficient data transfer
- **Full Refresh**: Complete table replacement for tables without modifier columns
- **File-Based Staging**: Alternative processing mode for large tables to avoid database conflicts
- **Hybrid Processing**: Combines DLT's extraction strengths with custom loading optimization

### Schema & Data Management
- **Auto Schema Management**: Automatically adds required `_dlt_load_id` and `_dlt_id` columns to target tables
- **Schema Synchronization**: Syncs new columns from source to target database
- **Primary Key Merging**: Uses primary keys for data deduplication and updates
- **Composite Primary Keys**: Support for multiple column primary keys (e.g., `order_id + product_id`)
- **Data Validation**: Pre and post-sync row count verification with detailed reporting
- **Data Sanitization**: Automatic handling of problematic data types and values

### Performance & Reliability
- **Advanced Connection Pool Management**: Prevents "QueuePool limit reached" errors with configurable pool settings
- **Connection Pool Monitoring**: Real-time logging of connection usage and pool status
- **Connection Pool Recovery**: Automatic detection and recovery from corrupted connection pools
- **Batch Processing**: Configurable batch sizes for optimal performance with large datasets
- **Memory Optimization**: Efficient memory usage through chunked processing
- **Transaction Management**: Optimized transaction handling with semaphore control

### Error Handling & Recovery
- **Multi-layered Error Recovery**: Connection errors, lock timeouts, and JSON serialization issues
- **Automatic Retry Logic**: Exponential backoff with configurable retry parameters
- **Graceful Degradation**: Continues processing other tables when individual tables fail
- **State Corruption Recovery**: Automatic cleanup of corrupted DLT state
- **Timezone Handling**: Robust timezone-aware timestamp comparisons

### Monitoring & Observability
- **Health Check**: Built-in HTTP server for monitoring pipeline status
- **Comprehensive Logging**: Structured logging with configurable verbosity levels
- **Performance Metrics**: Real-time sync performance and connection monitoring
- **Debug Mode**: Enhanced debugging capabilities with detailed analysis
- **Sync Verification**: Automatic detection of sync failures and data discrepancies

### Deployment & Configuration
- **Docker Support**: Full containerized deployment with Docker Compose
- **Environment-Based Config**: Flexible configuration via `.env` files
- **Modular Architecture**: Clean separation of concerns with 9 specialized modules
- **CI/CD Integration**: GitLab CI support for automated deployments
- **Multi-Environment**: Support for staging and production deployments

## Prerequisites

### System Requirements
- **Python**: 3.8+ (3.11 recommended for Docker)
- **Memory**: Minimum 512MB, Recommended 2GB for large tables
- **CPU**: 2+ cores for concurrent processing
- **Disk**: 1GB+ for logs, staging files, and temporary data
- **Network**: Stable connection to source/target databases

### Database Requirements
- **Source Database**: MySQL 5.7+, MySQL 8.0+, MariaDB 10.2+
- **Target Database**: MySQL 5.7+, MySQL 8.0+, MariaDB 10.2+
- **Permissions**: SELECT on source, SELECT/INSERT/UPDATE/ALTER on target
- **InnoDB Engine**: Required for transaction support
- **Binary Logging**: Enabled for audit trail (recommended)

### Direct Installation
- Python 3.8+ with pip package manager
- MySQL databases (source and target) with proper permissions

### Docker Installation
- Docker Engine 20.10+
- Docker Compose 2.0+
- Sufficient resources for containerized deployment

## Installation & Setup

### Option 1: Direct Installation

1. **Clone the repository**
   ```bash
   git clone <repository-url>
   cd dlt
   ```

2. **Install dependencies**
   ```bash
   pip install -r requirements.txt
   ```

3. **Configure environment variables**
   Create a `.env` file in the project root:
   ```env
   # Source Database Configuration
   SOURCE_DB_USER=symuser
   SOURCE_DB_PASS=sympass
   SOURCE_DB_HOST=127.0.0.1
   SOURCE_DB_PORT=3306
   SOURCE_DB_NAME=dbzains

   # Target Database Configuration
   TARGET_DB_USER=symuser
   TARGET_DB_PASS=sympass
   TARGET_DB_HOST=127.0.0.1
   TARGET_DB_PORT=3307
   TARGET_DB_NAME=dbzains

   # Pipeline Configuration
   INTERVAL=60                    # Sync interval in seconds (0 for single run)
   BATCH_SIZE=8                   # Tables per batch (default: 8)
   BATCH_DELAY=2                  # Delay between batches in seconds

   # Processing Mode Configuration
   PIPELINE_MODE=direct           # 'direct' (db-to-db) or 'file_staging' (for large tables)

   # Connection Pool Configuration (MariaDB-optimized)
   POOL_SIZE=15                   # Base connection pool size (reduced for MariaDB stability)
   MAX_OVERFLOW=20                # Additional connections beyond pool_size
   POOL_TIMEOUT=30                # Seconds to wait for available connection (faster for MariaDB)
   POOL_RECYCLE=1800              # Seconds before recycling connections (more frequent for MariaDB)
   POOL_PRE_PING=true             # Enable connection validation

   # Debug and Data Handling
   DEBUG_MODE=true                # Enable detailed logging
   DEEP_DEBUG_JSON=true           # Enable JSON error analysis
   AUTO_SANITIZE_DATA=true        # Automatic data sanitization
   PRESERVE_COLUMN_NAMES=true     # Preserve original column names

   # Error Handling Configuration
   LOCK_TIMEOUT_RETRIES=5         # Max retries for lock timeouts
   LOCK_TIMEOUT_BASE_DELAY=10     # Base delay for exponential backoff
   LOCK_TIMEOUT_MAX_DELAY=300     # Maximum delay cap
   CONNECTION_LOSS_RETRIES=3      # Max retries for connection loss

   # File Staging Configuration (when PIPELINE_MODE=file_staging)
   FILE_STAGING_DIR=staging       # Base directory for staging files
   FILE_STAGING_RETENTION_HOURS=24 # How long to keep staging files

   # Index Management Configuration
   ENABLE_INDEX_OPTIMIZATION=true      # Enable automatic index creation
   INDEX_OPTIMIZATION_TIMEOUT=45       # Timeout for staging table index creation
   CLEANUP_TEMPORARY_INDEXES=true      # Clean up temporary indexes after operations

   # DLT Load Configuration
   TRUNCATE_STAGING_DATASET=true       # Enable DLT staging management

   # HTTP Server Configuration
   HTTP_SERVER_PORT=8089               # Port for health check server
   HTTP_SERVER_TIMEOUT=30              # Request timeout in seconds
   HTTP_SERVER_MAX_REQUESTS=1000       # Max requests before restart
   ```

4. **Configure tables**
   Create a `tables.json` file to specify which tables to sync:
   ```json
   [
     {
       "table": "users",
       "primary_key": "id",
       "modifier": "updated_at"
     },
     {
       "table": "products",
       "primary_key": "product_id"
     }
   ]
   ```

5. **Prepare target database**
   - Restore/preload table structure and data to target database
   - The pipeline will automatically add required columns:
     - `_dlt_load_id` text NOT NULL
     - `_dlt_id` varchar(128) NOT NULL

6. **Run the pipeline**
   ```bash
   python source/db_pipeline.py
   ```

### Option 2: Docker Compose Deployment

1. **Clone the repository**
   ```bash
   git clone <repository-url>
   cd dlt
   ```

2. **Build and start services**
   ```bash
   docker compose build
   docker compose up -d
   ```

3. **Services included:**
   - **Source MySQL** (port 3307): Source database
   - **Target MySQL** (port 3308): Target database  
   - **DLT Runner** (port 8089): Pipeline executor with health check

4. **Monitor logs**
   ```bash
   docker compose logs -f dlt_runner
   ```

## Configuration

### Table Configuration Format

The `tables.json` file supports two types of table configurations:

**Incremental Sync Table** (with modifier column):**
```json
{
  "table": "table_name",
  "primary_key": "id", 
  "modifier": "updated_at"
}
```

**Full Refresh Table** (without modifier):**
```json
{
  "table": "table_name",
  "primary_key": "id"
}
```

**Multiple Primary Keys (Composite Keys):**
```json
{
  "table": "order_items",
  "primary_key": ["order_id", "product_id"],
  "modifier": "updated_at"
}
```

**Complex Composite Keys:**
```json
{
  "table": "user_permissions",
  "primary_key": ["user_id", "permission_id", "resource_id"]
}
```

### Environment Variables

#### Database Configuration
| Variable | Description | Default |
|----------|-------------|---------|
| `SOURCE_DB_*` | Source database connection details | - |
| `TARGET_DB_*` | Target database connection details | - |

#### Pipeline Configuration
| Variable | Description | Default |
|----------|-------------|---------|
| `FETCH_LIMIT` | Number of records to fetch per batch | 1 |
| `INTERVAL` | Sync interval in seconds (0 = single run) | 60 |

#### Connection Pool Configuration
| Variable | Description | Default |
|----------|-------------|---------|
| `POOL_SIZE` | Base connection pool size | 20 |
| `MAX_OVERFLOW` | Additional connections beyond pool_size | 30 |
| `POOL_TIMEOUT` | Seconds to wait for available connection | 60 |
| `POOL_RECYCLE` | Seconds before recycling connections | 3600 |

**Total Available Connections**: `POOL_SIZE + MAX_OVERFLOW` (default: 50)

## How It Works

### Architecture Overview
The pipeline uses a modular architecture with 9 specialized modules that work together to provide robust data synchronization:

1. **Main Entry Point** (`db_pipeline.py`): Orchestrates the entire sync process
2. **Configuration Module** (`config.py`): Manages environment variables and table configurations
3. **Database Module** (`database.py`): Handles connection pools and transaction management
4. **Pipeline Management** (`pipeline_management.py`): Orchestrates DLT operations and table processing
5. **Schema Management** (`schema_management.py`): Handles schema synchronization
6. **Data Processing** (`data_processing.py`): Manages data validation and sanitization
7. **Error Handling** (`error_handling.py`): Provides retry mechanisms and recovery
8. **Monitoring** (`monitoring.py`): Health checks and performance monitoring
9. **Utilities** (`utils.py`): Common helper functions and logging

### Processing Modes

#### Direct Mode (PIPELINE_MODE=direct)
1. **Schema Sync**: Ensures target tables have the same structure as source tables
2. **Column Addition**: Automatically adds `_dlt_load_id` and `_dlt_id` tracking columns
3. **Incremental Processing**: For tables with modifier columns, only syncs records newer than the last sync
4. **Full Refresh**: For tables without modifiers, replaces all data
5. **Primary Key Merging**: Uses primary keys to handle updates and prevent duplicates
6. **Data Validation**: Pre and post-sync row count verification

#### File Staging Mode (PIPELINE_MODE=file_staging)
1. **DLT Extraction**: Uses DLT to extract incremental data to Parquet files
2. **File Processing**: Reads files in memory-efficient chunks
3. **Custom Loading**: Uses optimized MySQL upsert operations to load data
4. **State Management**: DLT manages incremental state automatically
5. **Large Table Support**: Handles tables of any size without timeout issues

### Advanced Features
- **Composite Primary Keys**: Support for multiple column primary keys
- **Connection Pool Recovery**: Automatic detection and recovery from pool corruption
- **Timezone Handling**: Robust timezone-aware timestamp comparisons
- **Batch Processing**: Configurable batch sizes for optimal performance
- **Error Recovery**: Multi-layered error handling with automatic retry

## Health Check

The pipeline includes an HTTP health check server accessible at:
- Direct installation: `http://localhost:8089`
- Docker deployment: `http://localhost:8089`

Returns "We are Groot!" when the service is running.

## Connection Pool Management

The pipeline includes enhanced connection pool management to prevent "QueuePool limit reached" errors:

### Monitoring Connection Usage
The pipeline logs connection pool status during execution:
```
Source pool status - Size: 20, Checked out: 5
Target pool status - Size: 20, Checked out: 3
```

### Connection Pool Optimization
- **Pool Size**: Base number of persistent connections (default: 20)
- **Max Overflow**: Additional temporary connections (default: 30)
- **Pool Timeout**: Wait time for available connections (default: 60s)
- **Pool Recycle**: Connection refresh interval (default: 3600s)

### Troubleshooting Connection Issues
If you encounter connection pool errors:

1. **"QueuePool limit reached"**: Increase `POOL_SIZE` and `MAX_OVERFLOW`
2. **"Connection timeout"**: Increase `POOL_TIMEOUT`
3. **Slow performance**: Check `POOL_RECYCLE` and database network latency
4. **For large tables**: Consider processing them separately during off-peak hours

## Multiple Primary Key Support

The pipeline now supports **composite primary keys** (multiple columns that together form a unique identifier) in addition to single primary keys.

### Benefits of Multiple Primary Keys
- **Complex Relationships**: Handle tables with natural composite keys (e.g., order_id + product_id)
- **Data Integrity**: Ensure proper deduplication across multiple identifying columns
- **Flexibility**: Support various database design patterns and normalization levels

### Configuration Examples

**Simple Composite Key:**
```json
{
  "table": "order_items",
  "primary_key": ["order_id", "product_id"],
  "modifier": "updated_at"
}
```

**Complex Composite Key:**
```json
{
  "table": "user_permissions",
  "primary_key": ["user_id", "permission_id", "resource_id", "tenant_id"]
}
```

**Mixed Configuration:**
```json
[
  {
    "table": "users",
    "primary_key": "id",
    "modifier": "updated_at"
  },
  {
    "table": "order_items",
    "primary_key": ["order_id", "product_id"],
    "modifier": "modified_at"
  }
]
```

### How It Works
1. **Single Keys**: Passed directly to DLT as `primary_key="id"`
2. **Composite Keys**: Automatically formatted as `primary_key="order_id, product_id"`
3. **Validation**: Configuration is validated at startup to ensure proper format
4. **Logging**: Clear indication of which tables use single vs. composite keys

## Important Notes

- **No Column Deletion**: The pipeline never deletes columns during sync operations
- **Modifier Requirement**: Incremental sync requires a modifier column (e.g., `updated_at`) that's always updated
- **Initial Sync**: First run will sync all data; subsequent runs sync only changes
- **Schema Evolution**: New columns in source tables are automatically added to target tables
- **Primary Key Flexibility**: Support for both single columns and arrays of columns as primary keys

## Logging System

The pipeline now uses a clean, organized logging system that shows only essential information by default:

- **Configuration**: Database setup, table configuration, pipeline initialization
- **Phase Status**: Current processing phase, batch progress, table processing
- **Status Updates**: Completion results, success/failure counts, final summaries
- **Error Information**: All errors and failures (always visible)
- **Debug Information**: Detailed operations, performance metrics (only when `DEBUG_MODE=true`)

### Logging Configuration

```bash
# Clean production logs (default)
DEBUG_MODE=false

# Detailed debug logs
DEBUG_MODE=true
```

See `LOGGING.md` for complete logging documentation.

## Troubleshooting

### Common Issues

#### JSON Parsing Errors (orjson.JSONDecodeError)

If you encounter `orjson.JSONDecodeError: unexpected character: line 1 column 1 (char 0)` errors:

**Cause:** This typically happens when switching MySQL modes (strict/unrestricted) or with data encoding issues.

**Solution:** The app now automatically handles both strict and unrestricted MySQL modes. When JSON errors occur:
- The pipeline automatically retries failed tables individually  
- Uses the same sync method (incremental/full refresh) as originally configured
- Continues processing other tables without stopping
- Provides detailed error logs to identify problematic tables

**No manual configuration needed** - the app adapts automatically to your MySQL settings.

**For troubleshooting:** Debug mode can be enabled for detailed logging:
```bash
# Clean production logs (default)
DEBUG_MODE=false             # General detailed logging (default: false)

# Detailed debug logs for troubleshooting
DEBUG_MODE=true              # Enable detailed logging
DEEP_DEBUG_JSON=true         # Deep analysis of JSON errors
AUTO_SANITIZE_DATA=true      # Automatic data sanitization
```

**Enhanced JSON Error Debugging:** With debugging enabled by default, you'll automatically see:
- **Row-by-row analysis** - Identifies exactly which row causes JSON errors
- **Column-by-column analysis** - Shows specific problematic values and data types
- **Full error stack traces** - Pinpoints where in the pipeline JSON parsing fails
- **Automatic data sanitization** - Attempts to fix common data issues automatically

**Automatic Data Sanitization:** When JSON errors occur, the pipeline automatically:
- ✅ **Converts Decimal objects** to float (MySQL DECIMAL columns → JSON compatible numbers)
- ✅ **Removes NULL bytes** (`\x00`) from string fields
- ✅ **Converts invalid dates** (`0000-00-00`) to NULL
- ✅ **Handles NaN/Infinity** values in numeric fields  
- ✅ **Truncates very large strings** (>1MB) to prevent memory issues
- ✅ **Sanitizes problematic characters** that break JSON encoding

**Example Debug Output:**
```
*** IMMEDIATE JSON ERROR DETECTED FOR corez_akad_qurban ***
IMMEDIATE ANALYSIS - Found problematic data:
  Problem: transaksi = Decimal('4000000.00')
  Error: Type is not JSON serializable: decimal.Decimal

=== COMPREHENSIVE JSON ERROR ANALYSIS FOR TABLE: corez_akad_qurban ===
*** FOUND PROBLEMATIC DATA IN corez_akad_qurban ***
Problem 1: Column 'transaksi' = Decimal('4000000.00')
  Data type: Decimal
  JSON Error: Type is not JSON serializable: decimal.Decimal
  Raw bytes: "Decimal('4000000.00')"

DEBUG: Converting Decimal to float in column transaksi: 4000000.00
Individual sync with sanitization successful for corez_akad_qurban
```

This will show detailed information about table processing and help identify specific data issues.

#### Zero Dates Handling

**Zero dates** (like `'0000-00-00 00:00:00'`) are now **automatically synced** from source to destination:

✅ **Source with zero dates**: Data syncs successfully  
✅ **Preserves original data**: Zero dates are copied as-is  
✅ **Destination handles it**: Target database applies its own rules  

The pipeline uses a permissive SQL mode that allows zero dates to pass through, letting each database handle them according to its own configuration.

#### Connection Pool Exhaustion

1. **"QueuePool limit of size X overflow Y reached"**:
   ```env
   # Increase pool limits in .env
   POOL_SIZE=30
   MAX_OVERFLOW=50
   POOL_TIMEOUT=90
   ```

2. **"Connection timeout"**: 
   - Increase `POOL_TIMEOUT` value
   - Check database server performance and network latency
   - Consider reducing `INTERVAL` for frequent smaller syncs

3. **"Connection refused"**: 
   - Verify database credentials in `.env`
   - Check database server status and network connectivity
   - Ensure database ports are correctly configured

4. **Memory Issues with Large Tables**:
   - Process large tables during off-peak hours
   - Consider splitting large tables into smaller batches
   - Increase system memory or use streaming processing

## Project Status

### ✅ Completed Features (v2.1)
- **Modular Architecture**: 97% code reduction from monolithic structure (3,774 → 161 lines main file)
- **Enhanced Logging**: Clean, organized logging system with configurable levels
- **Connection Pool Management**: Advanced pool settings with monitoring and recovery
- **Error Handling**: Multi-layered retry logic with graceful error recovery
- **File-Based Staging**: Alternative processing for large tables to avoid timeouts
- **Composite Primary Keys**: Support for multiple column primary keys
- **Data Validation**: Pre and post-sync row count verification
- **Connection Pool Recovery**: Automatic detection and recovery from corruption
- **Timezone Handling**: Robust timezone-aware timestamp comparisons
- **Batch Processing**: Configurable batch sizes for optimal performance
- **Health Monitoring**: Comprehensive HTTP health check endpoints

### 🔄 In Development
- Web-based monitoring dashboard
- Metrics and alerting integration
- Configurable initial values for incremental sync
- Advanced conflict resolution strategies

### 📋 Future Roadmap
- Support for additional database types (PostgreSQL, Oracle, SQL Server)
- Real-time streaming sync (CDC)
- Enterprise security features
- Cloud-native deployment options
- Machine learning-based optimization