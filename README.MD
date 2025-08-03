# DLT Database Sync Pipeline

A data synchronization pipeline built with [dlt (data load tool)](https://dlthub.com/) that provides incremental and full refresh data synchronization between MySQL databases. The pipeline supports automatic schema synchronization, primary key-based merging, and configurable sync intervals.

## Features

- **Incremental Sync**: Sync data based on modifier columns (e.g., `updated_at`) for efficient data transfer
- **Full Refresh**: Complete table replacement for tables without modifier columns
- **Auto Schema Management**: Automatically adds required `_dlt_load_id` and `_dlt_id` columns to target tables
- **Schema Synchronization**: Syncs new columns from source to target database
- **Primary Key Merging**: Uses primary keys for data deduplication and updates
- **Configurable Intervals**: Set custom sync intervals or run once
- **Advanced Connection Pool Management**: Prevents "QueuePool limit reached" errors with configurable pool settings
- **Connection Pool Monitoring**: Real-time logging of connection usage and pool status
- **Error Recovery**: Automatic retry logic with graceful error handling
- **Health Check**: Built-in HTTP server for monitoring pipeline status
- **Docker Support**: Full containerized deployment with Docker Compose

## Prerequisites

### Direct Installation
- Python 3.8+
- MySQL databases (source and target)
- pip package manager

### Docker Installation
- Docker
- Docker Compose

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
   FETCH_LIMIT=1
   INTERVAL=60  # Sync interval in seconds (0 for single run)
   
   # Connection Pool Configuration (to prevent "QueuePool limit reached" errors)
   POOL_SIZE=20        # Base connection pool size (default: 5)
   MAX_OVERFLOW=30     # Additional connections beyond pool_size (default: 10) 
   POOL_TIMEOUT=60     # Seconds to wait for available connection (default: 30)
   POOL_RECYCLE=3600   # Seconds before recycling connections (default: -1, no recycle)
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

**Incremental Sync Table** (with modifier column):
```json
{
  "table": "table_name",
  "primary_key": "id", 
  "modifier": "updated_at"
}
```

**Full Refresh Table** (without modifier):
```json
{
  "table": "table_name",
  "primary_key": "id"
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

1. **Schema Sync**: Ensures target tables have the same structure as source tables
2. **Column Addition**: Automatically adds `_dlt_load_id` and `_dlt_id` tracking columns
3. **Incremental Processing**: For tables with modifier columns, only syncs records newer than the last sync
4. **Full Refresh**: For tables without modifiers, replaces all data
5. **Primary Key Merging**: Uses primary keys to handle updates and prevent duplicates

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

## Important Notes

- **No Column Deletion**: The pipeline never deletes columns during sync operations
- **Modifier Requirement**: Incremental sync requires a modifier column (e.g., `updated_at`) that's always updated
- **Initial Sync**: First run will sync all data; subsequent runs sync only changes
- **Schema Evolution**: New columns in source tables are automatically added to target tables

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

**For troubleshooting:** Enable debug mode for detailed logging:
```bash
# Add to your .env file:
DEBUG_MODE=true
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

## TODO

- ✅ ~~Enhanced logging output~~ (Completed: Added connection pool monitoring)
- ✅ ~~Connection pool management~~ (Completed: Added configurable pool settings)
- ✅ ~~Error handling improvements~~ (Completed: Added retry logic and graceful errors)
- Configurable initial values for incremental sync
- Support for custom sync strategies
- Web-based monitoring dashboard
- Batch processing for large tables
- Metrics and alerting integration
- Support for additional database types (PostgreSQL, Oracle)