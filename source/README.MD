Step to Duplicate and sync
1. Restore / preload table structure and data to target database
2. for each table, add column 
  `_dlt_load_id` text NOT NULL,
  `_dlt_id` varchar(128) NOT NULL
3. update .env
4. .env TABLES format explained
5. docker compose build
6. docker compose up -d


feature
- sync data by primary key and modifier data
- incremental sync, when sync new table will auto create for column _dlt_load_id and _dlt_id
- auto load script per interval time


Condition
- do not delete any column when sync happen
- incremental sync can only be succeed if theres modifier data (eg. updated_at) that updated always updated

TODO:
- output log
- set initial value for incremental sync, so it can sync from initial_value > updated_at