# flake8: noqa
import humanize
from typing import Any
import os

import dlt
from dlt.common import pendulum
from dlt.sources.credentials import ConnectionStringCredentials

from dlt.sources.sql_database import sql_database, sql_table, Table

from sqlalchemy.sql.sqltypes import TypeEngine
import sqlalchemy as sa

def load_tables():

    # Create a dlt source that will load tables
    source = sql_database().with_resources("users", "products")
    
     # Apply incremental loading per table
    source.users.apply_hints(incremental=dlt.sources.incremental("inserted_at"))  # Use `updated_at` for incremental sync
    source.products.apply_hints(incremental=dlt.sources.incremental("updated_at"))


    # Create a dlt pipeline object
    pipeline = dlt.pipeline(
        pipeline_name="mysql_to_mysql", # Custom name for the pipeline
        destination="sqlalchemy", # dlt destination to which the data will be loaded
        dataset_name="dbzains_dlt", # Custom name for the dataset created in the destination
        # dev_mode=True  # # Set to True if you want to replace tables andPrevents metadata caching issues
    )

    # Run the pipeline
    load_info = pipeline.run(source,write_disposition="merge")

    # Pretty print load information
    print(load_info)

if __name__ == '__main__':
    load_tables()
