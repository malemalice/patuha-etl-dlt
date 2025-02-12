import mysql.connector
import dlt
from dlt.destinations import sqlalchemy
from datetime import datetime

# Define source and destination
source_db = mysql.connector.connect(
    host="dlt_mysql_source",
    user="root",
    password="rootpass",
    database="dbzains",
    port=3306,
    ssl_disabled=True
)


# Define pipeline
pipeline = dlt.pipeline(
    pipeline_name="mysql_replication",
    destination=sqlalchemy("mysql+pymysql://root:rootpass@dlt_mysql_target:3306/dbzains")
)

# Sync tables
tables = ["users"]
# tables = ["users", "orders", "transactions"]  # Change based on your tables

print("Replication Start!")

cursor = source_db.cursor(dictionary=True)
for table in tables:
    cursor.execute(f"SELECT * FROM {table}")
    data = cursor.fetchall()

    # Convert datetime fields to MySQL-compatible format
    for row in data:
        for key, value in row.items():
            if isinstance(value, datetime):
                row[key] = value.strftime('%Y-%m-%d %H:%M:%S')
  # Remove timezone

    pipeline.run(data, table_name=table)

print("Replication completed!")
