%run /Workspace/ETL/config
%run /Workspace/ETL/Utils/delta_utils
import sqlite3
import pandas as pd
from datetime import datetime, timedelta
import random

from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, lit, col, to_timestamp
from pyspark.sql.types import *

# -----------------------------
# Create fake DB
# -----------------------------
def setup_legacy_db():
    db_path = "/tmp/customers.db"
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS customers (
            customer_id INTEGER PRIMARY KEY,
            first_name TEXT,
            last_name TEXT,
            email TEXT,
            phone TEXT,
            city TEXT,
            country TEXT,
            signup_date TEXT,
            updated_at TEXT,
            is_active INTEGER
        )
    """)

    cursor.execute("SELECT COUNT(*) FROM customers")
    count = cursor.fetchone()[0]

    if count == 0:
        data = [
            (1,"Rahul","Sharma","rahul@gmail.com","9999999999","Delhi","IN","2024-01-01","2024-01-10",1),
            (2,"Ankit","Verma","ankit@gmail.com","8888888888","Mumbai","IN","2024-01-02","2024-01-11",1),
            (3,"John","Doe","john@gmail.com","7777777777","NY","US","2024-01-03","2024-01-12",1),
        ]

        cursor.executemany("INSERT INTO customers VALUES (?,?,?,?,?,?,?,?,?,?)", data)
        conn.commit()

    conn.close()


# -----------------------------
# Fetch DB data
# -----------------------------
def fetch_data():
    conn = sqlite3.connect("/tmp/customers.db")
    df = pd.read_sql_query("SELECT * FROM customers", conn)
    conn.close()
    return df


# -----------------------------
# MAIN INGEST
# -----------------------------
def ingest_customers(spark):
    table_name = "customers"
    full_table = f"{BRONZE_SCHEMA}.bronze_{table_name}"

    print("[BRONZE] Ingesting customers...")

    create_schema_if_not_exists(spark, BRONZE_SCHEMA)

    setup_legacy_db()

    pdf = fetch_data()

    df = spark.createDataFrame(pdf)

    df = df.withColumn("signup_date", to_timestamp(col("signup_date"))) \
           .withColumn("updated_at", to_timestamp(col("updated_at"))) \
           .withColumn("_ingest_ts", current_timestamp()) \
           .withColumn("_source", lit("sqlite_db"))

    df.write.format("delta") \
        .mode("overwrite") \
        .saveAsTable(full_table)

    print("[BRONZE] ✅ customers ingestion complete")
  spark = SparkSession.builder.getOrCreate()

ingest_customers(spark)
