%run /Workspace/ETL/config
%run /Workspace/ETL/Utils/delta_utils
%run /Workspace/ETL/Utils/watermark
import requests
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, lit, col, to_timestamp
from pyspark.sql.types import *

def fetch_from_api(endpoint_key: str) -> list:
    url = "https://jsonplaceholder.typicode.com/posts"

    print(f"[API] Fetching from: {url}")

    response = requests.get(url)

    data = response.json()

    print(f"[API] Fetched {len(data)} records")

    return data

def parse_orders(raw_data: list, spark: SparkSession):
    records = []
    for item in raw_data:
        records.append({
            "order_id": item.get("id"),
            "product_id": item.get("id"),
            "product_name": item.get("title", ""),
            "category": item.get("category", ""),
            "price": float(item.get("price", 0.0)),
            "rating_rate": float(item.get("rating", {}).get("rate", 0.0)),
            "rating_count": int(item.get("rating", {}).get("count", 0)),
            "order_date": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "source": "rest_api"
        })

    schema = StructType([
        StructField("order_id", IntegerType(), True),
        StructField("product_id", IntegerType(), True),
        StructField("product_name", StringType(), True),
        StructField("category", StringType(), True),
        StructField("price", DoubleType(), True),
        StructField("rating_rate", DoubleType(), True),
        StructField("rating_count", IntegerType(), True),
        StructField("order_date", StringType(), True),
        StructField("source", StringType(), True),
    ])

    return spark.createDataFrame(records, schema)

def ingest_orders(spark: SparkSession):
    table_name = "orders"
    table_path = f"{BRONZE_PATH}/{table_name}"
    full_table = f"{BRONZE_SCHEMA}.bronze_{table_name}"

    print(f"[BRONZE] Ingesting orders...")

    create_schema_if_not_exists(spark, BRONZE_SCHEMA)

    raw_data = fetch_from_api("orders")
    df = parse_orders(raw_data, spark)

    df = df.withColumn("order_date", to_timestamp(col("order_date")))

    df = df.withColumn("_ingest_ts", current_timestamp()) \
           .withColumn("_source", lit("api"))

    df.write.format("delta") \
    .mode("overwrite") \
    .saveAsTable(full_table)

    print("[BRONZE] ✅ orders ingestion complete")

spark = SparkSession.builder.getOrCreate()

ingest_orders(spark)
