%run /Workspace/ETL/config
%run /Workspace/ETL/Utils/delta_utils
from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, lit

def create_sample_df(spark):
    data = [
        (1, "Wireless Mouse", "Electronics", 29.99, 150, "SUP001", "IN", "2024-01-15"),
        (2, "USB-C Hub", "Electronics", 49.99, 80, "SUP001", "IN", "2024-01-16"),
        (3, "Running Shoes", "Footwear", 89.99, 200, "SUP002", "CN", "2024-01-17"),
        (4, "Yoga Mat", "Sports", 25.00, 300, "SUP003", "US", "2024-01-18"),
        (5, "Coffee Maker", "Kitchen", 75.50, 60, "SUP004", "DE", "2024-01-19")
    ]

    columns = [
        "product_id","product_name","category","price",
        "stock_qty","supplier_id","country","updated_at"
    ]

    return spark.createDataFrame(data, columns)


def ingest_products(spark):
    table_name = "products"
    full_table = f"{BRONZE_SCHEMA}.bronze_{table_name}"

    print("[BRONZE] Ingesting products...")

    create_schema_if_not_exists(spark, BRONZE_SCHEMA)

    df = create_sample_df(spark)

    df = df.withColumn("_ingest_ts", current_timestamp()) \
           .withColumn("_source", lit("csv_mock"))

    df.write.format("delta") \
        .mode("overwrite") \
        .saveAsTable(full_table)

    print("[BRONZE] ✅ products ingestion complete")
  spark = SparkSession.builder.getOrCreate()

ingest_products(spark)
