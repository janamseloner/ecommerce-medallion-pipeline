from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, trim, lower, upper, when, lit,
    current_timestamp, to_timestamp, concat
)

spark = SparkSession.builder.getOrCreate()

ENV           = "dev"
BRONZE_SCHEMA = "ecommerce_bronze_dev"
SILVER_SCHEMA = "ecommerce_silver_dev"
PIPELINE_NAME = "ecommerce_medallion_pipeline"


def add_audit_columns(df, layer, source_table):
    df = df.withColumn("_silver_ingest_ts", current_timestamp())
    df = df.withColumn("_layer",            lit(layer))
    df = df.withColumn("_source_table",     lit(source_table))
    df = df.withColumn("_pipeline",         lit(PIPELINE_NAME))
    df = df.withColumn("_env",              lit(ENV))
    return df


def print_dq_report(df, table_name):
    print("DQ REPORT:", table_name)
    print("  Total rows:", df.count())
    print("  Columns   :", len(df.columns))
    return df


def clean_orders():
    print("\n=== CLEANING ORDERS ===")
    df = spark.read.table(BRONZE_SCHEMA + ".bronze_orders")
    df = df.dropDuplicates(["order_id"])
    df = df.dropna(subset=["order_id", "product_id", "price"])
    df = df.withColumn("product_name", trim(col("product_name")))
    df = df.withColumn("category",     lower(trim(col("category"))))
    df = df.withColumn("source",       lower(trim(col("source"))))
    df = df.filter(col("price") > 0)
    df = df.withColumn("price_bucket",
        when(col("price") < 20,  lit("low"))
        .when(col("price") < 50, lit("medium"))
        .otherwise(lit("high"))
    )
    df = df.withColumn("is_highly_rated",
        when(col("rating_rate") >= 4.0, lit(True)).otherwise(lit(False))
    )
    df = add_audit_columns(df, "silver", "orders")
    print_dq_report(df, "silver_orders")
    spark.sql("CREATE DATABASE IF NOT EXISTS " + SILVER_SCHEMA)
    df.write.mode("overwrite").format("delta").saveAsTable(SILVER_SCHEMA + ".silver_orders")
    print("orders done ✅")


def clean_products():
    print("\n=== CLEANING PRODUCTS ===")
    df = spark.read.table(BRONZE_SCHEMA + ".bronze_products")
    df = df.dropDuplicates(["product_id"])
    df = df.dropna(subset=["product_id", "product_name", "price"])
    df = df.withColumn("product_name", trim(col("product_name")))
    df = df.withColumn("category",     lower(trim(col("category"))))
    df = df.withColumn("country",      upper(trim(col("country"))))
    df = df.withColumn("supplier_id",  upper(trim(col("supplier_id"))))
    df = df.withColumn("stock_qty",
        when(col("stock_qty") < 0, lit(0)).otherwise(col("stock_qty"))
    )
    df = df.filter(col("price") > 0)
    df = df.withColumn("stock_status",
        when(col("stock_qty") == 0,  lit("out_of_stock"))
        .when(col("stock_qty") < 20, lit("low_stock"))
        .otherwise(lit("in_stock"))
    )
    df = df.withColumn("price_with_tax", col("price") * lit(1.18))
    df = add_audit_columns(df, "silver", "products")
    print_dq_report(df, "silver_products")
    spark.sql("CREATE DATABASE IF NOT EXISTS " + SILVER_SCHEMA)
    df.write.mode("overwrite").format("delta").saveAsTable(SILVER_SCHEMA + ".silver_products")
    print("products done ✅")


def clean_customers():
    print("\n=== CLEANING CUSTOMERS ===")
    df = spark.read.table(BRONZE_SCHEMA + ".bronze_customers")
    df = df.dropDuplicates(["customer_id"])
    df = df.dropna(subset=["customer_id", "email"])
    df = df.withColumn("first_name", trim(col("first_name")))
    df = df.withColumn("last_name",  trim(col("last_name")))
    df = df.withColumn("email",      lower(trim(col("email"))))
    df = df.withColumn("city",       trim(col("city")))
    df = df.withColumn("country",    upper(trim(col("country"))))
    df = df.withColumn("is_valid_email",
        when(col("email").rlike("^[^@]+@[^@]+\\.[^@]+$"), lit(True))
        .otherwise(lit(False))
    )
    df = df.withColumn("full_name",
        concat(col("first_name"), lit(" "), col("last_name"))
    )
    df = df.withColumn("is_active",
        when(col("is_active") == 1, lit(True)).otherwise(lit(False))
    )
    df = df.withColumn("signup_date", to_timestamp(col("signup_date")))
    df = df.withColumn("updated_at",  to_timestamp(col("updated_at")))
    df = add_audit_columns(df, "silver", "customers")
    print_dq_report(df, "silver_customers")
    spark.sql("CREATE DATABASE IF NOT EXISTS " + SILVER_SCHEMA)
    df.write.mode("overwrite").format("delta").saveAsTable(SILVER_SCHEMA + ".silver_customers")
    print("customers done ✅")


print("SILVER LAYER STARTING...")
clean_orders()
clean_products()
clean_customers()
print("\nSILVER COMPLETE ✅")
