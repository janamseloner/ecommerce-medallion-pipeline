from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, lit, when, round as spark_round,
    datediff, current_date, current_timestamp,
    year, month, dayofmonth, dayofweek,
    concat_ws, coalesce,
    lag
)
from pyspark.sql.window import Window

ENV           = "dev"
SILVER_SCHEMA = "ecommerce_silver_dev"
PIPELINE_NAME = "ecommerce_medallion_pipeline"

spark = SparkSession.builder.getOrCreate()


def add_audit_columns(df, source_table):
    df = df.withColumn("_transform_ts", current_timestamp())
    df = df.withColumn("_layer",        lit("silver_transformed"))
    df = df.withColumn("_source_table", lit(source_table))
    df = df.withColumn("_pipeline",     lit(PIPELINE_NAME))
    df = df.withColumn("_env",          lit(ENV))
    return df


# =============================================================================
# 1. TRANSFORM ORDERS
# =============================================================================
def transform_orders():
    print("\n=== TRANSFORM ORDERS ===")
    source_table = SILVER_SCHEMA + ".silver_orders"
    target_table = SILVER_SCHEMA + ".silver_orders_transformed"

    df = spark.read.table(source_table)

    # Date columns
    df = df.withColumn("order_year",    year(col("order_date")))
    df = df.withColumn("order_month",   month(col("order_date")))
    df = df.withColumn("order_day",     dayofmonth(col("order_date")))
    df = df.withColumn("order_weekday", dayofweek(col("order_date")))
    df = df.withColumn("is_weekend",
        when(col("order_weekday").isin(1, 7), lit(True)).otherwise(lit(False))
    )

    # Revenue columns
    df = df.withColumn("revenue",          spark_round(col("price"), 2))
    df = df.withColumn("revenue_with_tax", spark_round(col("price") * lit(1.18), 2))
    df = df.withColumn("net_revenue",      spark_round(col("price"), 2))

    # Rank within category by rating
    from pyspark.sql.window import Window
    from pyspark.sql.functions import dense_rank
    window_cat = Window.partitionBy("category").orderBy(col("rating_rate").desc())
    df = df.withColumn("rank_in_category", dense_rank().over(window_cat))

    # Price change vs previous (lag)
    window_order = Window.partitionBy("product_id").orderBy("order_date")
    df = df.withColumn("prev_price", lag("price", 1).over(window_order))
    df = df.withColumn("price_change",
        spark_round(col("price") - coalesce(col("prev_price"), col("price")), 2)
    )

    df = add_audit_columns(df, source_table)

    df.write.mode("overwrite").format("delta").saveAsTable(target_table)
    print("orders_transformed done ✅ — rows:", df.count())


# =============================================================================
# 2. TRANSFORM PRODUCTS
# =============================================================================
def transform_products():
    print("\n=== TRANSFORM PRODUCTS ===")
    source_table = SILVER_SCHEMA + ".silver_products"
    target_table = SILVER_SCHEMA + ".silver_products_transformed"

    df = spark.read.table(source_table)

    # Category grouping
    df = df.withColumn("category_group",
        when(col("category").isin("electronics", "gadgets", "tech"),       lit("Electronics"))
        .when(col("category").isin("footwear", "shoes", "sandals"),        lit("Footwear"))
        .when(col("category").isin("sports", "fitness", "outdoor"),        lit("Sports & Fitness"))
        .when(col("category").isin("kitchen", "cookware", "appliances"),   lit("Kitchen"))
        .otherwise(lit("Other"))
    )

    # Inventory value
    df = df.withColumn("inventory_value",
        spark_round(col("price") * col("stock_qty"), 2)
    )

    # Price tier
    df = df.withColumn("price_tier",
        when(col("price") < 10,   lit("Budget"))
        .when(col("price") < 50,  lit("Mid-Range"))
        .when(col("price") < 100, lit("Premium"))
        .otherwise(lit("Luxury"))
    )

    # Domestic supplier flag
    df = df.withColumn("is_domestic_supplier",
        when(col("country") == lit("IN"), lit(True)).otherwise(lit(False))
    )

    # Rank by inventory value within category
    from pyspark.sql.functions import row_number
    window_inv = Window.partitionBy("category").orderBy(col("inventory_value").desc())
    df = df.withColumn("inventory_rank", row_number().over(window_inv))

    df = add_audit_columns(df, source_table)

    df.write.mode("overwrite").format("delta").saveAsTable(target_table)
    print("products_transformed done ✅ — rows:", df.count())


# =============================================================================
# 3. TRANSFORM CUSTOMERS
# =============================================================================
def transform_customers():
    print("\n=== TRANSFORM CUSTOMERS ===")
    source_table = SILVER_SCHEMA + ".silver_customers"
    target_table = SILVER_SCHEMA + ".silver_customers_transformed"

    df = spark.read.table(source_table)

    # Tenure in days
    df = df.withColumn("tenure_days",
        datediff(current_date(), col("signup_date"))
    )

    # Tenure bucket
    df = df.withColumn("tenure_bucket",
        when(col("tenure_days") < 30,   lit("New"))
        .when(col("tenure_days") < 180, lit("Regular"))
        .when(col("tenure_days") < 365, lit("Loyal"))
        .otherwise(lit("Champion"))
    )

    # Region mapping
    df = df.withColumn("region",
        when(col("country").isin("IN", "SG", "JP", "CN"), lit("Asia"))
        .when(col("country").isin("US", "CA"),             lit("North America"))
        .when(col("country").isin("UK", "DE", "FR", "IT"), lit("Europe"))
        .when(col("country").isin("AU", "NZ"),             lit("Oceania"))
        .otherwise(lit("Other"))
    )

    # Display name using concat_ws (safe for nulls)
    df = df.withColumn("display_name",
        concat_ws(" ", col("first_name"), col("last_name"))
    )

    # Seniority rank within region
    from pyspark.sql.functions import dense_rank
    window_region = Window.partitionBy("region").orderBy(col("tenure_days").desc())
    df = df.withColumn("seniority_rank", dense_rank().over(window_region))

    df = add_audit_columns(df, source_table)

    df.write.mode("overwrite").format("delta").saveAsTable(target_table)
    print("customers_transformed done ✅ — rows:", df.count())


# ── RUN ──────────────────────────────────────────────────────────────────────
print("SILVER TRANSFORM STARTING...")

transform_orders()
transform_products()
transform_customers()

print("\nSILVER TRANSFORM COMPLETE ✅")
