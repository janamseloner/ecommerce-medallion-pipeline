from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, lit, when, round as spark_round,
    current_timestamp, monotonically_increasing_id,
    year, month, dayofmonth, dayofweek,
    sum as spark_sum, avg, count, max as spark_max, min as spark_min,
    countDistinct
)

ENV           = "dev"
SILVER_SCHEMA = "ecommerce_silver_dev"
GOLD_SCHEMA   = "ecommerce_gold_dev"
PIPELINE_NAME = "ecommerce_medallion_pipeline"

spark = SparkSession.builder.getOrCreate()


def add_audit_columns(df, source_table):
    df = df.withColumn("_gold_ingest_ts", current_timestamp())
    df = df.withColumn("_layer",          lit("gold"))
    df = df.withColumn("_source_table",   lit(source_table))
    df = df.withColumn("_pipeline",       lit(PIPELINE_NAME))
    df = df.withColumn("_env",            lit(ENV))
    return df


# =============================================================================
# DIMENSION TABLE 1 — dim_product
# =============================================================================
def build_dim_product():
    print("\n=== BUILDING dim_product ===")
    source = SILVER_SCHEMA + ".silver_products_transformed"
    target = GOLD_SCHEMA   + ".dim_product"

    df = spark.read.table(source)

    dim = df.select(
        col("product_id"),
        col("product_name"),
        col("category"),
        col("category_group"),
        col("price"),
        col("price_with_tax"),
        col("price_tier"),
        col("stock_qty"),
        col("stock_status"),
        col("inventory_value"),
        col("supplier_id"),
        col("country").alias("supplier_country"),
        col("is_domestic_supplier")
    ).dropDuplicates(["product_id"])

    dim = add_audit_columns(dim, source)

    spark.sql("CREATE DATABASE IF NOT EXISTS " + GOLD_SCHEMA)
    dim.write.mode("overwrite").format("delta").saveAsTable(target)
    print("dim_product done ✅ — rows:", dim.count())


# =============================================================================
# DIMENSION TABLE 2 — dim_customer
# =============================================================================
def build_dim_customer():
    print("\n=== BUILDING dim_customer ===")
    source = SILVER_SCHEMA + ".silver_customers_transformed"
    target = GOLD_SCHEMA   + ".dim_customer"

    df = spark.read.table(source)

    dim = df.select(
        col("customer_id"),
        col("first_name"),
        col("last_name"),
        col("display_name").alias("full_name"),
        col("email"),
        col("phone"),
        col("city"),
        col("country"),
        col("region"),
        col("signup_date"),
        col("tenure_days"),
        col("tenure_bucket"),
        col("seniority_rank"),
        col("is_active"),
        col("is_valid_email")
    ).dropDuplicates(["customer_id"])

    dim = add_audit_columns(dim, source)

    spark.sql("CREATE DATABASE IF NOT EXISTS " + GOLD_SCHEMA)
    dim.write.mode("overwrite").format("delta").saveAsTable(target)
    print("dim_customer done ✅ — rows:", dim.count())


# =============================================================================
# DIMENSION TABLE 3 — dim_date
# =============================================================================
def build_dim_date():
    print("\n=== BUILDING dim_date ===")
    target = GOLD_SCHEMA + ".dim_date"

    # Generate date range 2024-01-01 to 2026-12-31
    df = spark.sql("""
        SELECT sequence(
            to_date('2024-01-01'),
            to_date('2026-12-31'),
            interval 1 day
        ) as date_array
    """).selectExpr("explode(date_array) as full_date")

    dim = df.select(
        col("full_date"),
        year(col("full_date")).alias("year"),
        month(col("full_date")).alias("month"),
        dayofmonth(col("full_date")).alias("day"),
        dayofweek(col("full_date")).alias("weekday"),
        when(dayofweek(col("full_date")).isin(1, 7), lit(True))
            .otherwise(lit(False)).alias("is_weekend"),
        when(month(col("full_date")).isin(12, 1, 2), lit("Winter"))
            .when(month(col("full_date")).isin(3, 4, 5), lit("Spring"))
            .when(month(col("full_date")).isin(6, 7, 8), lit("Summer"))
            .otherwise(lit("Autumn")).alias("season")
    )

    dim = add_audit_columns(dim, "generated")

    spark.sql("CREATE DATABASE IF NOT EXISTS " + GOLD_SCHEMA)
    dim.write.mode("overwrite").format("delta").saveAsTable(target)
    print("dim_date done ✅ — rows:", dim.count())


# =============================================================================
# FACT TABLE — fact_orders (Star Schema center)
# =============================================================================
def build_fact_orders():
    print("\n=== BUILDING fact_orders ===")
    source_orders    = SILVER_SCHEMA + ".silver_orders_transformed"
    source_customers = GOLD_SCHEMA   + ".dim_customer"
    source_products  = GOLD_SCHEMA   + ".dim_product"
    target           = GOLD_SCHEMA   + ".fact_orders"

    orders    = spark.read.table(source_orders)
    customers = spark.read.table(source_customers).select("customer_id")
    products  = spark.read.table(source_products).select("product_id")

    # orders mein customer_id nahi hai (FakeAPI simulate) — product_id se link karenge
    # real project mein orders table mein customer_id hoga
    fact = orders.select(
        col("order_id"),
        col("product_id"),
        col("order_date"),
        col("order_year"),
        col("order_month"),
        col("order_day"),
        col("order_weekday"),
        col("is_weekend"),
        col("price").alias("unit_price"),
        col("revenue"),
        col("revenue_with_tax"),
        col("net_revenue"),
        col("price_bucket"),
        col("rating_rate"),
        col("rating_count"),
        col("is_highly_rated"),
        col("rank_in_category"),
        col("price_change"),
        col("category")
    )

    # Add surrogate key
    fact = fact.withColumn("order_sk", monotonically_increasing_id())

    fact = add_audit_columns(fact, source_orders)

    spark.sql("CREATE DATABASE IF NOT EXISTS " + GOLD_SCHEMA)
    fact.write.mode("overwrite").format("delta").saveAsTable(target)
    print("fact_orders done ✅ — rows:", fact.count())


# =============================================================================
# GOLD AGGREGATE TABLE 1 — agg_sales_by_category
# =============================================================================
def build_agg_sales_by_category():
    print("\n=== BUILDING agg_sales_by_category ===")
    source = GOLD_SCHEMA + ".fact_orders"
    target = GOLD_SCHEMA + ".agg_sales_by_category"

    df = spark.read.table(source)

    agg = df.groupBy("category", "order_year", "order_month").agg(
        spark_sum("revenue").alias("total_revenue"),
        spark_sum("revenue_with_tax").alias("total_revenue_with_tax"),
        avg("unit_price").alias("avg_price"),
        count("order_id").alias("total_orders"),
        countDistinct("product_id").alias("unique_products"),
        spark_max("unit_price").alias("max_price"),
        spark_min("unit_price").alias("min_price"),
        avg("rating_rate").alias("avg_rating")
    )

    agg = agg.withColumn("total_revenue",          spark_round(col("total_revenue"), 2))
    agg = agg.withColumn("total_revenue_with_tax",  spark_round(col("total_revenue_with_tax"), 2))
    agg = agg.withColumn("avg_price",               spark_round(col("avg_price"), 2))
    agg = agg.withColumn("avg_rating",              spark_round(col("avg_rating"), 2))

    agg = add_audit_columns(agg, source)

    agg.write.mode("overwrite").format("delta").saveAsTable(target)
    print("agg_sales_by_category done ✅ — rows:", agg.count())


# =============================================================================
# GOLD AGGREGATE TABLE 2 — agg_sales_by_date
# =============================================================================
def build_agg_sales_by_date():
    print("\n=== BUILDING agg_sales_by_date ===")
    source = GOLD_SCHEMA + ".fact_orders"
    target = GOLD_SCHEMA + ".agg_sales_by_date"

    df = spark.read.table(source)

    agg = df.groupBy("order_year", "order_month", "order_day", "is_weekend").agg(
        spark_sum("revenue").alias("daily_revenue"),
        count("order_id").alias("daily_orders"),
        avg("unit_price").alias("avg_order_value"),
        countDistinct("product_id").alias("unique_products_sold")
    )

    agg = agg.withColumn("daily_revenue",    spark_round(col("daily_revenue"), 2))
    agg = agg.withColumn("avg_order_value",  spark_round(col("avg_order_value"), 2))

    agg = add_audit_columns(agg, source)

    agg.write.mode("overwrite").format("delta").saveAsTable(target)
    print("agg_sales_by_date done ✅ — rows:", agg.count())


# =============================================================================
# GOLD AGGREGATE TABLE 3 — agg_top_products
# =============================================================================
def build_agg_top_products():
    print("\n=== BUILDING agg_top_products ===")
    source_fact    = GOLD_SCHEMA + ".fact_orders"
    source_product = GOLD_SCHEMA + ".dim_product"
    target         = GOLD_SCHEMA + ".agg_top_products"

    fact    = spark.read.table(source_fact)
    dim_prd = spark.read.table(source_product)

    agg = fact.groupBy("product_id").agg(
        spark_sum("revenue").alias("total_revenue"),
        count("order_id").alias("total_orders"),
        avg("rating_rate").alias("avg_rating")
    )

    # Join with dim_product to get product details
    agg = agg.join(
        dim_prd.select("product_id", "product_name", "category", "category_group", "price_tier"),
        on="product_id",
        how="left"
    )

    agg = agg.withColumn("total_revenue", spark_round(col("total_revenue"), 2))
    agg = agg.withColumn("avg_rating",    spark_round(col("avg_rating"), 2))
    agg = agg.orderBy(col("total_revenue").desc())

    agg = add_audit_columns(agg, source_fact)

    agg.write.mode("overwrite").format("delta").saveAsTable(target)
    print("agg_top_products done ✅ — rows:", agg.count())


# ── RUN ALL ──────────────────────────────────────────────────────────────────
print("GOLD LAYER STARTING...")

# Dimensions pehle
build_dim_product()
build_dim_customer()
build_dim_date()

# Fact table
build_fact_orders()

# Aggregates
build_agg_sales_by_category()
build_agg_sales_by_date()
build_agg_top_products()

print("\nGOLD LAYER COMPLETE ✅")
print("\nTables created:")
spark.sql("SHOW TABLES IN " + GOLD_SCHEMA).show(truncate=False)
