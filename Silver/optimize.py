from pyspark.sql import SparkSession

ENV           = "dev"
SILVER_SCHEMA = "ecommerce_silver_dev"

spark = SparkSession.builder.getOrCreate()

# Z-ORDER config — kaunse columns pe filter/join hota hai
TABLES = {
    SILVER_SCHEMA + ".silver_orders_transformed"   : ["order_id", "product_id", "category"],
    SILVER_SCHEMA + ".silver_products_transformed" : ["product_id", "category"],
    SILVER_SCHEMA + ".silver_customers_transformed": ["customer_id", "country", "region"],
}

VACUUM_RETAIN_HOURS = 168  # 7 days


# =============================================================================
# OPTIMIZE + Z-ORDER
# =============================================================================
def optimize_table(table_name, zorder_cols):
    print("\n[OPTIMIZE] Running on:", table_name)
    cols_str = ", ".join(zorder_cols)
    try:
        spark.sql("OPTIMIZE " + table_name + " ZORDER BY (" + cols_str + ")")
        print("[OPTIMIZE] ✅ Done — Z-ORDER on:", cols_str)
    except Exception as e:
        print("[OPTIMIZE] ❌ Failed:", str(e))


# =============================================================================
# TABLE STATS
# =============================================================================
def show_table_stats(table_name):
    print("\n[STATS]", table_name)
    try:
        df = spark.read.format("delta").table(table_name)
        print("  Rows    :", df.count())
        print("  Columns :", len(df.columns))
        spark.sql("DESCRIBE DETAIL " + table_name).select("name", "numFiles", "sizeInBytes").show(truncate=False)
    except Exception as e:
        print("[STATS] Could not fetch:", str(e))


# =============================================================================
# VACUUM
# =============================================================================
def vacuum_table(table_name):
    print("\n[VACUUM] Running on:", table_name)
    try:
        spark.conf.set("spark.databricks.delta.retentionDurationCheck.enabled", "false")
        spark.sql("VACUUM " + table_name + " RETAIN " + str(VACUUM_RETAIN_HOURS) + " HOURS")
        print("[VACUUM] ✅ Done:", table_name)
    except Exception as e:
        print("[VACUUM] ❌ Failed:", str(e))


# =============================================================================
# DELTA HISTORY
# =============================================================================
def show_history(table_name):
    print("\n[HISTORY]", table_name)
    try:
        spark.sql("DESCRIBE HISTORY " + table_name).show(5, truncate=False)
    except Exception as e:
        print("[HISTORY] Could not fetch:", str(e))


# ── RUN ──────────────────────────────────────────────────────────────────────
print("SILVER OPTIMIZE STARTING...")

print("\n--- OPTIMIZE + Z-ORDER ---")
for table, zcols in TABLES.items():
    optimize_table(table, zcols)

print("\n--- TABLE STATS ---")
for table in TABLES:
    show_table_stats(table)

print("\n--- VACUUM ---")
for table in TABLES:
    vacuum_table(table)

print("\n--- DELTA HISTORY ---")
for table in TABLES:
    show_history(table)

print("\nSILVER OPTIMIZE COMPLETE ✅")
