# =============================================================================
# utils/delta_utils.py — Reusable Delta Lake helpers
# =============================================================================

from pyspark.sql import SparkSession, DataFrame
from delta.tables import DeltaTable


def create_schema_if_not_exists(spark: SparkSession, schema_name: str):
    """Create database/schema if it doesn't exist."""
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {schema_name}")
    print(f"[DELTA] Schema ready: {schema_name}")


def upsert_to_delta(
    spark       : SparkSession,
    df_new      : DataFrame,
    full_table  : str,
    table_path  : str,
    pk_col      : str,
    delta_options: dict,
):
    """
    MERGE (upsert) new records into existing Delta table.
    - If table doesn't exist → creates it (first incremental run)
    - If record exists (matched by pk_col) → UPDATE
    - If record is new → INSERT
    """
    table_exists = spark._jvm.org.apache.spark.sql.delta.catalog.DeltaCatalog \
        if False else _check_table_exists(spark, full_table)

    if not table_exists:
        print(f"[DELTA] Table not found — creating: {full_table}")
        (
            df_new.write
            .format("delta")
            .mode("overwrite")
            .options(**delta_options)
            .option("path", table_path)
            .saveAsTable(full_table)
        )
        return

    # MERGE
    delta_tbl = DeltaTable.forName(spark, full_table)
    (
        delta_tbl.alias("target")
        .merge(
            df_new.alias("source"),
            f"target.{pk_col} = source.{pk_col}"
        )
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )
    print(f"[DELTA] MERGE complete on: {full_table} (pk={pk_col})")


def _check_table_exists(spark: SparkSession, full_table: str) -> bool:
    """Check if a Delta table already exists."""
    try:
        spark.sql(f"DESCRIBE TABLE {full_table}")
        return True
    except Exception:
        return False


def optimize_table(spark: SparkSession, full_table: str, zorder_cols: list = None):
    """
    Run OPTIMIZE + optional Z-ORDER on a Delta table.
    Reduces small files, improves query speed.
    """
    print(f"[OPTIMIZE] Running OPTIMIZE on: {full_table}")
    if zorder_cols:
        cols = ", ".join(zorder_cols)
        spark.sql(f"OPTIMIZE {full_table} ZORDER BY ({cols})")
        print(f"[OPTIMIZE] Z-ORDER applied on: {cols}")
    else:
        spark.sql(f"OPTIMIZE {full_table}")
    print(f"[OPTIMIZE] ✅ Done: {full_table}")


def vacuum_table(spark: SparkSession, full_table: str, retain_hours: int = 168):
    """
    VACUUM Delta table — removes old file versions.
    Default retain = 168 hours (7 days).
    """
    print(f"[VACUUM] Running VACUUM on: {full_table} (retain={retain_hours}h)")
    spark.sql(f"VACUUM {full_table} RETAIN {retain_hours} HOURS")
    print(f"[VACUUM] ✅ Done: {full_table}")
