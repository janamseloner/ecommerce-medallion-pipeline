# =============================================================================
# utils/watermark.py — Watermark management for incremental loads
# =============================================================================

from pyspark.sql import SparkSession
from pyspark.sql.functions import lit
from pyspark.sql.types import StructType, StructField, StringType


WATERMARK_SCHEMA = StructType([
    StructField("table_name",    StringType(), False),
    StructField("last_watermark",StringType(), True),
])


def get_watermark(spark: SparkSession, table_name: str, watermark_path: str) -> str:
    """
    Read last watermark value for a given table.
    Returns None if no watermark exists yet (first run = full load).
    """
    wm_file = f"{watermark_path}/{table_name}.json"
    try:
        df  = spark.read.json(wm_file)
        val = df.filter(df.table_name == table_name).select("last_watermark").collect()
        if val:
            print(f"[WATERMARK] Found watermark for '{table_name}': {val[0][0]}")
            return val[0][0]
    except Exception:
        print(f"[WATERMARK] No watermark file found for '{table_name}' — first run")
    return None


def save_watermark(spark: SparkSession, table_name: str, watermark_value: str, watermark_path: str):
    """
    Persist new watermark value to DBFS as JSON.
    Overwrites previous watermark for the table.
    """
    wm_file = f"{watermark_path}/{table_name}.json"
    data    = [(table_name, watermark_value)]
    df      = spark.createDataFrame(data, schema=WATERMARK_SCHEMA)
    df.write.mode("overwrite").json(wm_file)
    print(f"[WATERMARK] Saved '{table_name}' watermark → {watermark_value}")
