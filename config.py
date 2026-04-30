# =============================================================================
# config.py — Central Configuration (No hardcoding anywhere else!)
# =============================================================================

import os
from datetime import datetime

# -----------------------------------------------------------------------------
# ENVIRONMENT  (change "dev" to "prod" when needed)
# -----------------------------------------------------------------------------
ENV = os.getenv("PIPELINE_ENV", "dev")

# -----------------------------------------------------------------------------
# CATALOG / DATABASE NAMES  (Unity Catalog style: catalog.schema.table)
# Community Edition mein catalog = "hive_metastore" hoga
# -----------------------------------------------------------------------------
CATALOG        = os.getenv("CATALOG", "hive_metastore")
BRONZE_SCHEMA  = f"ecommerce_bronze_{ENV}"
SILVER_SCHEMA  = f"ecommerce_silver_{ENV}"
GOLD_SCHEMA    = f"ecommerce_gold_{ENV}"

# -----------------------------------------------------------------------------
# DBFS PATHS  (S3 / ADLS ka substitute in Community Edition)
# -----------------------------------------------------------------------------
BASE_PATH        = f"dbfs:/FileStore/ecommerce/{ENV}"
BRONZE_PATH      = f"{BASE_PATH}/bronze"
SILVER_PATH      = f"{BASE_PATH}/silver"
GOLD_PATH        = f"{BASE_PATH}/gold"
CHECKPOINT_PATH  = f"{BASE_PATH}/checkpoints"
WATERMARK_PATH   = f"{BASE_PATH}/watermarks"
RAW_FILES_PATH   = f"{BASE_PATH}/raw_files"

# -----------------------------------------------------------------------------
# SOURCE — REST API  (simulate orders)
# -----------------------------------------------------------------------------
API_CONFIG = {
    "base_url"  : os.getenv("API_BASE_URL", "https://fakestoreapi.com"),
    "endpoints" : {
        "orders"   : "/products",   # FakeStoreAPI free endpoint
        "users"    : "/users",
        "carts"    : "/carts",
    },
    "timeout"   : 30,
    "max_retries": 3,
}

# -----------------------------------------------------------------------------
# SOURCE — CSV (products)
# -----------------------------------------------------------------------------
CSV_CONFIG = {
    "products_path" : f"{RAW_FILES_PATH}/products.csv",
    "delimiter"     : ",",
    "encoding"      : "utf-8",
    "header"        : True,
}

# -----------------------------------------------------------------------------
# SOURCE — Legacy DB / SQLite (customers)
# -----------------------------------------------------------------------------
LEGACY_DB_CONFIG = {
    # Community Edition mein SQLite file DBFS pe store karenge
    "db_path"   : "/tmp/legacy_customers.db",
    "table"     : "customers",
}

# -----------------------------------------------------------------------------
# LOAD STRATEGY
# full_load = truncate & reload | incremental = watermark based
# -----------------------------------------------------------------------------
LOAD_CONFIG = {
    "orders"    : {"mode": "incremental", "watermark_col": "order_date",    "pk": "order_id"},
    "products"  : {"mode": "full_load",   "watermark_col": None,            "pk": "product_id"},
    "customers" : {"mode": "incremental", "watermark_col": "updated_at",    "pk": "customer_id"},
}

# -----------------------------------------------------------------------------
# DELTA TABLE OPTIONS
# -----------------------------------------------------------------------------
DELTA_OPTIONS = {
    "mergeSchema"          : "true",
    "optimizeWrite"        : "true",       # auto small file compaction
    "autoCompact"          : "true",
}

# Partition columns per table
PARTITION_CONFIG = {
    "orders"    : ["order_year", "order_month"],
    "products"  : ["category"],
    "customers" : ["country"],
}

# Z-ORDER columns per table (Silver / Gold optimisation)
ZORDER_CONFIG = {
    "orders"    : ["customer_id", "product_id"],
    "products"  : ["product_id"],
    "customers" : ["customer_id"],
}

# -----------------------------------------------------------------------------
# PIPELINE RUN METADATA
# -----------------------------------------------------------------------------
PIPELINE_META = {
    "pipeline_name" : "ecommerce_medallion_pipeline",
    "version"       : "1.0.0",
    "owner"         : "Pratham Gupta",
    "run_date"      : datetime.now().strftime("%Y-%m-%d"),
    "run_ts"        : datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
    "env"           : ENV,
}
