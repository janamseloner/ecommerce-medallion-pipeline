# 🛒 E-Commerce Data Pipeline — Medallion Architecture on Databricks

> **Author:** Pratham Gupta  
> **Platform:** Databricks Community Edition  
> **Architecture:** Medallion (Bronze → Silver → Gold)  
> **Domain:** E-Commerce  

---

## 📌 Project Overview

A complete, production-grade **Data Engineering Pipeline** that ingests data from multiple sources, cleans and transforms it, and exposes it as optimized Gold tables using the **Medallion Architecture** pattern.

The pipeline handles:
- ✅ Multi-source ingestion (REST API, CSV, Legacy Database)
- ✅ Full Load & Incremental Load strategies
- ✅ Data cleaning, validation, and transformation
- ✅ Star Schema in Gold layer (Fact + Dimension tables)
- ✅ Z-ORDER optimization and file compaction
- ✅ Parameterized config (zero hardcoding)
- ✅ Pipeline orchestration via Databricks Jobs

---

## 🏗️ Architecture

```
┌─────────────────────────────────────────────────────┐
│                  DATA SOURCES                       │
│  REST API (Orders) │ CSV (Products) │ DB (Customers)│
└──────────┬──────────────┬──────────────┬────────────┘
           │              │              │
           ▼              ▼              ▼
┌─────────────────────────────────────────────────────┐
│              BRONZE LAYER (Raw Ingestion)           │
│   bronze_orders  │ bronze_products │ bronze_customers│
│            Delta Tables + Audit Columns             │
└──────────────────────────┬──────────────────────────┘
                           │
                           ▼
┌─────────────────────────────────────────────────────┐
│           SILVER LAYER (Clean & Transform)          │
│  silver_orders  │ silver_products │ silver_customers │
│     Dedup │ Null Handling │ Business Logic │ Z-ORDER │
└──────────────────────────┬──────────────────────────┘
                           │
                           ▼
┌─────────────────────────────────────────────────────┐
│              GOLD LAYER (Star Schema)               │
│  dim_product │ dim_customer │ dim_date │ fact_orders │
│   agg_sales_by_category │ agg_sales_by_date         │
│              agg_top_products                       │
└─────────────────────────────────────────────────────┘
```

---

## 📁 Project Structure

```
ecommerce-medallion-pipeline/
│
├── config                    # Central config — all params, zero hardcoding
├── main                      # Pipeline orchestrator — runs all notebooks in order
│
├── Bronze/
│   ├── ingest_api            # Orders ingestion from REST API (FakeStoreAPI)
│   ├── ingest_csv            # Products ingestion from CSV file on DBFS
│   └── ingest_db             # Customers ingestion from Legacy SQLite DB
│
├── Silver/
│   ├── clean                 # Dedup, null handling, string cleaning, validation
│   ├── transform             # Business logic, date columns, rankings, regions
│   └── optimize              # OPTIMIZE + Z-ORDER + VACUUM
│
├── Gold/
│   └── gold_layer            # Star Schema: Dims + Fact + Aggregates
│
└── Utils/
    ├── watermark             # Watermark read/write for incremental loads
    └── delta_utils           # MERGE, OPTIMIZE, VACUUM helper functions
```

---

## 🔄 Data Sources

| Source | Type | Table | Load Strategy |
|--------|------|-------|---------------|
| FakeStoreAPI | REST API | bronze_orders | Incremental (watermark on order_date) |
| products.csv | CSV File on DBFS | bronze_products | Full Load |
| Legacy SQLite DB | Database (JDBC) | bronze_customers | Incremental (watermark on updated_at) |

---

## 🥉 Bronze Layer — Raw Ingestion

**Purpose:** Ingest raw data from all sources exactly as received. Acts as the source of truth — data is never lost.

**Key features:**
- Generic API fetcher with retry logic (3 attempts)
- Explicit schema definition on CSV read (no inferSchema)
- SQLite simulates legacy on-prem Oracle/MySQL database
- Audit columns added to every table: `_ingest_ts`, `_source`, `_pipeline_name`, `_env`
- Watermark-based incremental load with MERGE (upsert) on primary key
- Delta partitioning: orders by `order_year/order_month`, products by `category`, customers by `country`

---

## 🥈 Silver Layer — Clean & Transform

### clean.py
| Operation | Tables | Detail |
|-----------|--------|--------|
| Deduplication | All | `dropDuplicates()` on primary key |
| Null handling | All | `dropna()` on critical columns |
| String cleaning | All | `trim()`, `lower()`, `upper()` |
| Validation | orders | `price > 0` filter |
| Validation | products | negative stock set to 0 |
| Email validation | customers | regex format check |
| Derived columns | orders | `price_bucket` (low/medium/high) |
| Derived columns | products | `stock_status`, `price_with_tax` (18% GST) |
| Derived columns | customers | `full_name` via `concat()`, `is_active` boolean |

### transform.py
| Transformation | Detail |
|----------------|--------|
| Date columns | `order_year`, `order_month`, `order_day`, `is_weekend` |
| Revenue | `revenue`, `revenue_with_tax`, `net_revenue` |
| Customer tenure | Days since signup → New/Regular/Loyal/Champion |
| Region mapping | Country → Asia/Europe/North America/Oceania |
| Rankings | `dense_rank()` within category by rating |
| Price trend | `lag()` window function for price change |

### optimize.py
```sql
OPTIMIZE silver_orders_transformed ZORDER BY (order_id, product_id, category)
OPTIMIZE silver_products_transformed ZORDER BY (product_id, category)
OPTIMIZE silver_customers_transformed ZORDER BY (customer_id, country, region)
VACUUM <table> RETAIN 168 HOURS
```

---

## 🥇 Gold Layer — Star Schema

```
                    ┌─────────────┐
                    │  dim_date   │
                    │  full_date  │
                    │  year/month │
                    │  is_weekend │
                    └──────┬──────┘
                           │
┌──────────────┐    ┌──────┴──────┐    ┌───────────────┐
│  dim_product │────│ fact_orders │────│  dim_customer │
│  product_id  │    │  order_id   │    │  customer_id  │
│  category    │    │  product_id │    │  full_name    │
│  price_tier  │    │  revenue    │    │  region       │
│  stock_status│    │  order_date │    │  tenure_bucket│
└──────────────┘    └─────────────┘    └───────────────┘
```

**Aggregate Tables (pre-computed metrics):**
| Table | Metrics |
|-------|---------|
| `agg_sales_by_category` | total revenue, order count, avg price per category per month |
| `agg_sales_by_date` | daily revenue, daily orders, weekend vs weekday |
| `agg_top_products` | best selling products with revenue, orders, avg rating |

---

## ⚙️ Configuration — config notebook

All parameters are centralized in `config` — **zero hardcoding** in any other notebook:

```python
ENV           = "dev"              # Change to "prod" for production
BRONZE_SCHEMA = f"ecommerce_bronze_{ENV}"
SILVER_SCHEMA = f"ecommerce_silver_{ENV}"
GOLD_SCHEMA   = f"ecommerce_gold_{ENV}"
BASE_PATH     = f"dbfs:/FileStore/ecommerce/{ENV}"

LOAD_CONFIG = {
    "orders"   : {"mode": "incremental", "watermark_col": "order_date", "pk": "order_id"},
    "products" : {"mode": "full_load",   "watermark_col": None,         "pk": "product_id"},
    "customers": {"mode": "incremental", "watermark_col": "updated_at", "pk": "customer_id"},
}
```

---

## 🔁 Full Load vs Incremental Load Strategy

| Strategy | How it works | Used for |
|----------|-------------|----------|
| **Full Load** | Truncate and reload entire table every run | Products (small, static catalog) |
| **Incremental** | Read watermark → filter new records → MERGE | Orders, Customers |

**Watermark mechanism:**
```
Run 1: No watermark → load all → save watermark = "2024-01-20"
Run 2: Load watermark → filter order_date > "2024-01-20" → MERGE → save new watermark
Run 3: Load watermark → filter order_date > "2024-01-25" → MERGE → save new watermark
```

---

## ⚡ Performance Optimization

### Z-ORDER
Co-locates related data in the same Delta files based on frequently filtered columns. A query filtering by `customer_id` only reads files containing that customer — skipping all others (data skipping).

### File Compaction (OPTIMIZE)
Merges many small files created by incremental writes into fewer large files — reducing I/O overhead significantly.

### VACUUM
Removes old file versions (older than 7 days) from DBFS to reclaim storage after OPTIMIZE.

---

## 🚀 How to Run

### Option 1 — Run full pipeline (recommended)
```
Open: ETL/main notebook
Run All → pipeline runs all 7 stages in sequence
```

### Option 2 — Run individual layers
```
1. ETL/Bronze/ingest_api    → orders from API
2. ETL/Bronze/ingest_csv    → products from CSV
3. ETL/Bronze/ingest_db     → customers from DB
4. ETL/Silver/clean         → clean all tables
5. ETL/Silver/transform     → transform all tables
6. ETL/Silver/optimize      → Z-ORDER + VACUUM
7. ETL/Gold/gold_layer      → build Star Schema
```

### Option 3 — Databricks Job
```
Jobs & Pipelines → ecommerce_medallion_pipeline → Run Now
```

---

## 📅 Scheduling (Production)

In production, the job would be scheduled with a cron expression:

```
# Run daily at 2:00 AM
Cron: 0 2 * * *

# Run every hour
Cron: 0 * * * *
```

In Databricks Community Edition, the job is triggered manually via **Run Now** in Jobs & Pipelines, which demonstrates the same execution flow as scheduled runs.

---

## 🗄️ Tables Created

### Bronze (Raw)
- `ecommerce_bronze_dev.bronze_orders`
- `ecommerce_bronze_dev.bronze_products`
- `ecommerce_bronze_dev.bronze_customers`

### Silver (Cleaned & Transformed)
- `ecommerce_silver_dev.silver_orders` → `silver_orders_transformed`
- `ecommerce_silver_dev.silver_products` → `silver_products_transformed`
- `ecommerce_silver_dev.silver_customers` → `silver_customers_transformed`

### Gold (Star Schema)
- `ecommerce_gold_dev.dim_product`
- `ecommerce_gold_dev.dim_customer`
- `ecommerce_gold_dev.dim_date`
- `ecommerce_gold_dev.fact_orders`
- `ecommerce_gold_dev.agg_sales_by_category`
- `ecommerce_gold_dev.agg_sales_by_date`
- `ecommerce_gold_dev.agg_top_products`

---

## 🛠️ Tech Stack

| Technology | Usage |
|------------|-------|
| **Databricks Community Edition** | Primary platform, notebooks, Jobs |
| **Apache Spark (PySpark)** | Distributed processing, transformations, window functions |
| **Delta Lake** | ACID storage, time travel, schema evolution |
| **Python** | Pipeline code, API calls, DB connections |
| **FakeStoreAPI** | Simulated REST API source for orders |
| **SQLite** | Simulated legacy on-premises database |
| **DBFS** | Databricks File System (S3 equivalent) |
| **Databricks Jobs** | Pipeline orchestration and deployment |

---

## 📊 Key Concepts Demonstrated

- **Medallion Architecture** — Bronze → Silver → Gold data layering
- **Delta Lake** — ACID transactions, time travel, schema enforcement
- **Incremental Load** — Watermark-based processing, MERGE upsert
- **Window Functions** — dense_rank(), lag(), row_number()
- **Star Schema** — Fact + Dimension tables for analytical queries
- **Z-ORDER** — Data skipping for query optimization
- **Idempotency** — Pipeline produces same result on multiple runs
- **Parameterization** — Zero hardcoding via central config

---

*Built with ❤️ by Pratham Gupta*

