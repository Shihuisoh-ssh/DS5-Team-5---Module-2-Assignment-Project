# Olist Data Pipeline — Architecture

**Date:** 2026-06-02
**Project:** DS5 Team 5 — Module 2 Assignment
**Dataset:** Olist Brazilian E-Commerce (Supabase)
**Warehouse:** GCP BigQuery (`our-project-93971`)
**EL tool:** Meltano (`tap-postgres` → `target-bigquery`)
**Transform tool:** dbt (`our_project`, profile: `our_project`)
**Orchestration:** Dagster

---

## Overview

An end-to-end ELT pipeline. Data is extracted from Supabase using Meltano and loaded into BigQuery raw tables. dbt handles all transformation: a staging layer standardises the raw data, a data quality layer surfaces problematic rows as queryable tables, and a star schema layer produces clean, analysis-ready fact and dimension tables. Dagster orchestrates the full sequence.

```text
Supabase (source)
    ↓ Meltano (tap-postgres → target-bigquery)
Supabase_data (raw, untouched)
    ↓ dbt run
olist_dev_staging   (views)
    ↓               ↓
olist_dev_          olist_dev_star
data_quality        (tables)
(tables)                ↓
                    Jupyter Notebook analysis
```

Dagster orchestration:

```text
Dagster job
├── Step 1: Run Meltano (Supabase → BigQuery)
├── Step 2: Validate raw tables exist in BigQuery
├── Step 3: Run dbt staging models
├── Step 4: Run dbt data quality models
├── Step 5: Run dbt star schema models
├── Step 6: Run dbt tests
└── Step 7: Prepare final tables for Jupyter Notebook analysis
```

---

## Source Data

**Source system:** Supabase (PostgreSQL)
**EL tool:** Meltano — `tap-postgres` (meltanolabs) → `target-bigquery` (z3z1ma)
**BigQuery project:** `our-project-93971`
**BigQuery dataset (raw):** `Supabase_data`

| Table | Rows | Key columns |
|---|---|---|
| `orders` | 99,441 | order_id, customer_id, order_status, timestamps |
| `customers` | 99,441 | customer_id, customer_unique_id, zip_code_prefix, city, state |
| `order_items` | 112,650 | order_id, order_item_id, product_id, seller_id, price, freight_value |
| `order_payments` | 103,886 | order_id, payment_sequential, payment_type, payment_value |
| `order_reviews` | 99,224 | review_id, order_id, review_score, review_comment_message |
| `products` | 32,951 | product_id, product_category_name, dimensions, weight |
| `sellers` | 3,095 | seller_id, zip_code_prefix, city, state |
| `geolocation` | 1,000,163 | zip_code_prefix, lat, lng, city, state |
| `category_name_translation` | 71 | product_category_name, product_category_name_english |

All 9 tables extracted from Supabase via Meltano and loaded into BigQuery `Supabase_data` dataset (row counts verified).

---

## dbt Project Configuration

**Project name:** `our_project`
**Profile:** `our_project` (in `~/.dbt/profiles.yml`)
**Connection:** OAuth, BigQuery, `our-project-93971`, location: US
**Output dataset (base):** `olist_dev`

**dbt_project.yml materialisation config:**
```yaml
models:
  our_project:
    staging:
      +materialized: view
      +schema: staging
    star:
      +materialized: table
      +schema: star
    data_quality:
      +materialized: table
      +schema: data_quality
```

**Commands to run:**
```bash
dbt debug   # verify connection
dbt run     # build all layers in BigQuery
dbt test    # validate schema tests
```

---

## Layer 1 — Staging

**BigQuery dataset:** `olist_dev_staging`
**Materialisation:** Views
**Location:** `models/staging/`
**Naming:** `stg_<source_table>`

### Purpose
Translate raw source tables into a clean, consistently structured foundation. No filtering, no joins, no business logic. Every downstream layer reads from staging — never directly from `Supabase_data`.

### Rules
- One model per source table
- No rows filtered out
- No tables joined together
- No business calculations
- Type casting, column renaming, and null standardisation only

### Models

| Model | Key transformations |
|---|---|
| `stg_orders` | Cast 5 timestamp columns to `TIMESTAMP` |
| `stg_customers` | Rename `customer_zip_code_prefix` → `zip_code_prefix`; cast zip to `STRING` |
| `stg_order_items` | Cast `shipping_limit_date` to `TIMESTAMP` |
| `stg_order_payments` | Cast `payment_value` to `FLOAT64` |
| `stg_order_reviews` | Cast date columns; `NULLIF` on empty comment strings |
| `stg_products` | Cast numeric dimensions to `INT64`; no COALESCE on category (left to DQ/star layers) |
| `stg_sellers` | Rename `seller_zip_code_prefix` → `zip_code_prefix`; cast zip to `STRING` |
| `stg_geolocation` | Cast `lat`/`lng` to `FLOAT64`; rename for brevity |
| `stg_category_name_translation` | Rename `product_category_name_english` → `category_name_english` |

---

## Layer 2 — Data Quality

**BigQuery dataset:** `olist_dev_data_quality`
**Materialisation:** Tables
**Location:** `models/data_quality/`
**Naming:** `dq_<source_table>`

### Purpose
Capture every row from staging that fails a data quality check. Stores only problem rows — an empty table means clean data. Makes data issues visible and queryable in BigQuery for the whole team.

### Model structure
Every `dq_` model follows the same pattern:
- One boolean flag column per check (e.g. `null_customer_id`, `negative_price`)
- One `issues` summary column listing all problems on that row as a comma-separated string
- Only rows where at least one flag is `TRUE` are stored

```sql
SELECT
    <primary_key>,
    <field> IS NULL AS null_<field>,
    ARRAY_TO_STRING(ARRAY_REMOVE([
        IF(<field> IS NULL, 'null_<field>', NULL)
    ], NULL), ', ') AS issues
FROM {{ ref('stg_<table>') }}
WHERE <at least one flag is true>
```

### Checks per model

| Model | Checks |
|---|---|
| `dq_orders` | `null_customer_id`, `null_order_status`, `invalid_order_status`, `delivered_before_purchased` |
| `dq_customers` | `null_customer_id`, `null_customer_unique_id`, `null_zip_code` |
| `dq_order_items` | `null_order_id`, `null_product_id`, `null_seller_id`, `negative_price`, `negative_freight` |
| `dq_order_payments` | `null_order_id`, `zero_or_negative_payment_value`, `invalid_payment_type` |
| `dq_order_reviews` | `null_review_id`, `null_order_id`, `invalid_review_score` (not between 1–5) |
| `dq_products` | `null_product_id`, `null_category_name`, `null_dimensions`, `negative_weight` |
| `dq_sellers` | `null_seller_id`, `null_zip_code` |
| `dq_geolocation` | `null_lat_lng`, `out_of_range_coordinates` (outside Brazil bounds) |
| `dq_category_name_translation` | `null_category_name`, `null_english_name` |

---

## Layer 3 — Star Schema

**BigQuery dataset:** `olist_dev_star`
**Materialisation:** Tables
**Location:** `models/star/`
**Naming:** `fact_<name>`, `dim_<name>`

### Purpose
Produce clean, analysis-ready fact and dimension tables. Excludes all rows flagged in the data quality layer using a LEFT ANTI JOIN pattern.

### Clean record pattern
```sql
FROM {{ ref('stg_order_items') }} i
LEFT JOIN {{ ref('dq_order_items') }} dq
    ON i.order_id = dq.order_id
    AND i.order_item_id = dq.order_item_id
WHERE dq.order_id IS NULL
```

### Fact tables

**`fact_order_items`** — grain: one row per order item (order_id + order_item_id)

| Column | Source | Type |
|---|---|---|
| `order_id` | stg_order_items | FK → fact_orders |
| `order_item_id` | stg_order_items | INT64 |
| `product_id` | stg_order_items | FK → dim_products |
| `seller_id` | stg_order_items | FK → dim_sellers |
| `customer_id` | stg_orders | FK → dim_customers |
| `purchase_date` | stg_orders | FK → dim_dates |
| `price` | stg_order_items | FLOAT64 |
| `freight_value` | stg_order_items | FLOAT64 |
| `total_item_cost` | calculated | price + freight_value |

**`fact_orders`** — grain: one row per order

| Column | Source | Type |
|---|---|---|
| `order_id` | stg_orders | PK |
| `customer_id` | stg_orders | FK → dim_customers |
| `order_status` | stg_orders | STRING |
| `purchase_date` | stg_orders | FK → dim_dates |
| `delivery_days` | calculated | delivered_at − purchased_at |
| `estimated_delivery_days` | calculated | estimated_delivery − purchased_at |
| `review_score` | stg_order_reviews | INT64, NULL if no review |
| `total_payment_value` | stg_order_payments | SUM of payments per order |

### Dimension tables

**`dim_customers`** — one row per customer_id

| Column | Source |
|---|---|
| `customer_id` | stg_customers |
| `customer_unique_id` | stg_customers |
| `city` | stg_customers |
| `state` | stg_customers |
| `zip_code_prefix` | stg_customers |

**`dim_products`** — one row per product, English category joined in

| Column | Source |
|---|---|
| `product_id` | stg_products |
| `category_name` | stg_products |
| `category_name_english` | stg_category_name_translation (LEFT JOIN) |
| `name_length` | stg_products |
| `description_length` | stg_products |
| `photos_qty` | stg_products |
| `weight_g` | stg_products |

**`dim_sellers`** — one row per seller_id

| Column | Source |
|---|---|
| `seller_id` | stg_sellers |
| `city` | stg_sellers |
| `state` | stg_sellers |
| `zip_code_prefix` | stg_sellers |

**`dim_dates`** — generated date spine (not from a source table)

| Column | Description |
|---|---|
| `date_id` | DATE (PK), range 2016-01-01 to 2018-12-31 |
| `year` | INT64 |
| `month` | INT64 (1–12) |
| `month_name` | STRING |
| `quarter` | INT64 (1–4) |
| `day_of_week` | STRING |
| `is_weekend` | BOOLEAN |

Generated using `GENERATE_DATE_ARRAY('2016-01-01', '2018-12-31')` in BigQuery.

---

## Data Flow Summary

```
Supabase → Meltano → Supabase_data (raw)
    │
    ├── stg_orders
    ├── stg_customers
    ├── stg_order_items          ← Layer 1: Staging (views)
    ├── stg_order_payments
    ├── stg_order_reviews
    ├── stg_products
    ├── stg_sellers
    ├── stg_geolocation
    └── stg_category_name_translation
         │
         ├──────────────────────── Layer 2: Data Quality (tables)
         │    dq_orders            Flags problem rows per source
         │    dq_customers         Stored in olist_dev_data_quality
         │    dq_order_items
         │    dq_order_payments
         │    dq_order_reviews
         │    dq_products
         │    dq_sellers
         │    dq_geolocation
         │    dq_category_name_translation
         │
         └──────────────────────── Layer 3: Star Schema (tables)
              fact_order_items     Clean records only (DQ rows excluded)
              fact_orders          Stored in olist_dev_star
              dim_customers
              dim_products
              dim_sellers
              dim_dates
```

---

## Architecture vs Common Reference Flow

| Reference step | This project |
|---|---|
| Source data | Stored in Supabase (PostgreSQL) |
| Extract and load | Meltano `tap-postgres` → `target-bigquery` into `Supabase_data` dataset |
| `dbt debug` | Configured and passing |
| `dbt run` to build staging + marts | Builds all 3 layers in order |
| `dbt test` to validate | schema.yml tests on all layers |
| Orchestration | Dagster job sequences Meltano → dbt staging → dbt DQ → dbt star schema → dbt tests |
| Star schema | `olist_dev_star` — 2 facts, 4 dims |
| Analyse in BigQuery / notebook | `notebooks/01_data_understanding.ipynb` |

---

## Raw Data Relationships

```
customers
   ↓ customer_id
orders
   ↓ order_id
order_items
   ├── product_id → products → category_name_translation
   └── seller_id  → sellers

orders → order_payments
orders → order_reviews

customers / sellers → geolocation (via zip_code_prefix)
```
