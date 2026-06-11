# DS5 Team 5 — Module 2 Assignment Project
## Olist E-Commerce Data Warehouse and Analytics Pipeline

---

## 1. Project Overview

This project builds an end-to-end ELT data pipeline for the Olist Brazilian E-Commerce dataset. Data is extracted from Supabase using Meltano and loaded into GCP BigQuery, transformed through three dbt layers (staging → data quality → star schema), orchestrated by Dagster, and analysed in Jupyter notebooks.

**Status:** Pipeline complete — 24 dbt models built, 53 tests passing.

---

## 2. Business Problem

Raw e-commerce data is spread across multiple operational files (orders, customers, products, sellers, payments, reviews). This makes it difficult for business teams to answer questions consistently.

This project creates a structured data warehouse to support analysis on:
- Monthly sales trends and revenue
- Top-selling product categories
- Customer purchasing behaviour
- Seller performance
- Delivery performance vs estimates
- Review score patterns

---

## 3. Architecture

### Updated Pipeline Flow

This project uses an end-to-end ELT data engineering pipeline to move data from Supabase into Google BigQuery, transform it using dbt, orchestrate the workflow using Dagster, and analyse the final warehouse tables in Jupyter Notebook.

The pipeline flow is:

```text
Supabase
    ↓
Meltano
    ↓
GCP BigQuery raw tables
    ↓
dbt staging models
    ↓
dbt data quality models
    ↓
dbt star schema warehouse
    ↓
Dagster orchestration
    ↓
Jupyter Notebook visualisation
    ↓
Business insights and recommendation
```

---

### Tool Responsibilities

| Tool             | Role in This Project                                                                                          |
| ---------------- | ------------------------------------------------------------------------------------------------------------- |
| Supabase         | Source database that stores the raw operational dataset                                                       |
| Meltano          | Extracts data from Supabase and loads it into BigQuery                                                        |
| GCP BigQuery     | Cloud data warehouse used to store raw, staging, data quality, and star schema tables                         |
| dbt              | Transforms raw BigQuery tables into cleaned staging models, data quality models, and final star schema models |
| Dagster          | Orchestrates the pipeline so Meltano and dbt run in the correct sequence                                      |
| Jupyter Notebook | Connects to the final BigQuery warehouse tables for analysis and visualisation                                |

---

### Architecture Diagram

```text
Supabase source tables
        ↓
Meltano extract and load
        ↓
BigQuery raw dataset
        ↓
dbt staging layer
        ↓
dbt data quality layer
        ↓
dbt star schema layer
        ↓
Jupyter Notebook analysis
```

With Dagster orchestration:

```text
Dagster job
├── Step 1: Run Meltano extraction and loading
├── Step 2: Validate that raw tables exist in BigQuery
├── Step 3: Run dbt staging models
├── Step 4: Run dbt data quality models
├── Step 5: Run dbt star schema models
├── Step 6: Run dbt tests
└── Step 7: Prepare final tables for Jupyter Notebook analysis
```

---

### Pipeline Stages

#### 1. Source Layer — Supabase

Supabase acts as the source system for this project. The raw operational dataset is stored in Supabase before being extracted into the analytics warehouse.

Example source tables may include:

```text
orders
customers
products
sellers
payments
reviews
order_items
```

---

#### 2. Extraction and Loading Layer — Meltano

Meltano is used to extract data from Supabase and load it into BigQuery.

In this project, Meltano handles the EL part of ELT:

```text
Extract: Supabase
Load: BigQuery raw dataset
```

Meltano does not perform the main business transformations. Its main purpose is to move the source data reliably into BigQuery.

---

#### 3. Raw Data Layer — BigQuery

After Meltano runs, the raw Supabase data is stored in BigQuery.

This layer keeps the loaded data close to its original source structure. It acts as the landing area before dbt transformations are applied.

Example:

```text
BigQuery raw dataset
├── raw_orders
├── raw_customers
├── raw_products
├── raw_sellers
├── raw_payments
├── raw_reviews
└── raw_order_items
```

---

#### 4. Transformation Layer — dbt Staging Models

dbt is used to transform the raw BigQuery tables into standardised staging models.

The staging layer performs light cleaning and standardisation, such as:

```text
Renaming columns
Casting data types
Standardising date and timestamp fields
Standardising ID fields
Handling empty strings and null values
Keeping source row counts unchanged where possible
```

Example staging models:

```text
stg_orders
stg_customers
stg_products
stg_sellers
stg_payments
stg_reviews
stg_order_items
```

---

#### 5. Data Quality Layer — dbt Data Quality Models

The data quality layer identifies records that may affect analysis reliability.

Example checks include:

```text
Missing primary keys
Duplicate records
Invalid date logic
Negative payment or order values
Missing product categories
Missing customer or seller references
Orders without matching order items
```

The purpose of this layer is to make data issues visible before the final warehouse tables are used for analysis.

---

#### 6. Warehouse Layer — dbt Star Schema

The final analytics layer is built as a star schema.

The star schema contains fact and dimension tables that are easier for business analysis.

Example star schema:

```text
dim_customers       dim_products
       \              /
        \            /
       fact_order_items
        /      |      \
dim_sellers dim_dates dim_payments
```

Example warehouse models:

```text
fact_order_items
fact_orders
dim_customers
dim_products
dim_sellers
dim_dates
```

The fact tables store measurable business events, while the dimension tables store descriptive information.

This structure supports business questions such as:

```text
Monthly sales trends
Top product categories by revenue
Customer purchasing behaviour
Seller performance
Delivery delay analysis
Review score patterns
```

---

#### 7. Calculated Business Metrics

dbt is also used to create calculated metrics that support business analysis.

Example calculated fields:

```text
total_order_item_value = price + freight_value
delivery_days = delivered_customer_date - purchase_date
is_late_delivery = delivered_customer_date > estimated_delivery_date
order_month = month extracted from purchase date
review_score_group = low, medium, high
freight_ratio = freight_value / total_order_item_value
```

These metrics make the final warehouse tables more useful for business reporting and visualisation.

---

#### 8. Orchestration Layer — Dagster

Dagster is used to orchestrate the full pipeline.

Instead of manually running Meltano and dbt commands one by one, Dagster controls the workflow sequence.

Dagster ensures that:

```text
Meltano runs before dbt
dbt staging runs before dbt star schema models
dbt tests run after dbt models are built
Failures are easier to identify and troubleshoot
The pipeline is repeatable and observable
```

Recommended Dagster flow:

```text
extract_load_supabase_to_bigquery
        ↓
run_dbt_staging
        ↓
run_dbt_data_quality
        ↓
run_dbt_star_schema
        ↓
run_dbt_tests
```

---

#### 9. Analysis Layer — Jupyter Notebook

Jupyter Notebook is used only after the cleaned warehouse tables are created.

The notebook connects to BigQuery and queries the final dbt models for visualisation and business analysis.

The notebook should focus on:

```text
SQL queries from final warehouse tables
Pandas analysis
Charts and visualisation
Business issue identification
Recommendations
```

The notebook should not contain the main transformation logic, because transformation is handled by dbt.

---

### Final End-to-End Flow

```text
1. Store source data in Supabase
2. Use Meltano to extract from Supabase and load into BigQuery
3. Store raw loaded data in BigQuery raw tables
4. Use dbt to create staging models
5. Use dbt to create data quality models
6. Use dbt to create star schema warehouse models
7. Use dbt tests to validate data quality and relationships
8. Use Dagster to orchestrate Meltano and dbt in the correct sequence
9. Use Jupyter Notebook to analyse and visualise final warehouse tables
10. Present business insights and recommendations
```

---

### Summary

This project demonstrates a modern ELT data engineering workflow.

Supabase is used as the source database, Meltano handles extraction and loading, BigQuery acts as the cloud data warehouse, dbt performs transformation and testing, Dagster orchestrates the pipeline, and Jupyter Notebook is used for final analysis and visualisation.

The final output is a clean star schema warehouse that supports business analysis and decision-making.

---

## 4. Dataset

**Source:** Brazilian E-Commerce Public Dataset by Olist — stored in Supabase
**Extracted via:** Meltano (`tap-postgres` → `target-bigquery`)
**Loaded into:** `our-project-93971.Supabase_data` (BigQuery)

| Table | Rows |
|---|---:|
| orders | 99,441 |
| customers | 99,441 |
| order_items | 112,650 |
| order_payments | 103,886 |
| order_reviews | 99,224 |
| products | 32,951 |
| sellers | 3,095 |
| geolocation | 1,000,163 |
| category_name_translation | 71 |

All 9 tables verified to match local CSVs exactly.

---

## 5. dbt Pipeline

**Project:** `our_project`
**Profile:** `our_project` → BigQuery (`our-project-93971`), oauth, location: US

### Layer 1 — Staging (`olist_dev_staging`)
9 views. One per source table. Renames columns, casts data types, standardises nulls. No rows removed.

| Model | Key changes |
|---|---|
| `stg_orders` | 5 timestamp columns cast to TIMESTAMP |
| `stg_customers` | zip_code_prefix standardised to STRING |
| `stg_order_items` | price, freight_value cast to FLOAT64 |
| `stg_order_payments` | payment_value cast to FLOAT64 |
| `stg_order_reviews` | dates cast, empty comments → NULL |
| `stg_products` | dimensions cast to INT64, typos fixed in column names |
| `stg_sellers` | zip_code_prefix standardised to STRING |
| `stg_geolocation` | lat/lng cast to FLOAT64 |
| `stg_category_name_translation` | English name column renamed |

### Layer 2 — Data Quality (`olist_dev_data_quality`)
9 tables. Stores only flagged rows. Empty table = clean data.

| Model | Flagged rows | Main issues |
|---|---:|---|
| `dq_orders` | 0 | Clean ✅ |
| `dq_customers` | 0 | Clean ✅ |
| `dq_order_items` | 0 | Clean ✅ |
| `dq_order_payments` | 9 | Zero/negative payment values |
| `dq_order_reviews` | 0 | Clean ✅ |
| `dq_products` | 611 | 609 missing category name, 2 missing dimensions |
| `dq_sellers` | 0 | Clean ✅ |
| `dq_geolocation` | 42 | Coordinates outside Brazil bounds |
| `dq_category_name_translation` | 0 | Clean ✅ |

See [`docs/data_quality_report.md`](docs/data_quality_report.md) for full details.

### Layer 3 — Star Schema (`olist_dev_star`)
6 tables. Clean records only — DQ-flagged rows excluded.

| Model | Rows | Description |
|---|---:|---|
| `fact_order_items` | 112,650 | Revenue fact — one row per order line item |
| `fact_orders` | 99,400 | Order fact — delivery times, review score, payment total |
| `dim_customers` | 99,441 | Customer dimension |
| `dim_products` | 32,340 | Product dimension with English category names |
| `dim_sellers` | 3,095 | Seller dimension |
| `dim_dates` | 1,096 | Generated date spine (2016–2018) |

See [`docs/schema_design.md`](docs/schema_design.md) for column definitions and sample queries.

---

## 6. Running the Pipeline

**Prerequisites:** conda env `elt` with dbt-bigquery, GCP credentials via `gcloud auth application-default login`.

```bash
cd our_project

# 1. Verify connection
/home/fionalyh/miniconda3/envs/elt/bin/dbt debug

# 2. Build all models
/home/fionalyh/miniconda3/envs/elt/bin/dbt run

# 3. Run all tests
/home/fionalyh/miniconda3/envs/elt/bin/dbt test
```

Expected: `24 of 24 OK` on run, `53 of 53 PASS` on test.

---

## 7. Project Structure

```
meltano/                              ← full pipeline (lecture module 2 format)
├── extract/                          ← Meltano handles extraction (meltano.yml)
├── load/                             ← Meltano handles loading (meltano.yml)
├── transform/
│   └── our_project/                  ← dbt project root
│       ├── dbt_project.yml
│       └── models/
│           ├── sources.yml           ← All 9 BigQuery source tables declared
│           ├── staging/              ← 9 stg_* view models
│           ├── data_quality/         ← 9 dq_* table models
│           └── star/                 ← 2 fact + 4 dim table models
├── orchestrate/
│   └── dagster_project/              ← Dagster assets and schedule
├── notebook/
│   └── 01_data_understanding.ipynb   ← EDA + staging/DQ explanation
├── output/
├── plugins/                          ← Meltano plugin lock files
└── meltano.yml                       ← Meltano config (tap-postgres → target-bigquery)

module2-olist-data-pipeline/
├── data/
│   └── raw/                          ← Local reference copies of the 9 CSVs
├── docs/
│   ├── architecture.md               ← Pipeline design and layer details
│   ├── data_dictionary.md            ← Column definitions and staging mapping
│   ├── data_quality_report.md        ← DQ findings with actual row counts
│   └── schema_design.md              ← Star schema with sample queries
└── README.md
```

---

## 8. Documentation

| File | Contents |
|---|---|
| [`docs/architecture.md`](docs/architecture.md) | Full pipeline architecture, dbt config, layer-by-layer design |
| [`docs/data_dictionary.md`](docs/data_dictionary.md) | Raw table summaries, staging column mapping |
| [`docs/data_quality_report.md`](docs/data_quality_report.md) | DQ check results, flagged row counts, how to query issues |
| [`docs/schema_design.md`](docs/schema_design.md) | Star schema diagram, column types, common analysis queries |
