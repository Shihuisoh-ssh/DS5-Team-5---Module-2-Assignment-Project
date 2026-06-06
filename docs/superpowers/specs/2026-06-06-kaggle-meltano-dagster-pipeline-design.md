# Kaggle → Meltano → BigQuery → dbt Pipeline Design

**Date:** 2026-06-06
**Project:** DS5 Team 5 — Module 2 Assignment
**Status:** Approved

---

## Goal

Automate the existing manual pipeline (Kaggle download → bq load → dbt run) using Dagster as
the orchestrator, Meltano as the CSV-to-BigQuery loader, and a daily Dagster schedule.

---

## Architecture

```
[Kaggle API]
     ↓  Asset 1: kaggle_dataset_downloaded
     ↓  kaggle datasets download -d olistbr/brazilian-ecommerce --unzip --force

[data/raw/*.csv]  (9 Olist CSVs, overwritten each run)
     ↓  Asset 2: meltano_loaded_to_bigquery
     ↓  meltano run tap-spreadsheets-anywhere target-bigquery

[BigQuery: our-project-93971.kaggle_data]  (9 tables, overwritten each run)
     ↓  Asset 3: dbt_models_built
     ↓  dbt run  (24 models: staging → data_quality → star schema)

[BigQuery: olist_dev_staging / olist_dev_data_quality / olist_dev_star]
     ↓  Asset 4: dbt_tests_passed
     ↓  dbt test  (53 tests)

[Analysis-ready BigQuery tables]
     ↑
[Dagster Schedule: daily at 23:55 SGT (15:55 UTC) — cron: "55 15 * * *"]
```

**Overwrite + loading_date strategy:** Each run truncates and reloads all 9 BigQuery tables in
`kaggle_data`. Meltano `add_record_metadata: true` automatically adds `_sdc_received_at` (UTC
timestamp of load) to every row. dbt staging models alias this as `loading_date`. No
staging/MERGE needed.

---

## Services Used (all free)

| Service | Role | Cost |
|---|---|---|
| Kaggle API | Download dataset | Free (account required) |
| Meltano OSS | CSV → BigQuery EL | Free (self-hosted) |
| Dagster OSS | Orchestration + scheduling | Free (self-hosted, `dagster dev`) |
| GCP BigQuery | Data warehouse | Free tier: 10 GB storage, 1 TB queries/month |
| dbt Core | Transformation | Free (already set up) |

---

## Folder Structure

New files added at project root. Existing `our_project/` (dbt) and
`module2-olist-data-pipeline/` (data + docs) are unchanged.

```
DS5-Team-5---Module-2-Assignment-Project/
│
├── meltano/
│   └── meltano.yml              ← Meltano config: 9 CSV sources + BigQuery target
│
├── dagster_project/
│   ├── __init__.py
│   ├── assets.py                ← 4 Dagster software-defined assets
│   └── definitions.py           ← Dagster entry point + daily schedule
│
├── requirements.txt             ← Root-level: dagster, dagster-webserver, meltano, kaggle, python-dotenv
├── .env                         ← Secrets (gitignored) — user must fill in blanks
└── .env.example                 ← Updated with KAGGLE_USERNAME, KAGGLE_KEY, DATA_RAW_PATH
```

---

## Meltano Configuration

**Extractor:** `tap-spreadsheets-anywhere` (variant: ets)
- Reads all 9 local CSV files from `${DATA_RAW_PATH}`
- One entry per table in `meltano.yml`

**Loader:** `target-bigquery` (variant: meltanolabs)
- Writes to `our-project-93971.kaggle_data`
- `load_method: overwrite` — truncates and reloads each table on every run
- `add_record_metadata: true` — adds `_sdc_received_at` (UTC load timestamp) to every row
- Auth: service account JSON via `${GCP_KEYFILE_PATH}`, or GCP ADC if left blank

**9 table configs:**

| Stream name | CSV file | Key properties |
|---|---|---|
| customers | olist_customers_dataset.csv | customer_id |
| orders | olist_orders_dataset.csv | order_id |
| order_items | olist_order_items_dataset.csv | order_id, order_item_id |
| order_payments | olist_order_payments_dataset.csv | order_id, payment_sequential |
| order_reviews | olist_order_reviews_dataset.csv | review_id |
| products | olist_products_dataset.csv | product_id |
| sellers | olist_sellers_dataset.csv | seller_id |
| geolocation | olist_geolocation_dataset.csv | (none — no natural PK) |
| category_name_translation | product_category_name_translation.csv | product_category_name |

---

## Dagster Assets

| Asset | Depends on | Action |
|---|---|---|
| `kaggle_dataset_downloaded` | — | `kaggle datasets download -d olistbr/brazilian-ecommerce -p data/raw --unzip --force` |
| `meltano_loaded_to_bigquery` | `kaggle_dataset_downloaded` | `meltano run tap-spreadsheets-anywhere target-bigquery` |
| `dbt_models_built` | `meltano_loaded_to_bigquery` | `dbt run` in `our_project/` |
| `dbt_tests_passed` | `dbt_models_built` | `dbt test` in `our_project/` |

All assets use `subprocess.run` with `check=True` so any non-zero exit code raises an
exception and stops the pipeline with a visible error in the Dagster UI.

---

## loading_date Column

Meltano's `add_record_metadata: true` adds `_sdc_received_at` (UTC timestamp) to every row at
load time. All 9 dbt staging models alias this column:

```sql
_sdc_received_at AS loading_date
```

This gives every row an audit timestamp showing exactly when it was loaded into BigQuery.
Because the strategy is overwrite, all rows in a given run share the same `loading_date`.
Querying `SELECT MAX(loading_date) FROM kaggle_data.orders` tells you the last successful run.

---

## Schedule

| Setting | Value |
|---|---|
| Frequency | Daily |
| Local time | 23:55 SGT (GMT+8) |
| UTC cron | `55 15 * * *` |
| Targets | All 4 assets (full pipeline run) |

Can be changed to weekly (`55 15 * * 0`) or any other cadence by editing one line in
`definitions.py`. Manual runs are always available from the Dagster UI.

---

## Environment Variables (`.env`)

| Variable | Value | How to get |
|---|---|---|
| `GCP_PROJECT_ID` | `our-project-93971` | Already known |
| `GCP_KEYFILE_PATH` | `/path/to/service-account.json` | GCP Console → IAM → Service Accounts → Keys, OR leave blank for ADC |
| `DBT_DATASET` | `olist_dev` | Already set |
| `KAGGLE_USERNAME` | your Kaggle username | kaggle.com → avatar → Settings → API |
| `KAGGLE_KEY` | your Kaggle API key | Same page → "Create New Token" → copy `key` from downloaded `kaggle.json` |
| `DATA_RAW_PATH` | absolute path to `module2-olist-data-pipeline/data/raw` | Set to your local path |

---

## How to Run

```bash
# 1. Install dependencies
pip install -r requirements.txt

# 2. Install Meltano plugins (first time only)
cd meltano
meltano install

# 3. Start Dagster UI (runs on http://localhost:3000)
dagster dev -f dagster_project/definitions.py

# 4. Trigger manually from UI, or wait for daily schedule at 23:55 SGT
```

---

## Out of Scope

- Staging + MERGE pattern (overwrite is sufficient for this dataset)
- Intermediate validation assets (dbt's 53 tests are the quality gate)
- Docker / containerisation
- Cloud-hosted Dagster (Dagster+) — self-hosted is free and sufficient
