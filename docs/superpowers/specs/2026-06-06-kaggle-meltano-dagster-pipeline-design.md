# Kaggle → Meltano → BigQuery → dbt Pipeline Design

**Date:** 2026-06-06
**Project:** DS5 Team 5 — Module 2 Assignment
**Status:** Approved (Option A — mixed incremental + full-table)

---

## Goal

Automate the existing manual pipeline (Kaggle download → bq load → dbt run) using Dagster as
the orchestrator, Meltano as the CSV-to-BigQuery loader, and a daily Dagster schedule.
Use Singer-style incremental replication (state bookmarks) for the 3 tables that have
timestamp columns; full-table overwrite for the 6 that do not.

---

## Architecture

```
[Kaggle API]
     ↓  Asset 1: kaggle_dataset_downloaded
     ↓  kaggle datasets download -d olistbr/brazilian-ecommerce --unzip --force

[data/raw/*.csv]  (9 Olist CSVs, refreshed each run)
     ├──────────────────────────────────────────────────────┐
     ↓  Asset 2: meltano_full_tables_loaded                 ↓  Asset 3: meltano_incremental_tables_loaded
     ↓  tap-spreadsheets-anywhere → target-bigquery         ↓  tap-spreadsheets-anywhere--incremental
     ↓  FULL_TABLE + overwrite (6 tables)                   ↓  → target-bigquery--incremental
     ↓                                                      ↓  INCREMENTAL + append-only (3 tables)
     └──────────────────────────────────────────────────────┘
                              ↓  (both must complete)
                     Asset 4: dbt_models_built
                     ↓  dbt run  (24 models)

     [BigQuery: olist_dev_staging / olist_dev_data_quality / olist_dev_star]
                              ↓
                     Asset 5: dbt_tests_passed
                     ↓  dbt test  (53 tests)

                     [Analysis-ready BigQuery tables]
                              ↑
     [Dagster Schedule: daily 23:55 SGT — cron "55 23 * * *" Asia/Singapore]
```

Assets 2 and 3 run in parallel after Asset 1 completes.

---

## Replication Strategy

### Incremental tables (3) — `INCREMENTAL` + `append-only`

Meltano saves a state bookmark after each run. Next run loads only rows
where `replication_key > last_state_value`. BigQuery table grows over time (new rows only).

| Table | Replication key |
|---|---|
| `orders` | `order_purchase_timestamp` |
| `order_items` | `shipping_limit_date` |
| `order_reviews` | `review_creation_date` |

Check state with:
```
meltano --environment=dev state get dev:tap-spreadsheets-anywhere--incremental-to-target-bigquery--incremental
```

### Full-table tables (6) — `FULL_TABLE` + `overwrite`

No timestamp column available. Each run truncates and reloads completely.

| Table | Reason for full-table |
|---|---|
| `customers` | No timestamp column |
| `order_payments` | No timestamp column |
| `products` | No timestamp column |
| `sellers` | No timestamp column |
| `geolocation` | No timestamp column |
| `category_name_translation` | No timestamp column |

---

## loading_date Column

Both pipelines use `add_record_metadata: true` in `target-bigquery`.
This adds `_sdc_received_at` (UTC TIMESTAMP of load time) to every row in BigQuery.

All 9 dbt staging models alias it:
```sql
_sdc_received_at AS loading_date
```

For full-table tables: all rows share the same `loading_date` per run.
For incremental tables: only newly loaded rows get a new `loading_date`; old rows keep their original.

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

New files at project root. Existing `our_project/` (dbt) and
`module2-olist-data-pipeline/` (data + docs) are unchanged.

```
DS5-Team-5---Module-2-Assignment-Project/
│
├── meltano/
│   ├── meltano.yml              ← 2 extractors + 2 loaders
│   └── .gitignore               ← excludes .meltano/ plugin dir
│
├── dagster_project/
│   ├── __init__.py
│   ├── assets.py                ← 5 Dagster software-defined assets
│   └── definitions.py           ← entry point + daily schedule
│
├── requirements.txt             ← dagster, dagster-webserver, meltano, kaggle, python-dotenv
├── .env                         ← secrets (gitignored) ← FILL IN KAGGLE_USERNAME + KAGGLE_KEY
└── .env.example                 ← updated reference copy
```

---

## Meltano Plugins

### Extractor 1: `tap-spreadsheets-anywhere` (full-table, 6 tables)
- Variant: `ets`, pip: `tap-spreadsheets-anywhere`
- 6 streams: customers, order_payments, products, sellers, geolocation, category_name_translation
- No `replication_method` set → defaults to `FULL_TABLE`

### Extractor 2: `tap-spreadsheets-anywhere--incremental` (3 tables)
- Same pip install, separate config block
- 3 streams with `replication_method: INCREMENTAL` + `replication_key`

### Loader 1: `target-bigquery` (overwrite)
- Variant: `meltanolabs`, pip: `meltanolabs-target-bigquery`
- `load_method: overwrite`, `add_record_metadata: true`

### Loader 2: `target-bigquery--incremental` (append-only)
- Same pip install, separate config block
- `load_method: append-only`, `add_record_metadata: true`

---

## Dagster Assets

| Asset | Depends on | Action |
|---|---|---|
| `kaggle_dataset_downloaded` | — | `kaggle datasets download -d olistbr/brazilian-ecommerce --unzip --force` |
| `meltano_full_tables_loaded` | `kaggle_dataset_downloaded` | `meltano run tap-spreadsheets-anywhere target-bigquery` |
| `meltano_incremental_tables_loaded` | `kaggle_dataset_downloaded` | `meltano run tap-spreadsheets-anywhere--incremental target-bigquery--incremental` |
| `dbt_models_built` | both meltano assets | `dbt run` |
| `dbt_tests_passed` | `dbt_models_built` | `dbt test` |

Assets 2 + 3 run in parallel. All use `check=True` so failures halt the pipeline visibly.

---

## Schedule

| Setting | Value |
|---|---|
| Frequency | Daily |
| Local time | 23:55 SGT |
| Cron | `55 23 * * *` |
| Timezone | `Asia/Singapore` |

---

## Environment Variables (`.env`)

| Variable | Pre-filled? | Action needed |
|---|---|---|
| `GCP_PROJECT_ID` | ✅ `our-project-93971` | None |
| `GCP_KEYFILE_PATH` | ✅ blank (uses ADC) | Fill in if using service account |
| `DBT_DATASET` | ✅ `olist_dev` | None |
| `KAGGLE_USERNAME` | ❌ | Fill from `kaggle.json` |
| `KAGGLE_KEY` | ❌ | Fill from `kaggle.json` |
| `KAGGLE_DATASET` | ✅ `olistbr/brazilian-ecommerce` | None |
| `DATA_RAW_PATH` | ✅ pre-filled | None |

---

## Known Limitation

dbt staging models do a simple `SELECT *` from BigQuery source tables. For the 3 incremental
tables, if a dataset ever ships with duplicate primary keys across re-runs (e.g., a corrected
order row), staging would contain both old and new versions. Deduplication
(`QUALIFY ROW_NUMBER() ... = 1`) would be needed in staging for live production datasets.
For the static Olist dataset this is not an issue.

---

## How to Run

```bash
# 1. Install dependencies (once)
pip install -r requirements.txt

# 2. Install Meltano plugins (once, from meltano/ dir)
cd meltano && meltano install && cd ..

# 3. Start Dagster UI (http://localhost:3000)
dagster dev -f dagster_project/definitions.py

# 4. Trigger manually from UI, or wait for daily schedule at 23:55 SGT
```

---

## Out of Scope

- Staging + MERGE deduplication (Olist is static)
- Docker / containerisation
- Cloud-hosted Dagster (Dagster+)
