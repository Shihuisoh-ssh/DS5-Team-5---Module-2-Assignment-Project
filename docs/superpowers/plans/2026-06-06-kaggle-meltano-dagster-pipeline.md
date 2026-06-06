# Kaggle → Meltano → BigQuery → dbt Pipeline Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Wire up Dagster + Meltano to automate Kaggle CSV download → BigQuery load → dbt transform on a daily schedule.

**Architecture:** Dagster orchestrates 5 assets. Two parallel Meltano runs handle ingestion — one full-table (overwrite, 6 tables without timestamps), one incremental (append-only, 3 tables with timestamps). dbt then builds and tests all 24 models.

**Tech Stack:** Dagster OSS, Meltano (tap-spreadsheets-anywhere + target-bigquery), kaggle CLI, dbt-bigquery, python-dotenv

---

## File Map

| File | Action | Purpose |
|---|---|---|
| `requirements.txt` | CREATE | Root-level Python deps |
| `.gitignore` | MODIFY | Add `.meltano/` |
| `.env.example` | MODIFY | Add new vars |
| `meltano/meltano.yml` | CREATE | 2 extractors + 2 loaders |
| `meltano/.gitignore` | CREATE | Exclude `.meltano/` plugin dir |
| `dagster_project/__init__.py` | CREATE | Package marker |
| `dagster_project/assets.py` | CREATE | 5 Dagster assets |
| `dagster_project/definitions.py` | CREATE | Entry point + schedule |
| `our_project/models/staging/stg_orders.sql` | MODIFY | Add `loading_date` |
| `our_project/models/staging/stg_customers.sql` | MODIFY | Add `loading_date` |
| `our_project/models/staging/stg_order_items.sql` | MODIFY | Add `loading_date` |
| `our_project/models/staging/stg_order_payments.sql` | MODIFY | Add `loading_date` |
| `our_project/models/staging/stg_order_reviews.sql` | MODIFY | Add `loading_date` |
| `our_project/models/staging/stg_products.sql` | MODIFY | Add `loading_date` |
| `our_project/models/staging/stg_sellers.sql` | MODIFY | Add `loading_date` |
| `our_project/models/staging/stg_geolocation.sql` | MODIFY | Add `loading_date` |
| `our_project/models/staging/stg_category_name_translation.sql` | MODIFY | Add `loading_date` |
| `our_project/models/staging/schema.yml` | MODIFY | Add `loading_date` column entry to all 9 models |

---

## Task 1: Root requirements.txt and .gitignore

**Files:**
- Create: `requirements.txt`
- Modify: `.gitignore`

- [ ] **Step 1: Create `requirements.txt` at project root**

```
dagster
dagster-webserver
meltano
kaggle
python-dotenv
```

Full path: `DS5-Team-5---Module-2-Assignment-Project/requirements.txt`

- [ ] **Step 2: Add `.meltano/` to root `.gitignore`**

Open `.gitignore` and add after the `# dbt` block:

```
# Meltano
meltano/.meltano/
```

- [ ] **Step 3: Verify install works**

```bash
pip install -r requirements.txt
```

Expected: all 5 packages install without error. Run `dagster --version` and `meltano --version` to confirm.

- [ ] **Step 4: Commit**

```bash
git add requirements.txt .gitignore
git commit -m "chore: add pipeline requirements and gitignore meltano artifacts"
```

---

## Task 2: Update .env.example

**Files:**
- Modify: `.env.example`

- [ ] **Step 1: Replace `.env.example` with updated content**

```
# Copy this file to .env and fill in your own values.
# .env is gitignored — never commit it.

# GCP project where BigQuery data lives
GCP_PROJECT_ID=your-gcp-project-id

# Path to GCP service account JSON keyfile.
# Leave blank to use Application Default Credentials (gcloud auth application-default login).
GCP_KEYFILE_PATH=

# dbt output dataset prefix
DBT_DATASET=olist_dev

# Kaggle API — Legacy credentials
# kaggle.com → avatar → Settings → API Tokens → Create Legacy API Key
# Open downloaded kaggle.json and copy username + key below.
KAGGLE_USERNAME=your-kaggle-username
KAGGLE_KEY=your-kaggle-api-key

# Kaggle dataset slug — do not change unless switching datasets
KAGGLE_DATASET=olistbr/brazilian-ecommerce

# Absolute path to the folder containing the 9 Olist CSVs (no trailing slash)
DATA_RAW_PATH=/absolute/path/to/module2-olist-data-pipeline/data/raw
```

- [ ] **Step 2: Commit**

```bash
git add .env.example
git commit -m "chore: update .env.example with Kaggle and Meltano vars"
```

---

## Task 3: Meltano configuration

**Files:**
- Create: `meltano/meltano.yml`
- Create: `meltano/.gitignore`

- [ ] **Step 1: Create `meltano/` directory and `.gitignore`**

Create `meltano/.gitignore` with:

```
.meltano/
```

- [ ] **Step 2: Create `meltano/meltano.yml`**

```yaml
version: 1
default_environment: dev
project_id: a3b4c5d6-e7f8-9012-abcd-ef3456789012

environments:
  - name: dev

plugins:
  extractors:

    # ── Full-table extractor (6 tables without timestamp columns) ──────────
    - name: tap-spreadsheets-anywhere
      variant: ets
      pip_url: tap-spreadsheets-anywhere
      config:
        tables:
          - path: ${DATA_RAW_PATH}
            name: customers
            pattern: "olist_customers_dataset\\.csv"
            start_date: "2000-01-01T00:00:00Z"
            key_properties: [customer_id]
            format: csv

          - path: ${DATA_RAW_PATH}
            name: order_payments
            pattern: "olist_order_payments_dataset\\.csv"
            start_date: "2000-01-01T00:00:00Z"
            key_properties: [order_id, payment_sequential]
            format: csv

          - path: ${DATA_RAW_PATH}
            name: products
            pattern: "olist_products_dataset\\.csv"
            start_date: "2000-01-01T00:00:00Z"
            key_properties: [product_id]
            format: csv

          - path: ${DATA_RAW_PATH}
            name: sellers
            pattern: "olist_sellers_dataset\\.csv"
            start_date: "2000-01-01T00:00:00Z"
            key_properties: [seller_id]
            format: csv

          - path: ${DATA_RAW_PATH}
            name: geolocation
            pattern: "olist_geolocation_dataset\\.csv"
            start_date: "2000-01-01T00:00:00Z"
            key_properties: []
            format: csv

          - path: ${DATA_RAW_PATH}
            name: category_name_translation
            pattern: "product_category_name_translation\\.csv"
            start_date: "2000-01-01T00:00:00Z"
            key_properties: [product_category_name]
            format: csv

    # ── Incremental extractor (3 tables with timestamp columns) ───────────
    - name: tap-spreadsheets-anywhere--incremental
      variant: ets
      pip_url: tap-spreadsheets-anywhere
      config:
        tables:
          - path: ${DATA_RAW_PATH}
            name: orders
            pattern: "olist_orders_dataset\\.csv"
            start_date: "2016-01-01T00:00:00Z"
            key_properties: [order_id]
            replication_method: INCREMENTAL
            replication_key: order_purchase_timestamp
            format: csv

          - path: ${DATA_RAW_PATH}
            name: order_items
            pattern: "olist_order_items_dataset\\.csv"
            start_date: "2016-01-01T00:00:00Z"
            key_properties: [order_id, order_item_id]
            replication_method: INCREMENTAL
            replication_key: shipping_limit_date
            format: csv

          - path: ${DATA_RAW_PATH}
            name: order_reviews
            pattern: "olist_order_reviews_dataset\\.csv"
            start_date: "2016-01-01T00:00:00Z"
            key_properties: [review_id]
            replication_method: INCREMENTAL
            replication_key: review_creation_date
            format: csv

  loaders:

    # ── Full-table loader (overwrite) ──────────────────────────────────────
    - name: target-bigquery
      variant: meltanolabs
      pip_url: meltanolabs-target-bigquery
      config:
        project_id: ${GCP_PROJECT_ID}
        dataset_id: kaggle_data
        location: US
        add_record_metadata: true
        load_method: overwrite
        credentials_path: ${GCP_KEYFILE_PATH}

    # ── Incremental loader (append-only) ──────────────────────────────────
    - name: target-bigquery--incremental
      variant: meltanolabs
      pip_url: meltanolabs-target-bigquery
      config:
        project_id: ${GCP_PROJECT_ID}
        dataset_id: kaggle_data
        location: US
        add_record_metadata: true
        load_method: append-only
        credentials_path: ${GCP_KEYFILE_PATH}
```

- [ ] **Step 3: Install Meltano plugins**

```bash
cd meltano
meltano install
```

Expected: Meltano downloads and installs both tap and both target variants into `.meltano/`. Takes 1-3 minutes.

- [ ] **Step 4: Verify Meltano can see the plugins**

```bash
meltano --environment=dev invoke tap-spreadsheets-anywhere --version
meltano --environment=dev invoke target-bigquery --version
```

Expected: version strings printed for both, no errors.

- [ ] **Step 5: Commit**

```bash
cd ..
git add meltano/meltano.yml meltano/.gitignore
git commit -m "feat: add meltano config — full-table and incremental pipelines"
```

---

## Task 4: Dagster project

**Files:**
- Create: `dagster_project/__init__.py`
- Create: `dagster_project/assets.py`
- Create: `dagster_project/definitions.py`

- [ ] **Step 1: Create `dagster_project/__init__.py`** (empty file)

```python
```

- [ ] **Step 2: Create `dagster_project/assets.py`**

```python
import os
import subprocess
from pathlib import Path

from dagster import AssetExecutionContext, asset
from dotenv import load_dotenv

PROJECT_ROOT = Path(__file__).parent.parent
MELTANO_DIR = PROJECT_ROOT / "meltano"
DBT_DIR = PROJECT_ROOT / "our_project"

load_dotenv(PROJECT_ROOT / ".env")


@asset
def kaggle_dataset_downloaded(context: AssetExecutionContext):
    data_raw = Path(os.environ["DATA_RAW_PATH"])
    data_raw.mkdir(parents=True, exist_ok=True)
    result = subprocess.run(
        [
            "kaggle", "datasets", "download",
            "-d", os.environ.get("KAGGLE_DATASET", "olistbr/brazilian-ecommerce"),
            "-p", str(data_raw),
            "--unzip", "--force",
        ],
        capture_output=True,
        text=True,
        check=True,
    )
    context.log.info(result.stdout or "Kaggle download complete")


@asset(deps=[kaggle_dataset_downloaded])
def meltano_full_tables_loaded(context: AssetExecutionContext):
    """FULL_TABLE + overwrite: customers, order_payments, products, sellers, geolocation, category_name_translation."""
    result = subprocess.run(
        ["meltano", "run", "tap-spreadsheets-anywhere", "target-bigquery"],
        cwd=str(MELTANO_DIR),
        capture_output=True,
        text=True,
        check=True,
        env={**os.environ},
    )
    context.log.info(result.stdout or "Full-table load complete")


@asset(deps=[kaggle_dataset_downloaded])
def meltano_incremental_tables_loaded(context: AssetExecutionContext):
    """INCREMENTAL + append-only: orders, order_items, order_reviews. State bookmark saved each run."""
    result = subprocess.run(
        [
            "meltano", "run",
            "tap-spreadsheets-anywhere--incremental",
            "target-bigquery--incremental",
        ],
        cwd=str(MELTANO_DIR),
        capture_output=True,
        text=True,
        check=True,
        env={**os.environ},
    )
    context.log.info(result.stdout or "Incremental load complete")


@asset(deps=[meltano_full_tables_loaded, meltano_incremental_tables_loaded])
def dbt_models_built(context: AssetExecutionContext):
    result = subprocess.run(
        ["dbt", "run"],
        cwd=str(DBT_DIR),
        capture_output=True,
        text=True,
        check=True,
    )
    context.log.info(result.stdout)


@asset(deps=[dbt_models_built])
def dbt_tests_passed(context: AssetExecutionContext):
    result = subprocess.run(
        ["dbt", "test"],
        cwd=str(DBT_DIR),
        capture_output=True,
        text=True,
        check=True,
    )
    context.log.info(result.stdout)
```

- [ ] **Step 3: Create `dagster_project/definitions.py`**

```python
from dagster import Definitions, ScheduleDefinition, define_asset_job, load_assets_from_modules

from dagster_project import assets

all_assets = load_assets_from_modules([assets])

pipeline_job = define_asset_job(
    name="olist_pipeline_job",
    selection="*",
)

daily_schedule = ScheduleDefinition(
    name="olist_daily_schedule",
    job=pipeline_job,
    cron_schedule="55 23 * * *",
    execution_timezone="Asia/Singapore",
)

defs = Definitions(
    assets=all_assets,
    jobs=[pipeline_job],
    schedules=[daily_schedule],
)
```

- [ ] **Step 4: Verify Dagster loads without error**

```bash
dagster asset list -f dagster_project/definitions.py
```

Expected output (5 assets listed):
```
kaggle_dataset_downloaded
meltano_full_tables_loaded
meltano_incremental_tables_loaded
dbt_models_built
dbt_tests_passed
```

- [ ] **Step 5: Commit**

```bash
git add dagster_project/
git commit -m "feat: add dagster assets and daily schedule (23:55 SGT)"
```

---

## Task 5: Add loading_date to all 9 dbt staging models

**Files:**
- Modify: `our_project/models/staging/stg_orders.sql`
- Modify: `our_project/models/staging/stg_customers.sql`
- Modify: `our_project/models/staging/stg_order_items.sql`
- Modify: `our_project/models/staging/stg_order_payments.sql`
- Modify: `our_project/models/staging/stg_order_reviews.sql`
- Modify: `our_project/models/staging/stg_products.sql`
- Modify: `our_project/models/staging/stg_sellers.sql`
- Modify: `our_project/models/staging/stg_geolocation.sql`
- Modify: `our_project/models/staging/stg_category_name_translation.sql`
- Modify: `our_project/models/staging/schema.yml`

- [ ] **Step 1: Update `stg_orders.sql`**

```sql
SELECT
    order_id,
    customer_id,
    order_status,
    TIMESTAMP(order_purchase_timestamp)      AS purchase_at,
    TIMESTAMP(order_approved_at)             AS approved_at,
    TIMESTAMP(order_delivered_carrier_date)  AS delivered_carrier_at,
    TIMESTAMP(order_delivered_customer_date) AS delivered_customer_at,
    TIMESTAMP(order_estimated_delivery_date) AS estimated_delivery_at,
    _sdc_received_at                         AS loading_date
FROM {{ source('kaggle_data', 'orders') }}
```

- [ ] **Step 2: Update `stg_customers.sql`**

```sql
SELECT
    customer_id,
    customer_unique_id,
    CAST(customer_zip_code_prefix AS STRING) AS zip_code_prefix,
    customer_city                            AS city,
    customer_state                           AS state,
    _sdc_received_at                         AS loading_date
FROM {{ source('kaggle_data', 'customers') }}
```

- [ ] **Step 3: Update `stg_order_items.sql`**

```sql
SELECT
    order_id,
    order_item_id,
    product_id,
    seller_id,
    TIMESTAMP(shipping_limit_date) AS shipping_limit_at,
    CAST(price AS FLOAT64)         AS price,
    CAST(freight_value AS FLOAT64) AS freight_value,
    _sdc_received_at               AS loading_date
FROM {{ source('kaggle_data', 'order_items') }}
```

- [ ] **Step 4: Update `stg_order_payments.sql`**

```sql
SELECT
    order_id,
    payment_sequential,
    payment_type,
    payment_installments,
    CAST(payment_value AS FLOAT64) AS payment_value,
    _sdc_received_at               AS loading_date
FROM {{ source('kaggle_data', 'order_payments') }}
```

- [ ] **Step 5: Update `stg_order_reviews.sql`**

```sql
SELECT
    review_id,
    order_id,
    CAST(review_score AS INT64)        AS review_score,
    NULLIF(review_comment_title, '')   AS review_comment_title,
    NULLIF(review_comment_message, '') AS review_comment_message,
    TIMESTAMP(review_creation_date)    AS review_created_at,
    TIMESTAMP(review_answer_timestamp) AS review_answered_at,
    _sdc_received_at                   AS loading_date
FROM {{ source('kaggle_data', 'order_reviews') }}
```

- [ ] **Step 6: Update `stg_products.sql`**

```sql
SELECT
    product_id,
    product_category_name,
    CAST(product_name_lenght AS INT64)        AS name_length,
    CAST(product_description_lenght AS INT64) AS description_length,
    CAST(product_photos_qty AS INT64)         AS photos_qty,
    CAST(product_weight_g AS INT64)           AS weight_g,
    CAST(product_length_cm AS INT64)          AS length_cm,
    CAST(product_height_cm AS INT64)          AS height_cm,
    CAST(product_width_cm AS INT64)           AS width_cm,
    _sdc_received_at                          AS loading_date
FROM {{ source('kaggle_data', 'products') }}
```

- [ ] **Step 7: Update `stg_sellers.sql`**

```sql
SELECT
    seller_id,
    CAST(seller_zip_code_prefix AS STRING) AS zip_code_prefix,
    seller_city                            AS city,
    seller_state                           AS state,
    _sdc_received_at                       AS loading_date
FROM {{ source('kaggle_data', 'sellers') }}
```

- [ ] **Step 8: Update `stg_geolocation.sql`**

```sql
SELECT
    CAST(geolocation_zip_code_prefix AS STRING) AS zip_code_prefix,
    CAST(geolocation_lat AS FLOAT64)            AS lat,
    CAST(geolocation_lng AS FLOAT64)            AS lng,
    geolocation_city                            AS city,
    geolocation_state                           AS state,
    _sdc_received_at                            AS loading_date
FROM {{ source('kaggle_data', 'geolocation') }}
```

- [ ] **Step 9: Update `stg_category_name_translation.sql`**

```sql
SELECT
    product_category_name,
    product_category_name_english AS category_name_english,
    _sdc_received_at              AS loading_date
FROM {{ source('kaggle_data', 'category_name_translation') }}
```

- [ ] **Step 10: Add `loading_date` column entry to `schema.yml` for all 9 models**

Add the following `- name: loading_date` entry under `columns:` in each model block.
Full updated file:

```yaml
version: 2

models:
  - name: stg_orders
    description: "Orders with cast timestamps. One row per order."
    columns:
      - name: order_id
        tests: [not_null, unique]
      - name: customer_id
        tests: [not_null]
      - name: order_status
        tests: [not_null]
      - name: loading_date
        tests: [not_null]

  - name: stg_customers
    description: "Customer details with standardised zip code."
    columns:
      - name: customer_id
        tests: [not_null, unique]
      - name: customer_unique_id
        tests: [not_null]
      - name: loading_date
        tests: [not_null]

  - name: stg_order_items
    description: "Individual line items within orders."
    columns:
      - name: order_id
        tests: [not_null]
      - name: order_item_id
        tests: [not_null]
      - name: product_id
        tests: [not_null]
      - name: seller_id
        tests: [not_null]
      - name: loading_date
        tests: [not_null]

  - name: stg_order_payments
    description: "Payment records per order. One order may have multiple rows."
    columns:
      - name: order_id
        tests: [not_null]
      - name: loading_date
        tests: [not_null]

  - name: stg_order_reviews
    description: "Customer reviews with cast dates and null-standardised comments."
    columns:
      - name: review_id
        tests: [not_null]
      - name: order_id
        tests: [not_null]
      - name: loading_date
        tests: [not_null]

  - name: stg_products
    description: "Product catalogue with cast numeric dimensions."
    columns:
      - name: product_id
        tests: [not_null, unique]
      - name: loading_date
        tests: [not_null]

  - name: stg_sellers
    description: "Seller details with standardised zip code."
    columns:
      - name: seller_id
        tests: [not_null, unique]
      - name: loading_date
        tests: [not_null]

  - name: stg_geolocation
    description: "Geolocation lookup by zip code prefix."
    columns:
      - name: zip_code_prefix
        tests: [not_null]
      - name: loading_date
        tests: [not_null]

  - name: stg_category_name_translation
    description: "Portuguese to English product category name mapping."
    columns:
      - name: product_category_name
        tests: [not_null, unique]
      - name: loading_date
        tests: [not_null]
```

- [ ] **Step 11: Compile dbt to verify SQL syntax**

```bash
cd our_project
dbt compile --select staging
```

Expected: `Done. PASS=9 WARN=0 ERROR=0 SKIP=0 TOTAL=9`
No errors means the SQL references to `_sdc_received_at` are syntactically valid.

Note: `dbt run --select staging` will fail until Meltano has loaded data into BigQuery
(the `_sdc_received_at` column won't exist until after the first Meltano run).

- [ ] **Step 12: Commit**

```bash
cd ..
git add our_project/models/staging/
git commit -m "feat: add loading_date (_sdc_received_at) to all 9 staging models"
```

---

## Task 6: End-to-end smoke test

Run this after completing all tasks above and after filling `.env` with real credentials.

- [ ] **Step 1: Verify `.env` is fully populated**

```bash
grep "FILL_IN" .env
```

Expected: no output (all placeholders replaced).

- [ ] **Step 2: Start Dagster UI**

```bash
dagster dev -f dagster_project/definitions.py
```

Open http://localhost:3000 in browser.
Expected: Dagster UI loads, asset graph shows 5 assets with correct dependencies:
- `kaggle_dataset_downloaded` → `meltano_full_tables_loaded` (parallel with below)
- `kaggle_dataset_downloaded` → `meltano_incremental_tables_loaded`
- both → `dbt_models_built` → `dbt_tests_passed`

- [ ] **Step 3: Trigger a manual run from the UI**

In Dagster UI → Assets → click "Materialize all" → confirm.
Watch the run log for each asset completing in order.

- [ ] **Step 4: Verify data in BigQuery**

```sql
-- Confirm loading_date is populated
SELECT MAX(loading_date) AS last_loaded FROM `our-project-93971.kaggle_data.orders`;

-- Confirm incremental state was saved
-- (run from meltano/ dir)
-- meltano --environment=dev state get dev:tap-spreadsheets-anywhere--incremental-to-target-bigquery--incremental
```

- [ ] **Step 5: Verify dbt still passes all 53 tests**

```bash
cd our_project
dbt test
```

Expected: `53 of 53 PASS` (9 new `loading_date not_null` tests added = 62 total after first Meltano run)

- [ ] **Step 6: Verify schedule is registered**

In Dagster UI → Overview → Schedules.
Expected: `olist_daily_schedule` listed, next run shown as tomorrow 23:55 SGT.

- [ ] **Step 7: Final commit**

```bash
git add docs/superpowers/specs/ docs/superpowers/plans/
git commit -m "docs: update spec and add implementation plan for Meltano+Dagster pipeline"
```
