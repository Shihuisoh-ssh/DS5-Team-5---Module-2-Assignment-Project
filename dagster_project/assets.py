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
