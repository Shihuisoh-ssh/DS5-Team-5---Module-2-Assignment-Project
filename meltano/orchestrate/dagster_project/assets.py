import os
import subprocess
import sys
from pathlib import Path

from dagster import AssetExecutionContext, asset
from dotenv import load_dotenv

MELTANO_DIR = Path(__file__).parent.parent.parent  # dagster_project → orchestrate → meltano
PROJECT_ROOT = MELTANO_DIR.parent                  # repo root (for .env)
DBT_DIR = MELTANO_DIR / "transform" / "our_project"

load_dotenv(PROJECT_ROOT / ".env")

# Absolute paths to binaries so subprocesses work regardless of active conda env
_ENV_BIN = Path(sys.executable).parent
KAGGLE_BIN = str(_ENV_BIN / "kaggle")
MELTANO_BIN = str(_ENV_BIN / "meltano")
DBT_BIN = str(_ENV_BIN / "dbt")


@asset
def kaggle_dataset_downloaded(context: AssetExecutionContext):
    data_raw = Path(os.environ["DATA_RAW_PATH"])
    data_raw.mkdir(parents=True, exist_ok=True)
    result = subprocess.run(
        [
            KAGGLE_BIN, "datasets", "download",
            "-d", os.environ.get("KAGGLE_DATASET", "olistbr/brazilian-ecommerce"),
            "-p", str(data_raw),
            "--unzip", "--force",
        ],
        capture_output=True, text=True, check=True,
    )
    context.log.info(result.stdout or "Kaggle download complete")


@asset(deps=[kaggle_dataset_downloaded])
def meltano_loaded_to_bigquery(context: AssetExecutionContext):
    """Loads all 9 Olist CSVs into BigQuery kaggle_data dataset (overwrite)."""
    result = subprocess.run(
        [MELTANO_BIN, "run", "tap-csv", "target-bigquery"],
        cwd=str(MELTANO_DIR),
        capture_output=True, text=True, check=True,
        env={**os.environ},
    )
    context.log.info(result.stdout or "Meltano load complete")


@asset(deps=[meltano_loaded_to_bigquery])
def dbt_models_built(context: AssetExecutionContext):
    result = subprocess.run(
        [DBT_BIN, "run"],
        cwd=str(DBT_DIR),
        capture_output=True, text=True, check=True,
    )
    context.log.info(result.stdout)


@asset(deps=[dbt_models_built])
def dbt_tests_passed(context: AssetExecutionContext):
    result = subprocess.run(
        [DBT_BIN, "test"],
        cwd=str(DBT_DIR),
        capture_output=True, text=True, check=True,
    )
    context.log.info(result.stdout)
