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
