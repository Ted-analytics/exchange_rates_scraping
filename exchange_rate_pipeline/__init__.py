from dagster import Definitions, load_assets_from_modules, ScheduleDefinition, define_asset_job, EnvVar, AssetSelection
from exchange_rate_pipeline.resources import ClickHouseResource
from exchange_rate_pipeline import assets

# Load all assets
all_assets = load_assets_from_modules([assets])

# Define the job that will run all daily assets (excluding monthly refresh)
exchange_rates_job = define_asset_job(
    name="exchange_rates_job",
    selection=[
        assets.raw_exchange_rates,
        assets.processed_exchange_rates,
        assets.exchange_rates_clickhouse
    ]
)

# Define a separate job for the monthly refresh
monthly_exchange_rates_job = define_asset_job(
    name="monthly_exchange_rates_job",
    selection=AssetSelection.assets(assets.refresh_monthly_exchange_rates)
)

# Create a daily schedule
exchange_rates_schedule = ScheduleDefinition(
    name="daily_exchange_rates",
    cron_schedule="0 9 * * *",  # Runs at 9:00 AM every day
    job=exchange_rates_job,
    execution_timezone="Africa/Kigali",  # Adjust to your timezone
)

# Create a monthly schedule that runs on the 1st day of each month
monthly_exchange_rates_schedule = ScheduleDefinition(
    name="monthly_exchange_rates",
    cron_schedule="0 1 1 * *",  # Runs at 1:00 AM on the 1st day of every month
    job=monthly_exchange_rates_job,
    execution_timezone="Africa/Kigali",  # Adjust to your timezone
)

# Define all pipeline components
defs = Definitions(
    assets=all_assets,
    schedules=[exchange_rates_schedule, monthly_exchange_rates_schedule],
    resources={
        "clickhouse": ClickHouseResource(
            host=EnvVar("CLICKHOUSE_HOST").get_value(),
            port=int(EnvVar("CLICKHOUSE_PORT").get_value()),  # Convert string to int
            database=EnvVar("CLICKHOUSE_DB").get_value(),
            user=EnvVar("CLICKHOUSE_USER").get_value(),
            password=EnvVar("CLICKHOUSE_PASSWORD").get_value()
        )
    }
)