from dagster import Definitions, load_assets_from_modules, AssetSpec
from .assets import dbt
from .assets.ABC_api import post, comments, eod_tables
from .jobs.t24_parsing.t24_account import t24_topic_parsing_job
# from .jobs.t24_parsing import t24_account
# from my_dagster_project import assets
from .jobs import daily_pull_api_job, engine_x_jobs
# from.jobs.t24_parsing.t24_account import t24_parsing_job
from .resources import dbt_resource
from .partitions import daily_partition

# resources = {
#     "dbt": DbtCliClientResource(
#         project_dir=DBT_PROJECT_PATH,
#         profiles_dir=DBT_PROFILES,
#     ),
# }

dbt_analytics_assets = load_assets_from_modules(
    modules=[dbt]
)  # Load the assets from the file

dagster_assets = load_assets_from_modules(modules=[post, comments, eod_tables])

# eod_assets = [AssetSpec("t24_account__s2", group_name="eod", partitions_def=daily_partition, owners=["team:thanhtt@vpbank.com.vn"], description="Bảng t24 account chứa thông tin địa lý")]

all_jobs = [daily_pull_api_job, engine_x_jobs, t24_topic_parsing_job]

defs = Definitions(
    assets=dbt_analytics_assets + dagster_assets,
    resources={
        # "database": database_resource,
        "dbt": dbt_resource  # register your dbt resource with the code location
    },
    jobs=all_jobs,
    # schedules=all_schedules,
    # sensors=all_sensors,
)
