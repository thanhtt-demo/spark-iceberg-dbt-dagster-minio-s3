from dagster import Definitions, load_assets_from_modules
from .assets import dbt
from .assets.ABC_api import post, comments
# from my_dagster_project import assets
from .jobs import daily_pull_api_job
from .resources import dbt_resource

# resources = {
#     "dbt": DbtCliClientResource(
#         project_dir=DBT_PROJECT_PATH,
#         profiles_dir=DBT_PROFILES,
#     ),
# }

dbt_analytics_assets = load_assets_from_modules(
    modules=[dbt]
)  # Load the assets from the file

dagster_assets = load_assets_from_modules(modules=[post, comments])
all_jobs = [daily_pull_api_job]

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
