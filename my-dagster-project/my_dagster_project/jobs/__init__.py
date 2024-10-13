from dagster import AssetSelection, define_asset_job

from ..partitions import daily_partition

# from ..assets.dbt import dbt_analytics
# from dagster_dbt import build_dbt_asset_selection

source_posts_api = AssetSelection.groups("abc_api")
# adhoc_request = AssetSelection.assets("adhoc_request")

# dbt_trips_selection = build_dbt_asset_selection([dbt_analytics], "stg_trips+")
#  .downstream() to include all downstream assets, to
# dbt_trips_selection = build_dbt_asset_selection([dbt_analytics], "stg_trips+").downstream()

# trip_update_job = define_asset_job(
#     name="trip_update_job",
#     partitions_def=monthly_partition,
#     selection=AssetSelection.all() - trips_by_week - adhoc_request - dbt_trips_selection,
# )

# weekly_update_job = define_asset_job(
#     name="weekly_update_job", partitions_def=weekly_partition, selection=trips_by_week
# )

# adhoc_request_job = define_asset_job(name="adhoc_request_job", selection=adhoc_request)

daily_pull_api_job = define_asset_job(
    name="daily_pull_api_from_source_job",
    selection=source_posts_api,
    partitions_def=daily_partition,
)
