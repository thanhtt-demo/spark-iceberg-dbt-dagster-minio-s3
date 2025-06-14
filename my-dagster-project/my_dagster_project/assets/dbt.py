from dagster import AssetExecutionContext, AssetKey
from dagster_dbt import dbt_assets, DbtCliResource, DagsterDbtTranslator

from ..project import dbt_project
from ..partitions import daily_partition
import json
from datetime import timedelta

INCREMENTAL_SELECTOR = "config.materialized:incremental"


class CustomizedDagsterDbtTranslator(DagsterDbtTranslator):
    def get_asset_key(self, dbt_resource_props):
        resource_type = dbt_resource_props["resource_type"]
        name = dbt_resource_props["name"]
        if resource_type == "source":
            return AssetKey(f"{name}")
        else:
            return super().get_asset_key(dbt_resource_props)

    def get_group_name(self, dbt_resource_props):
        return dbt_resource_props["fqn"][1]


@dbt_assets(
    manifest=dbt_project.manifest_path,
    dagster_dbt_translator=CustomizedDagsterDbtTranslator(),
    exclude=INCREMENTAL_SELECTOR,  # Add this here
    partitions_def=daily_partition,  # partition those models using daily_partition
)
def dbt_demo(context: AssetExecutionContext, dbt: DbtCliResource):
    time_window = context.partition_time_window
    dbt_vars = {
        "partition_date": time_window.start.strftime("%Y-%m-%d"),
        "max_date": (time_window.end - timedelta(days=1)).strftime("%Y-%m-%d"),
    }
    yield from dbt.cli(
        [
            "build",
            "--exclude-resource-types",
            "unit_test",
            "--vars",
            json.dumps(dbt_vars),
        ],
        context=context,
    ).stream()


# Đối với dbt moel using incremental strategy, pass var as partition date to dbt model
@dbt_assets(
    manifest=dbt_project.manifest_path,
    dagster_dbt_translator=CustomizedDagsterDbtTranslator(),
    select=INCREMENTAL_SELECTOR,  # select only models with INCREMENTAL_SELECTOR
    partitions_def=daily_partition,  # partition those models using daily_partition
)
def incremental_dbt_models(context: AssetExecutionContext, dbt: DbtCliResource):
    time_window = context.partition_time_window
    dbt_vars = {
        "partition_date": time_window.start.strftime("%Y-%m-%d"),
        "max_date": (time_window.end - timedelta(days=1)).strftime("%Y-%m-%d"),
    }
    yield from dbt.cli(
        [
            "build",
            "--exclude-resource-types",
            "unit_test",
            "--vars",
            json.dumps(dbt_vars),
        ],
        context=context,
    ).stream()
