from dagster import asset, get_dagster_logger, AssetExecutionContext, Jitter,Backoff, RetryPolicy
import requests
import pyiceberg
import pyarrow as pa
from pyiceberg.catalog import load_catalog
from ...partitions import daily_partition
from datetime import datetime
# from pyiceberg.partitioning import PartitionSpec, PartitionField
# from pyiceberg.transforms import DayTransform
import pyiceberg.expressions as E
import random


@asset(
    partitions_def=daily_partition,
    group_name="eod",
    metadata={"dataset_name": "raw", "dagster/relation_identifier": "raw.t24_account__s2"},
    compute_kind="python",
    owners=["team:thanhtt@vpbank.com.vn"],
    # automation_condition=AutomationCondition.cron_tick_passed("@daily")
    retry_policy=RetryPolicy(
        max_retries=10,
        delay=1,  # 200ms
        backoff=Backoff.LINEAR,
        jitter=Jitter.PLUS_MINUS,
    )
)
def t24_account__s2(context: AssetExecutionContext):
    logger = get_dagster_logger()
    partition_date_str = context.partition_key
    partition_date = datetime.strptime(partition_date_str, "%Y-%m-%d").date()
    # checksource eod

    if random.choice([True, False]):
        raise Exception("EOD t24_account__s2 not ready")
    else:
        logger.info("EOD t24_account__s2 succeed")