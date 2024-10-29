from dagster import asset, get_dagster_logger, AssetExecutionContext
# from dagster import AssetSpec, AutomationCondition, asset
import requests
import pyiceberg
import pyarrow as pa
from pyiceberg.catalog import load_catalog
from ...partitions import daily_partition
from datetime import datetime
import pyiceberg.expressions as E

# from pyiceberg.partitioning import PartitionSpec, PartitionField
# from pyiceberg.transforms import DayTransform


def create_pyarrow_table(data, partition_date):
    """
    Create a PyArrow Table from a list of dictionaries and a partition date.

    Args:
        data (list of dict): A list of dictionaries where each dictionary represents a row of data.
            Each dictionary should contain the keys "userId", "id", "title", and "body".
        partition_date (str): A string representing the partition date to be added as a column.

    Returns:
        pyarrow.Table: A PyArrow Table constructed from the provided data and partition date.
    """
    # Define columns and corresponding data
    columns = {
        "userId": [item["userId"] for item in data],
        "id": [item["id"] for item in data],
        "title": [item["title"] for item in data],
        "body": [item["body"] for item in data],
        "partition_date": [partition_date] * len(data),  # Add partition_date column
    }

    # Create PyArrow Table from dictionary
    table = pa.Table.from_pydict(columns)
    return table


@asset(
    partitions_def=daily_partition,
    group_name="abc_api",
    metadata={"dataset_name": "raw", "dagster/relation_identifier": "raw.posts"},
    compute_kind="python",
    owners=["team:thanhtt@vpbank.com.vn"],
    # automation_condition=AutomationCondition.cron_tick_passed("@daily")
)
def posts(context: AssetExecutionContext):
    logger = get_dagster_logger()
    partition_date_str = context.partition_key
    partition_date = datetime.strptime(partition_date_str, "%Y-%m-%d").date()

    # Step 1: Call API to get data
    api_url = "https://jsonplaceholder.typicode.com/posts"
    try:
        response = requests.get(api_url)
        response.raise_for_status()
        data = response.json()
        logger.info(f"Retrieved {len(data)} posts from API.")
    except requests.exceptions.RequestException as e:
        logger.error(f"Error calling API: {e}")
        raise

    # Step 2: Convert data to DataFrame
    arrow_table = create_pyarrow_table(data, partition_date)

    catalog_warehouse = "s3://warehouse/raw/"

    catalog_properties = {
        "uri": "http://localhost:8181/",
        "warehouse": catalog_warehouse,
        "s3.region": "us-east-1",
        "region_name": "us-east-1",
        "s3.access-key-id": "admin",
        "s3.secret-access-key": "password",
        "aws_access_key_id": "admin",
        "aws_secret_access_key": "password",
        "s3.endpoint": "http://localhost:9000",
    }

    catalog = load_catalog("default", **catalog_properties)
    table_name = "raw.posts"

    # Check if the table already exists
    try:
        table = catalog.load_table(table_name)
        logger.info(f"Table {table_name} already exists.")

    except pyiceberg.exceptions.NoSuchTableError:
        logger.error(f"Table {table_name} does not exist.")

        # # Create a new table
        # table = catalog.create_table(
        #     identifier=table_name,
        #     location=f"{catalog_warehouse}posts",
        #     schema=arrow_table.schema,
        #     properties={"format-version": "2", "write.format.default": "parquet"},
        # )

        # logger.info(f"Table {table_name} created successfully.")
    logger.info(f"Inserting data into {table_name}")
    overwrite_filter = E.EqualTo("partition_date", partition_date.strftime("%Y-%m-%d"))
    table.overwrite(arrow_table, overwrite_filter)

# AssetSpec("my_cron_asset", automation_condition=AutomationCondition.on_cron("@daily"))
