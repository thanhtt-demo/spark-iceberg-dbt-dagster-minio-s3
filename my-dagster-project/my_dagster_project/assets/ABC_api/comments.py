from dagster import asset, get_dagster_logger, AssetExecutionContext
import requests
import pyiceberg
import pyarrow as pa
from pyiceberg.catalog import load_catalog
from ...partitions import daily_partition
from datetime import datetime
# from pyiceberg.partitioning import PartitionSpec, PartitionField
# from pyiceberg.transforms import DayTransform
import pyiceberg.expressions as E


def create_pyarrow_table(data, partition_date):
    """
    Create a PyArrow Table from a list of dictionaries and a partition date.

    Args:
        data (list of dict): A list of dictionaries where each dictionary represents a row of data.
            Each dictionary should have the keys: 'postId', 'id', 'name', 'email', and 'body'.
        partition_date (str): The partition date to be added as a column to the table.

    Returns:
        pyarrow.Table: A PyArrow Table containing the data and the partition date column.
    """
    # Define columns and corresponding data
    columns = {
        "postId": [item["postId"] for item in data],
        "id": [item["id"] for item in data],
        "name": [item["name"] for item in data],
        "email": [item["email"] for item in data],
        "body": [item["body"] for item in data],
        "partition_date": [partition_date] * len(data),  # Add partition_date column
    }

    # Create PyArrow Table from dictionary
    table = pa.Table.from_pydict(columns)
    return table


@asset(
    partitions_def=daily_partition,
    group_name="abc_api",
    metadata={"dataset_name": "raw", "dagster/relation_identifier": "raw.comments"},
    compute_kind="python",
    owners=["team:thanhtt@vpbank.com.vn"]
)
def comments(context: AssetExecutionContext):
    logger = get_dagster_logger()
    partition_date_str = context.partition_key
    partition_date = datetime.strptime(partition_date_str, "%Y-%m-%d").date()

    # Step 1: Call API to get data
    api_url = "https://jsonplaceholder.typicode.com/comments"
    try:
        response = requests.get(api_url)
        response.raise_for_status()
        data = response.json()
        logger.info(f"Retrieved {len(data)} comments from API.")
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
    table_name = "raw.comments"

    # Check if the table already exists
    try:
        table = catalog.load_table(table_name)
        logger.info(f"Table {table_name} already exists.")

    except pyiceberg.exceptions.NoSuchTableError:
        logger.error(f"Table {table_name} does not exist.")

        # # Create a new table
        # table = catalog.create_table(
        #     identifier=table_name,
        #     location=f"{catalog_warehouse}comments",
        #     schema=arrow_table.schema,
        #     properties={"format-version": "2", "write.format.default": "parquet"},
        # )

        # logger.info(f"Table {table_name} created successfully.")
    logger.info(f"Inserting data into {table_name}")
    overwrite_filter = E.EqualTo("partition_date", partition_date.strftime("%Y-%m-%d"))
    table.overwrite(arrow_table, overwrite_filter)
