from dagster import asset, get_dagster_logger, AssetExecutionContext
from dagster import op, graph, job, asset

@op
def op1():
    return "data from op1"

@op
def op2(input1):
    return f"{input1} processed by op2"

@graph
def my_graph():
    result_op1 = op1()
    result_op2 = op2(result_op1)
    return result_op2

@asset
def t24_master_tbl(context: AssetExecutionContext, my_graph):
    # Use the output from the graph as the asset's content
    context.log.info(f"Asset created with: {my_graph}")
    return my_graph

@job
def t24_topic_parsing_job():
    my_graph()
