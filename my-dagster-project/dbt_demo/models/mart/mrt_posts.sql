{{
    config(
        pre_hook=[
            "set hive.exec.dynamic.partition.mode=nonstrict;"
        ],
        materialized='table'
    )
}}
select userId,
         id,
         title,
         body,
         partition_date
from {{ source('raw_source', 'posts') }}
