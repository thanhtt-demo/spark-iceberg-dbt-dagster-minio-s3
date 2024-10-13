{{
    config(
        pre_hook=[
            "set hive.exec.dynamic.partition.mode=nonstrict;"
        ],
        materialized='incremental',
        incremental_strategy='insert_overwrite',
        partition_by=['partition_date']
    )
}}

with _tmp as (select 1)
, mappings as (
    select
        userId,
         id,
         title,
         body,
         partition_date
    from {{ ref("mrt_posts") }}
    where 1=1
)

select * from mappings;
