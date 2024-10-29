{{
    config(
        pre_hook=[
            "set hive.exec.dynamic.partition.mode=nonstrict;"
        ],
        materialized='incremental',
        incremental_strategy='insert_overwrite',
        file_format= "iceberg",
        tags=["engine", "ung_tien_an_nhau"]
    )
}}

with
tbl_a
AS(
    select * 
    FROM {{ source('raw', 't24_account__s2') }}
)

select * 
    from {{ ref('mrt_posts__s2') }}
    where partition_date = '{{ var('partition_date') }}'