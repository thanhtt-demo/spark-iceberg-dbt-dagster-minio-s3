{{
    config(
        pre_hook=[
            "set hive.exec.dynamic.partition.mode=nonstrict;"
        ],
        materialized='incremental',
        incremental_strategy='insert_overwrite',
        partition_by=['partition_date'],
        file_format= "iceberg",
        tags=["engine", "ung_luong"]
    )
}}

with mrt_posts as(
    select * 
    FROM {{ ref('mrt_posts') }}
    where partition_date = '{{ var('partition_date') }}'
),

mrt_comments as(
    select * 
    from {{ ref('mrt_comments') }}
    where partition_date = '{{ var('partition_date') }}'
)

select c.postId, 
    count(1) as number_of_comments, 
    max(length(c.body)) as max_length, 
    c.partition_date as partition_date
from mrt_posts p
inner join mrt_comments c
on p.id = c.postId
where p.partition_date = '{{ var('partition_date') }}'
and c.partition_date = '{{ var('partition_date') }}'
group by c.postId, c.partition_date

