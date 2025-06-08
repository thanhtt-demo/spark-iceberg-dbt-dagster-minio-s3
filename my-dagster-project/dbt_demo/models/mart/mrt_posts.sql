{{
    config(
        pre_hook=[
            "set hive.exec.dynamic.partition.mode=nonstrict;"
        ],
        materialized='incremental',
        partition_by='partition_date',
        incremental_strategy='insert_overwrite',
        file_format= "iceberg"
    )
}}

with tbl_a
AS(
    SELECT * 
    FROM {{ source('raw', 't24_account__s2') }}
)

SELECT userId,
        id,
        title,
        body,
        partition_date
FROM {{ source('raw', 'posts') }}
WHERE partition_date BETWEEN '{{ var('partition_date') }}' AND '{{ var('partition_date') }}'
    AND body IS NOT NULL
    and title IS NOT NULL

-- {# {% if is_incremental() %}

--   -- this filter will only be applied on an incremental run
--   -- (uses >= to include records whose timestamp occurred since the last run of this model)
--   -- (If event_time is NULL or the table is truncated, the condition will always be true and load all records)
-- -- where partition_date >= (select coalesce(max(partition_date),'1900-01-01') from {{ this }} )
--    where partition_date between '{{ var('min_date') }}' and '{{ var('max_date') }}'

-- {% endif %} #}
