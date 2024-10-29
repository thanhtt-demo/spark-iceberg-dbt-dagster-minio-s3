{% snapshot mrt_posts__s2 %}

{% set unique_key = 'id' %}
{% set strategy = 'check' %}
{% set check_cols = ['title', 'body'] %}

{{ config(
    target_schema='mart__s2',
    unique_key=unique_key,
    strategy=strategy,
    check_cols=check_cols,
    updated_at='updated_at',
    file_format='iceberg'
) }}

SELECT userId,
         id,
         title,
         body,
         partition_date,
         CURRENT_TIMESTAMP AS updated_at
FROM {{ source('raw', 'posts') }}
WHERE partition_date = '{{ var('partition_date') }}'
    AND body IS NOT NULL
    and title IS NOT NULL

{% endsnapshot %}

