{{
    config(
        materialized='table'
    )
}}

SELECT
    order_id,
    {{ moscow_time_macro('created_at') }} AS created_at_moscow
FROM {{ source('raw', 'raw_table_utc') }}
