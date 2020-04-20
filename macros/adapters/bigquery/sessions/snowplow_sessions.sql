
{% macro bigquery__snowplow_sessions() %}

{{
    config(
        materialized='incremental',
        partition_by='dt',
        incremental_strategy = "insert_overwrite",
    )
}}

with sessions as (

    select * from {{ ref('snowplow_sessions_tmp') }}

)



select
    * except(session_index),
    row_number() over (partition by user_snowplow_domain_id order by session_start) as session_index,
    DATE(session_start) as dt

from stitched

{% endmacro %}
