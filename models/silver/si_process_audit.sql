{{ config(
    materialized='incremental',
    unique_key='execution_id',
    on_schema_change='sync_all_columns',
    tags=['audit', 'silver']
) }}

WITH audit_data AS (
    SELECT
        {{ dbt_utils.generate_surrogate_key(['current_timestamp()', 'invocation_id']) }} AS execution_id,
        'Bronze_to_Silver_Transform' AS pipeline_name,
        CURRENT_TIMESTAMP() AS start_time,
        CURRENT_TIMESTAMP() AS end_time,
        'RUNNING' AS status,
        NULL AS error_message,
        0 AS records_processed,
        0 AS records_successful,
        0 AS records_failed,
        0 AS processing_duration_seconds,
        'DBT' AS source_system,
        'SILVER' AS target_system,
        'ETL' AS process_type,
        'dbt_user' AS user_executed,
        'dbt_cloud' AS server_name,
        NULL AS memory_usage_mb,
        NULL AS cpu_usage_percent,
        CURRENT_DATE() AS load_date,
        CURRENT_DATE() AS update_date
)

SELECT * FROM audit_data

{% if is_incremental() %}
    WHERE start_time > (SELECT MAX(start_time) FROM {{ this }})
{% endif %}
