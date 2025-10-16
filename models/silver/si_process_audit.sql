{{ config(
    materialized='incremental',
    unique_key='execution_id',
    on_schema_change='fail'
) }}

WITH audit_data AS (
    SELECT
        {{ dbt_utils.generate_surrogate_key(['current_timestamp()']) }} AS execution_id,
        'bronze_to_silver_transformation' AS pipeline_name,
        CURRENT_TIMESTAMP() AS start_time,
        CURRENT_TIMESTAMP() AS end_time,
        'SUCCESS' AS status,
        NULL AS error_message,
        0 AS records_processed,
        0 AS records_successful,
        0 AS records_failed,
        0 AS processing_duration_seconds,
        'Zoom' AS source_system,
        'Silver' AS target_system,
        'ETL' AS process_type,
        'dbt_user' AS user_executed,
        'dbt_cloud' AS server_name,
        NULL AS memory_usage_mb,
        NULL AS cpu_usage_percent,
        CURRENT_DATE() AS load_date,
        CURRENT_DATE() AS update_date
)

SELECT 
    execution_id,
    pipeline_name,
    start_time,
    end_time,
    status,
    error_message,
    records_processed,
    records_successful,
    records_failed,
    processing_duration_seconds,
    source_system,
    target_system,
    process_type,
    user_executed,
    server_name,
    memory_usage_mb,
    cpu_usage_percent,
    load_date,
    update_date
FROM audit_data

{% if is_incremental() %}
    WHERE start_time > (SELECT MAX(start_time) FROM {{ this }})
{% endif %}
