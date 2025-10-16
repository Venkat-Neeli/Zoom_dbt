{{ config(
    materialized='incremental',
    unique_key='execution_id',
    on_schema_change='fail'
) }}

WITH audit_data AS (
    SELECT 
        {{ dbt_utils.generate_surrogate_key(['current_timestamp()', 'current_user()']) }} AS execution_id,
        'si_process_audit' AS pipeline_name,
        CURRENT_TIMESTAMP() AS start_time,
        CURRENT_TIMESTAMP() AS end_time,
        'SUCCESS' AS status,
        NULL AS error_message,
        0 AS records_processed,
        0 AS records_successful,
        0 AS records_failed,
        0 AS processing_duration_seconds,
        'DBT' AS source_system,
        'SILVER' AS target_system,
        'ETL' AS process_type,
        CURRENT_USER() AS user_executed,
        'DBT_CLOUD' AS server_name,
        NULL AS memory_usage_mb,
        NULL AS cpu_usage_percent,
        CURRENT_DATE() AS load_date,
        CURRENT_DATE() AS update_date
)

SELECT 
    execution_id::VARCHAR(255) AS execution_id,
    pipeline_name::VARCHAR(255) AS pipeline_name,
    start_time,
    end_time,
    status::VARCHAR(50) AS status,
    error_message::VARCHAR(1000) AS error_message,
    records_processed,
    records_successful,
    records_failed,
    processing_duration_seconds,
    source_system::VARCHAR(255) AS source_system,
    target_system::VARCHAR(255) AS target_system,
    process_type::VARCHAR(100) AS process_type,
    user_executed::VARCHAR(255) AS user_executed,
    server_name::VARCHAR(255) AS server_name,
    memory_usage_mb,
    cpu_usage_percent,
    load_date,
    update_date
FROM audit_data

{% if is_incremental() %}
    WHERE start_time > (SELECT MAX(start_time) FROM {{ this }})
{% endif %}
