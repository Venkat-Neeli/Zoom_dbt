{{ config(
    materialized='table',
    unique_key='execution_id'
) }}

WITH audit_base AS (
    SELECT 
        {{ dbt_utils.generate_surrogate_key(['current_timestamp()']) }} AS execution_id,
        'Initial Setup' AS pipeline_name,
        CURRENT_TIMESTAMP() AS start_time,
        CURRENT_TIMESTAMP() AS end_time,
        'SUCCESS' AS status,
        CAST(NULL AS STRING) AS error_message,
        0 AS records_processed,
        0 AS records_successful,
        0 AS records_failed,
        0 AS processing_duration_seconds,
        'DBT' AS source_system,
        'SILVER' AS target_system,
        'AUDIT_SETUP' AS process_type,
        'dbt_user' AS user_executed,
        'dbt_cloud' AS server_name,
        0 AS memory_usage_mb,
        0 AS cpu_usage_percent,
        CURRENT_DATE() AS load_date,
        CURRENT_DATE() AS update_date
    WHERE FALSE -- This ensures no rows are inserted during initial setup
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
FROM audit_base
