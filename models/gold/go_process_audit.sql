{{ config(
    materialized='table',
    pre_hook='',
    post_hook=''
) }}

SELECT 
    UUID_STRING() AS execution_id,
    'INITIAL_SETUP' AS pipeline_name,
    CURRENT_TIMESTAMP() AS start_time,
    CURRENT_TIMESTAMP() AS end_time,
    'COMPLETED' AS status,
    NULL AS error_message,
    0 AS records_processed,
    0 AS records_successful,
    0 AS records_failed,
    0 AS processing_duration_seconds,
    'SYSTEM' AS source_system,
    'GOLD' AS target_system,
    'AUDIT_SETUP' AS process_type,
    'DBT_SYSTEM' AS user_executed,
    'DBT_SERVER' AS server_name,
    0 AS memory_usage_mb,
    0.0 AS cpu_usage_percent,
    CURRENT_DATE() AS load_date,
    CURRENT_DATE() AS update_date
WHERE FALSE  -- This ensures no initial records are created
