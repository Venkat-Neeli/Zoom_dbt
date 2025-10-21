{{ config(
    materialized='incremental',
    on_schema_change='sync_all_columns',
    unique_key='execution_id'
) }}

SELECT 
    UUID_STRING() AS execution_id,
    'go_process_audit' AS pipeline_name,
    'AUDIT' AS process_type,
    CURRENT_TIMESTAMP() AS start_time,
    CURRENT_TIMESTAMP() AS end_time,
    'INITIALIZED' AS status,
    NULL AS error_message,
    0 AS records_processed,
    0 AS records_successful,
    0 AS records_failed,
    0 AS processing_duration_seconds,
    'GOLD' AS source_system,
    'GOLD' AS target_system,
    'system' AS user_executed,
    'dbt_cloud' AS server_name,
    0 AS memory_usage_mb,
    0.0 AS cpu_usage_percent,
    0.0 AS data_volume_gb,
    CURRENT_DATE() AS load_date,
    CURRENT_DATE() AS update_date
WHERE FALSE  -- This ensures the model creates the table structure but doesn't insert initial data

{% if is_incremental() %}
UNION ALL
SELECT * FROM {{ this }}
{% endif %}
