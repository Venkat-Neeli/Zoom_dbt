{{ config(
    materialized='table',
    pre_hook=none,
    post_hook=none
) }}

-- Process audit table for Gold layer transformations
WITH audit_base AS (
    SELECT
        'INITIAL_LOAD' AS execution_id,
        'GOLD_DIMENSION_SETUP' AS pipeline_name,
        'DIMENSION_LOAD' AS process_type,
        CURRENT_TIMESTAMP() AS start_time,
        CURRENT_TIMESTAMP() AS end_time,
        'COMPLETED' AS status,
        CAST(NULL AS VARCHAR(2000)) AS error_message,
        0 AS records_processed,
        0 AS records_successful,
        0 AS records_failed,
        0 AS processing_duration_seconds,
        'SILVER' AS source_system,
        'GOLD' AS target_system,
        'DBT_CLOUD' AS user_executed,
        'DBT_SERVER' AS server_name,
        0 AS memory_usage_mb,
        0.0 AS cpu_usage_percent,
        0.0 AS data_volume_gb,
        CURRENT_DATE() AS load_date,
        CURRENT_DATE() AS update_date
)

SELECT * FROM audit_base
