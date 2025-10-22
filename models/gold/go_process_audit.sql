{{ config(
    materialized='table',
    cluster_by=['start_time', 'pipeline_name']
) }}

-- Gold Process Audit Table
SELECT 
    UUID_STRING() AS execution_id,
    'Gold Process Audit Initialization'::VARCHAR(200) AS pipeline_name,
    'AUDIT_SETUP'::VARCHAR(100) AS process_type,
    CURRENT_TIMESTAMP() AS start_time,
    CURRENT_TIMESTAMP() AS end_time,
    'COMPLETED'::VARCHAR(50) AS status,
    NULL::VARCHAR(2000) AS error_message,
    0 AS records_processed,
    0 AS records_successful,
    0 AS records_failed,
    0 AS processing_duration_seconds,
    'SYSTEM'::VARCHAR(100) AS source_system,
    'GOLD'::VARCHAR(100) AS target_system,
    'DBT_CLOUD'::VARCHAR(100) AS user_executed,
    'DBT_SERVER'::VARCHAR(100) AS server_name,
    0 AS memory_usage_mb,
    0.0 AS cpu_usage_percent,
    0.0 AS data_volume_gb,
    CURRENT_DATE() AS load_date,
    CURRENT_DATE() AS update_date
