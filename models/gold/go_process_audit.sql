{{ config(
    materialized='table',
    cluster_by=['start_time', 'pipeline_name'],
    tags=['audit', 'gold']
) }}

WITH audit_base AS (
    SELECT 
        {{ dbt_utils.generate_surrogate_key(['pipeline_name', 'start_time']) }} AS execution_id,
        'Gold Layer Transformation' AS pipeline_name,
        'Dimension Processing' AS process_type,
        CURRENT_TIMESTAMP() AS start_time,
        NULL AS end_time,
        'RUNNING' AS status,
        NULL AS error_message,
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

SELECT 
    execution_id::VARCHAR(50) AS execution_id,
    pipeline_name::VARCHAR(200) AS pipeline_name,
    process_type::VARCHAR(100) AS process_type,
    start_time,
    end_time,
    status::VARCHAR(50) AS status,
    error_message::VARCHAR(2000) AS error_message,
    records_processed,
    records_successful,
    records_failed,
    processing_duration_seconds,
    source_system::VARCHAR(100) AS source_system,
    target_system::VARCHAR(100) AS target_system,
    user_executed::VARCHAR(100) AS user_executed,
    server_name::VARCHAR(100) AS server_name,
    memory_usage_mb,
    cpu_usage_percent,
    data_volume_gb,
    load_date,
    update_date
FROM audit_base
