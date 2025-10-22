{{ config(
    materialized='table',
    cluster_by=['start_time', 'pipeline_name'],
    tags=['audit', 'gold']
) }}

-- Gold Process Audit Table
-- This table tracks all Gold layer transformation processes
-- Must be created first before any other Gold models

WITH audit_base AS (
    SELECT
        {{ dbt_utils.generate_surrogate_key(['current_timestamp()', 'current_user()']) }} AS execution_id,
        'GOLD_LAYER_TRANSFORMATION' AS pipeline_name,
        'DIMENSIONAL_MODELING' AS process_type,
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
        CURRENT_USER() AS user_executed,
        'DBT_CLOUD' AS server_name,
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
