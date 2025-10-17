{{
    config(
        materialized='incremental',
        unique_key='execution_id',
        on_schema_change='sync_all_columns'
    )
}}

WITH audit_base AS (
    SELECT
        '{{ invocation_id }}' as execution_id,
        'INITIAL_AUDIT_LOG' as pipeline_name,
        CURRENT_TIMESTAMP() as start_time,
        CURRENT_TIMESTAMP() as end_time,
        'SUCCESS' as status,
        NULL as error_message,
        0 as records_processed,
        0 as records_successful,
        0 as records_failed,
        0 as processing_duration_seconds,
        'SYSTEM' as source_system,
        'SILVER' as target_system,
        'INITIALIZATION' as process_type,
        'DBT_SYSTEM' as user_executed,
        'DBT_CLOUD' as server_name,
        0 as memory_usage_mb,
        0 as cpu_usage_percent,
        CURRENT_DATE() as load_date,
        CURRENT_DATE() as update_date
    WHERE 1=0  -- This ensures no initial records are inserted
)

SELECT 
    execution_id::VARCHAR(255) as execution_id,
    pipeline_name::VARCHAR(255) as pipeline_name,
    start_time,
    end_time,
    status::VARCHAR(50) as status,
    error_message::VARCHAR(1000) as error_message,
    records_processed,
    records_successful,
    records_failed,
    processing_duration_seconds,
    source_system::VARCHAR(255) as source_system,
    target_system::VARCHAR(255) as target_system,
    process_type::VARCHAR(100) as process_type,
    user_executed::VARCHAR(255) as user_executed,
    server_name::VARCHAR(255) as server_name,
    memory_usage_mb,
    cpu_usage_percent,
    load_date,
    update_date
FROM audit_base

{% if is_incremental() %}
    WHERE start_time > (SELECT MAX(start_time) FROM {{ this }})
{% endif %}
