{{
    config(
        materialized='incremental',
        unique_key='error_id',
        on_schema_change='sync_all_columns'
    )
}}

WITH error_base AS (
    SELECT
        '{{ invocation_id }}_ERROR_001' as error_id,
        'INITIAL_ERROR_LOG' as source_table,
        'INITIALIZATION' as source_column,
        'SYSTEM_INIT' as error_type,
        'Initial error log setup' as error_description,
        'N/A' as error_value,
        'N/A' as expected_format,
        'SYSTEM_INIT' as record_identifier,
        CURRENT_TIMESTAMP() as error_timestamp,
        'INFO' as severity_level,
        'RESOLVED' as resolution_status,
        'SYSTEM' as resolved_by,
        CURRENT_TIMESTAMP() as resolution_timestamp,
        CURRENT_DATE() as load_date,
        CURRENT_DATE() as update_date,
        'SYSTEM' as source_system
    WHERE 1=0  -- This ensures no initial records are inserted
)

SELECT 
    error_id::VARCHAR(255) as error_id,
    source_table::VARCHAR(255) as source_table,
    source_column::VARCHAR(255) as source_column,
    error_type::VARCHAR(100) as error_type,
    error_description::VARCHAR(1000) as error_description,
    error_value::VARCHAR(500) as error_value,
    expected_format::VARCHAR(255) as expected_format,
    record_identifier::VARCHAR(255) as record_identifier,
    error_timestamp,
    severity_level::VARCHAR(50) as severity_level,
    resolution_status::VARCHAR(50) as resolution_status,
    resolved_by::VARCHAR(255) as resolved_by,
    resolution_timestamp,
    load_date,
    update_date,
    source_system::VARCHAR(255) as source_system
FROM error_base

{% if is_incremental() %}
    WHERE error_timestamp > (SELECT MAX(error_timestamp) FROM {{ this }})
{% endif %}
