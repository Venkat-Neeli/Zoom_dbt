{{ config(
    materialized='incremental',
    unique_key='error_id'
) }}

WITH error_base AS (
    SELECT 
        {{ dbt_utils.generate_surrogate_key(['current_timestamp()']) }} AS error_id,
        'Initial Setup' AS source_table,
        'N/A' AS source_column,
        'SETUP' AS error_type,
        'Initial error table setup' AS error_description,
        'N/A' AS error_value,
        'N/A' AS expected_format,
        'N/A' AS record_identifier,
        CURRENT_TIMESTAMP() AS error_timestamp,
        'LOW' AS severity_level,
        'RESOLVED' AS resolution_status,
        'system' AS resolved_by,
        CURRENT_TIMESTAMP() AS resolution_timestamp,
        CURRENT_DATE() AS load_date,
        CURRENT_DATE() AS update_date,
        'DBT' AS source_system
    WHERE FALSE -- This ensures no rows are inserted during initial setup
)

SELECT 
    error_id,
    source_table,
    source_column,
    error_type,
    error_description,
    error_value,
    expected_format,
    record_identifier,
    error_timestamp,
    severity_level,
    resolution_status,
    resolved_by,
    resolution_timestamp,
    load_date,
    update_date,
    source_system
FROM error_base
