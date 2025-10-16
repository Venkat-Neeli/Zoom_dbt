{{ config(
    materialized='incremental',
    unique_key='error_id',
    on_schema_change='fail'
) }}

-- Data Quality Errors Table
WITH error_data AS (
    SELECT 
        {{ dbt_utils.generate_surrogate_key(['current_timestamp()', 'source_table', 'error_type']) }} AS error_id,
        'SAMPLE_ERROR' AS source_table,
        'SAMPLE_COLUMN' AS source_column,
        'NULL_CHECK' AS error_type,
        'Sample error for initialization' AS error_description,
        'NULL' AS error_value,
        'NOT NULL' AS expected_format,
        'SAMPLE_RECORD_ID' AS record_identifier,
        CURRENT_TIMESTAMP() AS error_timestamp,
        'LOW' AS severity_level,
        'OPEN' AS resolution_status,
        NULL AS resolved_by,
        NULL AS resolution_timestamp,
        CURRENT_DATE() AS load_date,
        CURRENT_DATE() AS update_date,
        'DBT' AS source_system
    WHERE FALSE -- This ensures no actual records are inserted during initial creation
)

SELECT 
    error_id::VARCHAR(255) AS error_id,
    source_table::VARCHAR(255) AS source_table,
    source_column::VARCHAR(255) AS source_column,
    error_type::VARCHAR(100) AS error_type,
    error_description::VARCHAR(1000) AS error_description,
    error_value::VARCHAR(500) AS error_value,
    expected_format::VARCHAR(500) AS expected_format,
    record_identifier::VARCHAR(255) AS record_identifier,
    error_timestamp,
    severity_level::VARCHAR(50) AS severity_level,
    resolution_status::VARCHAR(50) AS resolution_status,
    resolved_by::VARCHAR(255) AS resolved_by,
    resolution_timestamp,
    load_date,
    update_date,
    source_system::VARCHAR(255) AS source_system
FROM error_data

{% if is_incremental() %}
    WHERE error_timestamp > (SELECT COALESCE(MAX(error_timestamp), '1900-01-01') FROM {{ this }})
{% endif %}
