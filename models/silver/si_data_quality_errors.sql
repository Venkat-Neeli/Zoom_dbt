{{
    config(
        materialized='incremental',
        unique_key='error_id',
        on_schema_change='sync_all_columns'
    )
}}

-- Data Quality Errors tracking table
WITH error_data AS (
    SELECT
        {{ dbt_utils.generate_surrogate_key(['current_timestamp()', 'source_table', 'error_type']) }} AS error_id,
        'sample_table' AS source_table,
        'sample_column' AS source_column,
        'NULL_CHECK' AS error_type,
        'Sample error for initialization' AS error_description,
        'NULL' AS error_value,
        'NOT NULL' AS expected_format,
        'sample_record_id' AS record_identifier,
        CURRENT_TIMESTAMP() AS error_timestamp,
        'LOW' AS severity_level,
        'OPEN' AS resolution_status,
        NULL AS resolved_by,
        NULL AS resolution_timestamp,
        CURRENT_DATE() AS load_date,
        CURRENT_DATE() AS update_date,
        'Zoom' AS source_system
    WHERE FALSE -- This ensures no actual data is inserted during initialization
)

SELECT * FROM error_data

{% if is_incremental() %}
    WHERE error_timestamp > (SELECT MAX(error_timestamp) FROM {{ this }})
{% endif %}
