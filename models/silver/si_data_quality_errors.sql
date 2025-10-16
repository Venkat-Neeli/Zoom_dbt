{{ config(
    materialized='incremental',
    unique_key='error_id',
    on_schema_change='fail',
    tags=['silver', 'data_quality']
) }}

WITH error_data AS (
    -- Users table errors
    SELECT 
        {{ dbt_utils.generate_surrogate_key(['CURRENT_TIMESTAMP()', 'RANDOM()']) }} AS error_id,
        'bz_users' AS source_table,
        'user_id' AS source_column,
        'NULL_VALUE' AS error_type,
        'User ID cannot be null' AS error_description,
        COALESCE(user_id, 'NULL') AS error_value,
        'NOT NULL' AS expected_format,
        COALESCE(user_id, 'UNKNOWN') AS record_identifier,
        CURRENT_TIMESTAMP() AS error_timestamp,
        'HIGH' AS severity_level,
        'OPEN' AS resolution_status,
        NULL AS resolved_by,
        NULL AS resolution_timestamp,
        CURRENT_DATE() AS load_date,
        CURRENT_DATE() AS update_date,
        'BRONZE' AS source_system
    FROM {{ source('bronze', 'bz_users') }}
    WHERE user_id IS NULL
    
    UNION ALL
    
    -- Email format errors
    SELECT 
        {{ dbt_utils.generate_surrogate_key(['CURRENT_TIMESTAMP()', 'RANDOM()']) }} AS error_id,
        'bz_users' AS source_table,
        'email' AS source_column,
        'INVALID_FORMAT' AS error_type,
        'Email format is invalid' AS error_description,
        email AS error_value,
        'username@domain.tld' AS expected_format,
        user_id AS record_identifier,
        CURRENT_TIMESTAMP() AS error_timestamp,
        'MEDIUM' AS severity_level,
        'OPEN' AS resolution_status,
        NULL AS resolved_by,
        NULL AS resolution_timestamp,
        CURRENT_DATE() AS load_date,
        CURRENT_DATE() AS update_date,
        'BRONZE' AS source_system
    FROM {{ source('bronze', 'bz_users') }}
    WHERE email IS NOT NULL 
      AND NOT REGEXP_LIKE(email, '^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\\.[A-Za-z]{2,}$')
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
FROM error_data

{% if is_incremental() %}
  WHERE error_timestamp > (SELECT MAX(error_timestamp) FROM {{ this }})
{% endif %}
