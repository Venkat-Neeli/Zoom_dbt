{{
  config(
    materialized='incremental',
    unique_key='error_id',
    on_schema_change='sync_all_columns'
  )
}}

-- Data Quality Errors Table
WITH error_records AS (
  -- Users errors
  SELECT 
    {{ dbt_utils.generate_surrogate_key(['user_id', 'current_timestamp()']) }} as error_id,
    'bz_users' as source_table,
    'user_id' as source_column,
    'NULL_VALUE' as error_type,
    'User ID cannot be null' as error_description,
    COALESCE(user_id, 'NULL') as error_value,
    'NOT NULL' as expected_format,
    COALESCE(user_id, 'UNKNOWN') as record_identifier,
    CURRENT_TIMESTAMP() as error_timestamp,
    'HIGH' as severity_level,
    'OPEN' as resolution_status,
    NULL as resolved_by,
    NULL as resolution_timestamp,
    CURRENT_DATE() as load_date,
    CURRENT_DATE() as update_date,
    'BRONZE' as source_system
  FROM {{ source('bronze', 'bz_users') }}
  WHERE user_id IS NULL
  
  UNION ALL
  
  -- Email format errors
  SELECT 
    {{ dbt_utils.generate_surrogate_key(['user_id', 'email', 'current_timestamp()']) }} as error_id,
    'bz_users' as source_table,
    'email' as source_column,
    'INVALID_FORMAT' as error_type,
    'Email format is invalid' as error_description,
    COALESCE(email, 'NULL') as error_value,
    'username@domain.tld' as expected_format,
    user_id as record_identifier,
    CURRENT_TIMESTAMP() as error_timestamp,
    'MEDIUM' as severity_level,
    'OPEN' as resolution_status,
    NULL as resolved_by,
    NULL as resolution_timestamp,
    CURRENT_DATE() as load_date,
    CURRENT_DATE() as update_date,
    'BRONZE' as source_system
  FROM {{ source('bronze', 'bz_users') }}
  WHERE email IS NOT NULL AND NOT (email LIKE '%@%.%')
  
  UNION ALL
  
  -- Meeting time validation errors
  SELECT 
    {{ dbt_utils.generate_surrogate_key(['meeting_id', 'current_timestamp()']) }} as error_id,
    'bz_meetings' as source_table,
    'start_time,end_time' as source_column,
    'LOGICAL_ERROR' as error_type,
    'Start time must be before end time' as error_description,
    CONCAT('start:', start_time, ' end:', end_time) as error_value,
    'start_time < end_time' as expected_format,
    meeting_id as record_identifier,
    CURRENT_TIMESTAMP() as error_timestamp,
    'HIGH' as severity_level,
    'OPEN' as resolution_status,
    NULL as resolved_by,
    NULL as resolution_timestamp,
    CURRENT_DATE() as load_date,
    CURRENT_DATE() as update_date,
    'BRONZE' as source_system
  FROM {{ source('bronze', 'bz_meetings') }}
  WHERE start_time >= end_time
)

SELECT * FROM error_records

{% if is_incremental() %}
  WHERE error_timestamp > (SELECT COALESCE(MAX(error_timestamp), '1900-01-01') FROM {{ this }})
{% endif %}
