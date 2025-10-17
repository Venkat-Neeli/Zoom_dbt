{{ config(
    materialized='incremental',
    unique_key='error_id',
    on_schema_change='sync_all_columns'
) }}

WITH error_records AS (
    -- Users errors
    SELECT 
        {{ dbt_utils.generate_surrogate_key(['user_id', 'current_timestamp()']) }} AS error_id,
        'bz_users' AS source_table,
        CASE 
            WHEN user_id IS NULL THEN 'user_id'
            WHEN user_name IS NULL OR TRIM(user_name) = '' THEN 'user_name'
            WHEN email IS NULL OR TRIM(email) = '' THEN 'email'
            WHEN NOT REGEXP_LIKE(email, '^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}$') THEN 'email'
            WHEN plan_type NOT IN ('Free', 'Pro', 'Business', 'Enterprise') THEN 'plan_type'
        END AS source_column,
        CASE 
            WHEN user_id IS NULL THEN 'NULL_VALUE'
            WHEN user_name IS NULL OR TRIM(user_name) = '' THEN 'NULL_OR_EMPTY'
            WHEN email IS NULL OR TRIM(email) = '' THEN 'NULL_OR_EMPTY'
            WHEN NOT REGEXP_LIKE(email, '^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}$') THEN 'INVALID_FORMAT'
            WHEN plan_type NOT IN ('Free', 'Pro', 'Business', 'Enterprise') THEN 'INVALID_DOMAIN'
        END AS error_type,
        CASE 
            WHEN user_id IS NULL THEN 'User ID cannot be null'
            WHEN user_name IS NULL OR TRIM(user_name) = '' THEN 'User name cannot be null or empty'
            WHEN email IS NULL OR TRIM(email) = '' THEN 'Email cannot be null or empty'
            WHEN NOT REGEXP_LIKE(email, '^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}$') THEN 'Email format is invalid'
            WHEN plan_type NOT IN ('Free', 'Pro', 'Business', 'Enterprise') THEN 'Plan type must be Free, Pro, Business, or Enterprise'
        END AS error_description,
        COALESCE(CAST(user_id AS VARCHAR), CAST(user_name AS VARCHAR), CAST(email AS VARCHAR), CAST(plan_type AS VARCHAR)) AS error_value,
        CASE 
            WHEN user_id IS NULL THEN 'NOT NULL'
            WHEN user_name IS NULL OR TRIM(user_name) = '' THEN 'NOT NULL AND NOT EMPTY'
            WHEN email IS NULL OR TRIM(email) = '' THEN 'NOT NULL AND NOT EMPTY'
            WHEN NOT REGEXP_LIKE(email, '^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}$') THEN 'Valid email format'
            WHEN plan_type NOT IN ('Free', 'Pro', 'Business', 'Enterprise') THEN 'Free|Pro|Business|Enterprise'
        END AS expected_format,
        user_id AS record_identifier,
        current_timestamp() AS error_timestamp,
        'HIGH' AS severity_level,
        'OPEN' AS resolution_status,
        CAST(NULL AS VARCHAR(100)) AS resolved_by,
        CAST(NULL AS TIMESTAMP_NTZ) AS resolution_timestamp,
        current_date() AS load_date,
        current_date() AS update_date,
        source_system
    FROM {{ source('bronze', 'bz_users') }}
    WHERE user_id IS NULL 
       OR user_name IS NULL OR TRIM(user_name) = ''
       OR email IS NULL OR TRIM(email) = ''
       OR NOT REGEXP_LIKE(email, '^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}$')
       OR plan_type NOT IN ('Free', 'Pro', 'Business', 'Enterprise')
    
    UNION ALL
    
    -- Meetings errors
    SELECT 
        {{ dbt_utils.generate_surrogate_key(['meeting_id', 'current_timestamp()']) }} AS error_id,
        'bz_meetings' AS source_table,
        CASE 
            WHEN meeting_id IS NULL THEN 'meeting_id'
            WHEN host_id IS NULL THEN 'host_id'
            WHEN start_time IS NULL THEN 'start_time'
            WHEN end_time IS NULL THEN 'end_time'
            WHEN end_time <= start_time THEN 'end_time'
            WHEN duration_minutes IS NULL OR duration_minutes <= 0 OR duration_minutes > 1440 THEN 'duration_minutes'
        END AS source_column,
        CASE 
            WHEN meeting_id IS NULL THEN 'NULL_VALUE'
            WHEN host_id IS NULL THEN 'NULL_VALUE'
            WHEN start_time IS NULL THEN 'NULL_VALUE'
            WHEN end_time IS NULL THEN 'NULL_VALUE'
            WHEN end_time <= start_time THEN 'LOGICAL_ERROR'
            WHEN duration_minutes IS NULL OR duration_minutes <= 0 OR duration_minutes > 1440 THEN 'RANGE_ERROR'
        END AS error_type,
        CASE 
            WHEN meeting_id IS NULL THEN 'Meeting ID cannot be null'
            WHEN host_id IS NULL THEN 'Host ID cannot be null'
            WHEN start_time IS NULL THEN 'Start time cannot be null'
            WHEN end_time IS NULL THEN 'End time cannot be null'
            WHEN end_time <= start_time THEN 'End time must be after start time'
            WHEN duration_minutes IS NULL OR duration_minutes <= 0 OR duration_minutes > 1440 THEN 'Duration must be between 1 and 1440 minutes'
        END AS error_description,
        COALESCE(CAST(meeting_id AS VARCHAR), CAST(host_id AS VARCHAR), CAST(start_time AS VARCHAR), CAST(end_time AS VARCHAR), CAST(duration_minutes AS VARCHAR)) AS error_value,
        CASE 
            WHEN meeting_id IS NULL THEN 'NOT NULL'
            WHEN host_id IS NULL THEN 'NOT NULL'
            WHEN start_time IS NULL THEN 'NOT NULL'
            WHEN end_time IS NULL THEN 'NOT NULL'
            WHEN end_time <= start_time THEN 'end_time > start_time'
            WHEN duration_minutes IS NULL OR duration_minutes <= 0 OR duration_minutes > 1440 THEN '1 <= duration_minutes <= 1440'
        END AS expected_format,
        meeting_id AS record_identifier,
        current_timestamp() AS error_timestamp,
        'HIGH' AS severity_level,
        'OPEN' AS resolution_status,
        CAST(NULL AS VARCHAR(100)) AS resolved_by,
        CAST(NULL AS TIMESTAMP_NTZ) AS resolution_timestamp,
        current_date() AS load_date,
        current_date() AS update_date,
        source_system
    FROM {{ source('bronze', 'bz_meetings') }}
    WHERE meeting_id IS NULL 
       OR host_id IS NULL
       OR start_time IS NULL OR end_time IS NULL
       OR end_time <= start_time
       OR duration_minutes IS NULL OR duration_minutes <= 0 OR duration_minutes > 1440
)

SELECT * FROM error_records

{% if is_incremental() %}
    WHERE error_timestamp > (SELECT COALESCE(MAX(error_timestamp), '1900-01-01'::timestamp) FROM {{ this }})
{% endif %}
