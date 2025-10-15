{{ config(materialized='table') }}

-- Bronze layer transformation for users data
-- This model performs 1:1 mapping from raw users to bronze users with data quality checks

WITH source_data AS (
    SELECT 
        user_id,
        user_name,
        email,
        company,
        plan_type,
        load_timestamp as source_load_timestamp,
        update_timestamp as source_update_timestamp,
        source_system as source_source_system
    FROM {{ source('raw_data', 'users') }}
),

-- Data quality and validation layer
validated_data AS (
    SELECT 
        user_id,
        user_name,
        email,
        company,
        plan_type,
        source_load_timestamp,
        source_update_timestamp,
        source_source_system,
        -- Add data quality flags
        CASE 
            WHEN user_id IS NULL THEN 'MISSING_USER_ID'
            WHEN email IS NULL THEN 'MISSING_EMAIL'
            ELSE 'VALID'
        END as data_quality_status
    FROM source_data
),

-- Final transformation with audit columns
final_transform AS (
    SELECT 
        user_id,
        user_name,
        email,
        company,
        plan_type,
        -- Metadata columns with current timestamp for bronze layer
        CURRENT_TIMESTAMP() as load_timestamp,
        CURRENT_TIMESTAMP() as update_timestamp,
        'ZOOM_PLATFORM' as source_system
    FROM validated_data
    WHERE data_quality_status = 'VALID'  -- Only include valid records
)

SELECT * FROM final_transform
