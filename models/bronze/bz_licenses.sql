{{ config(materialized='table') }}

-- Bronze layer transformation for licenses data
-- This model performs 1:1 mapping from raw licenses to bronze licenses with data quality checks

WITH source_data AS (
    SELECT 
        license_id,
        license_type,
        assigned_to_user_id,
        start_date,
        end_date,
        load_timestamp as source_load_timestamp,
        update_timestamp as source_update_timestamp,
        source_system as source_source_system
    FROM {{ source('raw_data', 'licenses') }}
),

-- Data quality and validation layer
validated_data AS (
    SELECT 
        license_id,
        license_type,
        assigned_to_user_id,
        start_date,
        end_date,
        source_load_timestamp,
        source_update_timestamp,
        source_source_system,
        -- Add data quality flags
        CASE 
            WHEN license_id IS NULL THEN 'MISSING_LICENSE_ID'
            WHEN license_type IS NULL THEN 'MISSING_LICENSE_TYPE'
            ELSE 'VALID'
        END as data_quality_status
    FROM source_data
),

-- Final transformation with audit columns
final_transform AS (
    SELECT 
        license_id,
        license_type,
        assigned_to_user_id,
        start_date,
        end_date,
        -- Metadata columns with current timestamp for bronze layer
        CURRENT_TIMESTAMP() as load_timestamp,
        CURRENT_TIMESTAMP() as update_timestamp,
        'ZOOM_PLATFORM' as source_system
    FROM validated_data
    WHERE data_quality_status = 'VALID'  -- Only include valid records
)

SELECT * FROM final_transform
