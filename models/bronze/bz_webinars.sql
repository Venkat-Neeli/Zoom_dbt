{{ config(materialized='table') }}

-- Bronze layer transformation for webinars data
-- This model performs 1:1 mapping from raw webinars to bronze webinars with data quality checks

WITH source_data AS (
    SELECT 
        webinar_id,
        host_id,
        webinar_topic,
        start_time,
        end_time,
        registrants,
        load_timestamp as source_load_timestamp,
        update_timestamp as source_update_timestamp,
        source_system as source_source_system
    FROM {{ source('raw_data', 'webinars') }}
),

-- Data quality and validation layer
validated_data AS (
    SELECT 
        webinar_id,
        host_id,
        webinar_topic,
        start_time,
        end_time,
        registrants,
        source_load_timestamp,
        source_update_timestamp,
        source_source_system,
        -- Add data quality flags
        CASE 
            WHEN webinar_id IS NULL THEN 'MISSING_WEBINAR_ID'
            WHEN host_id IS NULL THEN 'MISSING_HOST_ID'
            ELSE 'VALID'
        END as data_quality_status
    FROM source_data
),

-- Final transformation with audit columns
final_transform AS (
    SELECT 
        webinar_id,
        host_id,
        webinar_topic,
        start_time,
        end_time,
        registrants,
        -- Metadata columns with current timestamp for bronze layer
        CURRENT_TIMESTAMP() as load_timestamp,
        CURRENT_TIMESTAMP() as update_timestamp,
        'ZOOM_PLATFORM' as source_system
    FROM validated_data
    WHERE data_quality_status = 'VALID'  -- Only include valid records
)

SELECT * FROM final_transform
