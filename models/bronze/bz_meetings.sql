{{ config(materialized='table') }}

-- Bronze layer transformation for meetings data
-- This model performs 1:1 mapping from raw meetings to bronze meetings with data quality checks

WITH source_data AS (
    SELECT 
        meeting_id,
        host_id,
        meeting_topic,
        start_time,
        end_time,
        duration_minutes,
        load_timestamp as source_load_timestamp,
        update_timestamp as source_update_timestamp,
        source_system as source_source_system
    FROM {{ source('raw_data', 'meetings') }}
),

-- Data quality and validation layer
validated_data AS (
    SELECT 
        meeting_id,
        host_id,
        meeting_topic,
        start_time,
        end_time,
        duration_minutes,
        source_load_timestamp,
        source_update_timestamp,
        source_source_system,
        -- Add data quality flags
        CASE 
            WHEN meeting_id IS NULL THEN 'MISSING_MEETING_ID'
            WHEN host_id IS NULL THEN 'MISSING_HOST_ID'
            ELSE 'VALID'
        END as data_quality_status
    FROM source_data
),

-- Final transformation with audit columns
final_transform AS (
    SELECT 
        meeting_id,
        host_id,
        meeting_topic,
        start_time,
        end_time,
        duration_minutes,
        -- Metadata columns with current timestamp for bronze layer
        CURRENT_TIMESTAMP() as load_timestamp,
        CURRENT_TIMESTAMP() as update_timestamp,
        'ZOOM_PLATFORM' as source_system
    FROM validated_data
    WHERE data_quality_status = 'VALID'  -- Only include valid records
)

SELECT * FROM final_transform
