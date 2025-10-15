{{ config(materialized='table') }}

-- Bronze layer transformation for billing_events data
-- This model performs 1:1 mapping from raw billing_events to bronze billing_events with data quality checks

WITH source_data AS (
    SELECT 
        event_id,
        user_id,
        event_type,
        amount,
        event_date,
        load_timestamp as source_load_timestamp,
        update_timestamp as source_update_timestamp,
        source_system as source_source_system
    FROM {{ source('raw_data', 'billing_events') }}
),

-- Data quality and validation layer
validated_data AS (
    SELECT 
        event_id,
        user_id,
        event_type,
        amount,
        event_date,
        source_load_timestamp,
        source_update_timestamp,
        source_source_system,
        -- Add data quality flags
        CASE 
            WHEN event_id IS NULL THEN 'MISSING_EVENT_ID'
            WHEN user_id IS NULL THEN 'MISSING_USER_ID'
            ELSE 'VALID'
        END as data_quality_status
    FROM source_data
),

-- Final transformation with audit columns
final_transform AS (
    SELECT 
        event_id,
        user_id,
        event_type,
        amount,
        event_date,
        -- Metadata columns with current timestamp for bronze layer
        CURRENT_TIMESTAMP() as load_timestamp,
        CURRENT_TIMESTAMP() as update_timestamp,
        'ZOOM_PLATFORM' as source_system
    FROM validated_data
    WHERE data_quality_status = 'VALID'  -- Only include valid records
)

SELECT * FROM final_transform
