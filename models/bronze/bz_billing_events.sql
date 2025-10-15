{{ config(materialized='table') }}

WITH source_data AS (
    SELECT 
        event_id,
        user_id,
        event_type,
        amount,
        event_date,
        load_timestamp,
        update_timestamp,
        source_system
    FROM {{ source('RAW', 'billing_events') }}
    WHERE event_id IS NOT NULL -- Basic data quality check
),

final AS (
    SELECT 
        event_id,
        user_id,
        event_type,
        amount,
        event_date,
        COALESCE(load_timestamp, CURRENT_TIMESTAMP()) as load_timestamp,
        COALESCE(update_timestamp, CURRENT_TIMESTAMP()) as update_timestamp,
        COALESCE(source_system, 'ZOOM_PLATFORM') as source_system
    FROM source_data
)

SELECT * FROM final
