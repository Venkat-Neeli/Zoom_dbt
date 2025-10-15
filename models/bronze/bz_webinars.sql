{{ config(materialized='table') }}

WITH source_data AS (
    SELECT 
        webinar_id,
        host_id,
        webinar_topic,
        start_time,
        end_time,
        registrants,
        load_timestamp,
        update_timestamp,
        source_system
    FROM {{ source('RAW', 'webinars') }}
    WHERE webinar_id IS NOT NULL -- Basic data quality check
),

final AS (
    SELECT 
        webinar_id,
        host_id,
        webinar_topic,
        start_time,
        end_time,
        registrants,
        COALESCE(load_timestamp, CURRENT_TIMESTAMP()) as load_timestamp,
        COALESCE(update_timestamp, CURRENT_TIMESTAMP()) as update_timestamp,
        COALESCE(source_system, 'ZOOM_PLATFORM') as source_system
    FROM source_data
)

SELECT * FROM final
