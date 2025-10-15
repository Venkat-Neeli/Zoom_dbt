{{ config(materialized='table') }}

WITH source_data AS (
    SELECT 
        meeting_id,
        host_id,
        meeting_topic,
        start_time,
        end_time,
        duration_minutes,
        load_timestamp,
        update_timestamp,
        source_system
    FROM {{ source('RAW', 'meetings') }}
    WHERE meeting_id IS NOT NULL -- Basic data quality check
),

final AS (
    SELECT 
        meeting_id,
        host_id,
        meeting_topic,
        start_time,
        end_time,
        duration_minutes,
        COALESCE(load_timestamp, CURRENT_TIMESTAMP()) as load_timestamp,
        COALESCE(update_timestamp, CURRENT_TIMESTAMP()) as update_timestamp,
        COALESCE(source_system, 'ZOOM_PLATFORM') as source_system
    FROM source_data
)

SELECT * FROM final
