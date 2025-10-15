{{ config(materialized='table') }}

WITH source_data AS (
    SELECT 
        ticket_id,
        user_id,
        ticket_type,
        resolution_status,
        open_date,
        load_timestamp,
        update_timestamp,
        source_system
    FROM {{ source('RAW', 'support_tickets') }}
    WHERE ticket_id IS NOT NULL -- Basic data quality check
),

final AS (
    SELECT 
        ticket_id,
        user_id,
        ticket_type,
        resolution_status,
        open_date,
        COALESCE(load_timestamp, CURRENT_TIMESTAMP()) as load_timestamp,
        COALESCE(update_timestamp, CURRENT_TIMESTAMP()) as update_timestamp,
        COALESCE(source_system, 'ZOOM_PLATFORM') as source_system
    FROM source_data
)

SELECT * FROM final
