{{ config(materialized='table') }}

WITH source_data AS (
    SELECT 
        user_id,
        user_name,
        email,
        company,
        plan_type,
        load_timestamp,
        update_timestamp,
        source_system
    FROM {{ source('RAW', 'users') }}
    WHERE user_id IS NOT NULL -- Basic data quality check
),

final AS (
    SELECT 
        user_id,
        user_name,
        email,
        company,
        plan_type,
        COALESCE(load_timestamp, CURRENT_TIMESTAMP()) as load_timestamp,
        COALESCE(update_timestamp, CURRENT_TIMESTAMP()) as update_timestamp,
        COALESCE(source_system, 'ZOOM_PLATFORM') as source_system
    FROM source_data
)

SELECT * FROM final
