{{ config(
    materialized='table'
) }}

-- Gold Device Dimension Table
-- Creates device dimension with placeholder data since device info not in Silver
WITH device_base AS (
    SELECT DISTINCT
        participant_id,
        source_system,
        load_date,
        update_date
    FROM {{ source('silver', 'si_participants') }}
    WHERE record_status = 'ACTIVE'
),

device_dimension AS (
    SELECT
        -- Surrogate key generation
        CONCAT('DD_', participant_id) AS device_dim_id,
        
        -- Use participant_id as device_connection_id since no device info available
        CONCAT('DC_', participant_id) AS device_connection_id,
        
        -- Placeholder fields (not available in Silver schema)
        'Desktop' AS device_type,
        'Windows' AS operating_system,
        '5.0.0' AS application_version,
        'WiFi' AS network_connection_type,
        'Computer' AS device_category,
        'Desktop' AS platform_family,
        
        -- Audit fields
        load_date,
        CURRENT_DATE() AS update_date,
        source_system
        
    FROM device_base
)

SELECT * FROM device_dimension
