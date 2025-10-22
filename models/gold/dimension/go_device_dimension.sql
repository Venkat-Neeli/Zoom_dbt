{{ config(
    materialized='table',
    cluster_by=['device_type', 'load_date']
) }}

-- Gold Device Dimension
WITH device_base AS (
    SELECT DISTINCT
        'Desktop' AS device_type,
        'Windows' AS operating_system,
        'v5.0.0' AS application_version,
        'WiFi' AS network_connection_type,
        'Computer' AS device_category,
        'Desktop' AS platform_family,
        load_date,
        update_date,
        source_system
    FROM {{ source('silver', 'si_participants') }}
    WHERE record_status = 'ACTIVE'
    LIMIT 1
),

device_dimension AS (
    SELECT 
        UUID_STRING() AS device_dim_id,
        UUID_STRING() AS device_connection_id,
        device_type,
        operating_system,
        application_version,
        network_connection_type,
        device_category,
        platform_family,
        load_date,
        update_date,
        source_system
    FROM device_base
)

SELECT * FROM device_dimension
