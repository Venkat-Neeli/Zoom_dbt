{{ config(
    materialized='table',
    cluster_by=['device_type', 'load_date']
) }}

-- Device Dimension Transformation
WITH device_base AS (
    SELECT 
        participant_id,
        'Desktop' AS device_type,
        'Windows' AS operating_system,
        'Unknown' AS application_version,
        'Ethernet' AS network_connection_type,
        'Computer' AS device_category,
        'PC' AS platform_family,
        load_date,
        update_date,
        source_system,
        ROW_NUMBER() OVER (PARTITION BY participant_id ORDER BY update_timestamp DESC) as rn
    FROM {{ source('silver', 'si_participants') }}
    WHERE record_status = 'ACTIVE'
)

SELECT 
    UUID_STRING() AS device_dim_id,
    participant_id AS device_connection_id,
    device_type::VARCHAR(100),
    operating_system::VARCHAR(100),
    application_version::VARCHAR(50),
    network_connection_type::VARCHAR(50),
    device_category::VARCHAR(50),
    platform_family::VARCHAR(50),
    load_date,
    CURRENT_DATE() AS update_date,
    source_system
FROM device_base
WHERE rn = 1
