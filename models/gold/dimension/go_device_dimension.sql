{{ config(
    materialized='table',
    cluster_by=['device_type', 'load_date']
) }}

-- Gold Device Dimension Table
WITH device_defaults AS (
    SELECT 'Desktop' as device_type, 'Windows' as operating_system, 'Zoom Client' as application_version, 'WiFi' as network_connection_type, 'Computer' as device_category, 'Windows' as platform_family
    UNION ALL
    SELECT 'Mobile' as device_type, 'iOS' as operating_system, 'Zoom Mobile' as application_version, 'Cellular' as network_connection_type, 'Mobile' as device_category, 'iOS' as platform_family
    UNION ALL
    SELECT 'Mobile' as device_type, 'Android' as operating_system, 'Zoom Mobile' as application_version, 'Cellular' as network_connection_type, 'Mobile' as device_category, 'Android' as platform_family
    UNION ALL
    SELECT 'Tablet' as device_type, 'iPadOS' as operating_system, 'Zoom Mobile' as application_version, 'WiFi' as network_connection_type, 'Tablet' as device_category, 'iOS' as platform_family
    UNION ALL
    SELECT 'Web' as device_type, 'Browser' as operating_system, 'Zoom Web Client' as application_version, 'WiFi' as network_connection_type, 'Web' as device_category, 'Web' as platform_family
),

device_dimension AS (
    SELECT 
        UUID_STRING() as device_dim_id,
        CONCAT('DC_', ROW_NUMBER() OVER (ORDER BY device_type, operating_system)) as device_connection_id,
        device_type,
        operating_system,
        application_version,
        network_connection_type,
        device_category,
        platform_family,
        CURRENT_DATE() as load_date,
        CURRENT_DATE() as update_date,
        'DEFAULT' as source_system
    FROM device_defaults
)

SELECT * FROM device_dimension
