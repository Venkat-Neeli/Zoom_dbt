{{ config(
    materialized='table',
    cluster_by=['country_code', 'load_date']
) }}

-- Gold Geography Dimension Table
WITH geography_defaults AS (
    SELECT 'US' as country_code, 'United States' as country_name, 'North America' as region_name, 'America/New_York' as time_zone, 'North America' as continent
    UNION ALL
    SELECT 'CA' as country_code, 'Canada' as country_name, 'North America' as region_name, 'America/Toronto' as time_zone, 'North America' as continent
    UNION ALL
    SELECT 'GB' as country_code, 'United Kingdom' as country_name, 'Europe' as region_name, 'Europe/London' as time_zone, 'Europe' as continent
    UNION ALL
    SELECT 'DE' as country_code, 'Germany' as country_name, 'Europe' as region_name, 'Europe/Berlin' as time_zone, 'Europe' as continent
    UNION ALL
    SELECT 'FR' as country_code, 'France' as country_name, 'Europe' as region_name, 'Europe/Paris' as time_zone, 'Europe' as continent
    UNION ALL
    SELECT 'JP' as country_code, 'Japan' as country_name, 'Asia Pacific' as region_name, 'Asia/Tokyo' as time_zone, 'Asia' as continent
    UNION ALL
    SELECT 'AU' as country_code, 'Australia' as country_name, 'Asia Pacific' as region_name, 'Australia/Sydney' as time_zone, 'Oceania' as continent
    UNION ALL
    SELECT 'IN' as country_code, 'India' as country_name, 'Asia Pacific' as region_name, 'Asia/Kolkata' as time_zone, 'Asia' as continent
    UNION ALL
    SELECT 'BR' as country_code, 'Brazil' as country_name, 'South America' as region_name, 'America/Sao_Paulo' as time_zone, 'South America' as continent
    UNION ALL
    SELECT 'UNKNOWN' as country_code, 'Unknown' as country_name, 'Unknown' as region_name, 'UTC' as time_zone, 'Unknown' as continent
),

geography_dimension AS (
    SELECT 
        UUID_STRING() as geography_dim_id,
        country_code,
        country_name,
        region_name,
        time_zone,
        continent,
        CURRENT_DATE() as load_date,
        CURRENT_DATE() as update_date,
        'DEFAULT' as source_system
    FROM geography_defaults
)

SELECT * FROM geography_dimension
