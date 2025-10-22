{{ config(
    materialized='table',
    cluster_by=['country_code', 'load_date']
) }}

-- Geography Dimension Transformation
WITH geography_base AS (
    SELECT 
        'US' AS country_code,
        'United States' AS country_name,
        'North America' AS region_name,
        'EST' AS time_zone,
        'North America' AS continent,
        CURRENT_DATE() AS load_date,
        CURRENT_DATE() AS update_date,
        'DEFAULT' AS source_system
    
    UNION ALL
    
    SELECT 
        'CA' AS country_code,
        'Canada' AS country_name,
        'North America' AS region_name,
        'EST' AS time_zone,
        'North America' AS continent,
        CURRENT_DATE() AS load_date,
        CURRENT_DATE() AS update_date,
        'DEFAULT' AS source_system
    
    UNION ALL
    
    SELECT 
        'UK' AS country_code,
        'United Kingdom' AS country_name,
        'Europe' AS region_name,
        'GMT' AS time_zone,
        'Europe' AS continent,
        CURRENT_DATE() AS load_date,
        CURRENT_DATE() AS update_date,
        'DEFAULT' AS source_system
)

SELECT 
    UUID_STRING() AS geography_dim_id,
    country_code::VARCHAR(10),
    country_name::VARCHAR(100),
    region_name::VARCHAR(100),
    time_zone::VARCHAR(50),
    continent::VARCHAR(50),
    load_date,
    update_date,
    source_system
FROM geography_base
