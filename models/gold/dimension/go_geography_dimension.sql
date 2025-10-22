{{ config(
    materialized='table',
    cluster_by=['country_code', 'load_date']
) }}

-- Gold Geography Dimension
WITH geography_base AS (
    SELECT 
        'US' AS country_code,
        'United States' AS country_name,
        'North America' AS region_name,
        'America/New_York' AS time_zone,
        'North America' AS continent,
        CURRENT_DATE() AS load_date,
        CURRENT_DATE() AS update_date,
        'SYSTEM_DEFAULT' AS source_system
),

geography_dimension AS (
    SELECT 
        UUID_STRING() AS geography_dim_id,
        country_code,
        country_name,
        region_name,
        time_zone,
        continent,
        load_date,
        update_date,
        source_system
    FROM geography_base
)

SELECT * FROM geography_dimension
