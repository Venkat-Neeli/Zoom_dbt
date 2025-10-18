{{ config(
    materialized='table'
) }}

-- Gold Geography Dimension Table
-- Creates geography dimension with default data since geo info not in Silver
WITH geography_base AS (
    SELECT DISTINCT
        source_system,
        load_date,
        update_date
    FROM {{ source('silver', 'si_users') }}
    WHERE record_status = 'active'
    LIMIT 1
),

geography_dimension AS (
    SELECT
        -- Surrogate key generation
        'GD_US' AS geography_dim_id,
        
        -- Default geography data (not available in Silver schema)
        'US' AS country_code,
        'United States' AS country_name,
        'North America' AS region_name,
        'UTC-5' AS time_zone,
        'North America' AS continent,
        
        -- Audit fields
        load_date,
        CURRENT_DATE() AS update_date,
        source_system
        
    FROM geography_base
)

SELECT * FROM geography_dimension
