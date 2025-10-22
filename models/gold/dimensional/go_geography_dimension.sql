{{ config(
    materialized='table',
    cluster_by=['country_code', 'load_date'],
    tags=['dimension', 'gold'],
    pre_hook="INSERT INTO {{ ref('go_process_audit') }} (execution_id, pipeline_name, process_type, start_time, status, source_system, target_system, user_executed, server_name, load_date, update_date) SELECT '{{ dbt_utils.generate_surrogate_key(['invocation_id', 'this.name']) }}', 'GO_GEOGRAPHY_DIMENSION', 'DIMENSION_BUILD', CURRENT_TIMESTAMP(), 'STARTED', 'SILVER', 'GOLD', CURRENT_USER(), 'DBT_CLOUD', CURRENT_DATE(), CURRENT_DATE() WHERE '{{ this.name }}' != 'go_process_audit'",
    post_hook="UPDATE {{ ref('go_process_audit') }} SET end_time = CURRENT_TIMESTAMP(), status = 'COMPLETED', records_processed = (SELECT COUNT(*) FROM {{ this }}), records_successful = (SELECT COUNT(*) FROM {{ this }}), processing_duration_seconds = DATEDIFF('second', start_time, CURRENT_TIMESTAMP()) WHERE execution_id = '{{ dbt_utils.generate_surrogate_key(['invocation_id', 'this.name']) }}' AND '{{ this.name }}' != 'go_process_audit'"
) }}

-- Gold Geography Dimension Table
-- Creates geography dimension with default values since geography info not available in Silver

WITH geography_base AS (
    SELECT
        'US' AS country_code,
        'United States' AS country_name,
        'North America' AS region_name,
        'America/New_York' AS time_zone,
        'North America' AS continent,
        CURRENT_DATE() AS load_date,
        CURRENT_DATE() AS update_date,
        'SYSTEM_GENERATED' AS source_system
    
    UNION ALL
    
    SELECT
        'CA' AS country_code,
        'Canada' AS country_name,
        'North America' AS region_name,
        'America/Toronto' AS time_zone,
        'North America' AS continent,
        CURRENT_DATE() AS load_date,
        CURRENT_DATE() AS update_date,
        'SYSTEM_GENERATED' AS source_system
        
    UNION ALL
    
    SELECT
        'UK' AS country_code,
        'United Kingdom' AS country_name,
        'Europe' AS region_name,
        'Europe/London' AS time_zone,
        'Europe' AS continent,
        CURRENT_DATE() AS load_date,
        CURRENT_DATE() AS update_date,
        'SYSTEM_GENERATED' AS source_system
),

geography_dimension AS (
    SELECT
        {{ dbt_utils.generate_surrogate_key(['country_code']) }} AS geography_dim_id,
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

SELECT
    geography_dim_id::VARCHAR(50) AS geography_dim_id,
    country_code::VARCHAR(10) AS country_code,
    country_name::VARCHAR(100) AS country_name,
    region_name::VARCHAR(100) AS region_name,
    time_zone::VARCHAR(50) AS time_zone,
    continent::VARCHAR(50) AS continent,
    load_date,
    update_date,
    source_system::VARCHAR(100) AS source_system
FROM geography_dimension
