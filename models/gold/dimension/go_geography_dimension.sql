{{ config(
    materialized='table',
    cluster_by=['country_code', 'load_date'],
    pre_hook="INSERT INTO {{ this.schema }}.go_process_audit (execution_id, pipeline_name, process_type, start_time, status, source_system, target_system, user_executed, load_date) SELECT UUID_STRING(), 'Geography Dimension Load', 'DBT_MODEL', CURRENT_TIMESTAMP(), 'STARTED', 'SILVER', 'GOLD', 'DBT_CLOUD', CURRENT_DATE() WHERE '{{ this.name }}' != 'go_process_audit'",
    post_hook="INSERT INTO {{ this.schema }}.go_process_audit (execution_id, pipeline_name, process_type, start_time, end_time, status, records_processed, source_system, target_system, user_executed, processing_duration_seconds, load_date) SELECT UUID_STRING(), 'Geography Dimension Load', 'DBT_MODEL', CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP(), 'COMPLETED', (SELECT COUNT(*) FROM {{ this }}), 'SILVER', 'GOLD', 'DBT_CLOUD', 0, CURRENT_DATE() WHERE '{{ this.name }}' != 'go_process_audit'"
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
