{{ config(
    materialized='table',
    cluster_by=['date_key'],
    tags=['dimension', 'gold'],
    pre_hook="INSERT INTO {{ ref('go_process_audit') }} (execution_id, pipeline_name, process_type, start_time, status, source_system, target_system, load_date, update_date) SELECT '{{ dbt_utils.generate_surrogate_key(['go_time_dimension', run_started_at]) }}', 'go_time_dimension', 'DIMENSION_LOAD', CURRENT_TIMESTAMP(), 'STARTED', 'SILVER', 'GOLD', CURRENT_DATE(), CURRENT_DATE() WHERE '{{ this.name }}' != 'go_process_audit'",
    post_hook="UPDATE {{ ref('go_process_audit') }} SET end_time = CURRENT_TIMESTAMP(), status = 'COMPLETED', processing_duration_seconds = DATEDIFF('second', start_time, CURRENT_TIMESTAMP()) WHERE execution_id = '{{ dbt_utils.generate_surrogate_key(['go_time_dimension', run_started_at]) }}' AND '{{ this.name }}' != 'go_process_audit'"
) }}

WITH date_spine AS (
    SELECT DISTINCT 
        CAST(m.start_time AS DATE) AS date_key
    FROM {{ source('silver', 'si_meetings') }} m
    WHERE m.record_status = 'ACTIVE'
        AND m.data_quality_score >= 0.7
        AND m.start_time IS NOT NULL
    
    UNION 
    
    SELECT DISTINCT 
        CAST(w.start_time AS DATE) AS date_key
    FROM {{ source('silver', 'si_webinars') }} w
    WHERE w.record_status = 'ACTIVE'
        AND w.data_quality_score >= 0.7
        AND w.start_time IS NOT NULL
        
    UNION 
    
    SELECT DISTINCT 
        fu.usage_date AS date_key
    FROM {{ source('silver', 'si_feature_usage') }} fu
    WHERE fu.record_status = 'ACTIVE'
        AND fu.data_quality_score >= 0.7
        AND fu.usage_date IS NOT NULL
),

time_dimension AS (
    SELECT 
        {{ dbt_utils.generate_surrogate_key(['date_key']) }} AS time_dim_id,
        date_key,
        EXTRACT(YEAR FROM date_key) AS year_number,
        EXTRACT(QUARTER FROM date_key) AS quarter_number,
        EXTRACT(MONTH FROM date_key) AS month_number,
        TO_VARCHAR(date_key, 'MMMM') AS month_name,
        EXTRACT(WEEK FROM date_key) AS week_number,
        EXTRACT(DOY FROM date_key) AS day_of_year,
        EXTRACT(DAY FROM date_key) AS day_of_month,
        EXTRACT(DOW FROM date_key) AS day_of_week,
        TO_VARCHAR(date_key, 'DAY') AS day_name,
        CASE WHEN EXTRACT(DOW FROM date_key) IN (0,6) THEN TRUE ELSE FALSE END AS is_weekend,
        FALSE AS is_holiday,
        EXTRACT(YEAR FROM date_key) AS fiscal_year,
        EXTRACT(QUARTER FROM date_key) AS fiscal_quarter,
        CURRENT_DATE() AS load_date,
        CURRENT_DATE() AS update_date,
        'DERIVED' AS source_system
    FROM date_spine
    WHERE date_key IS NOT NULL
)

SELECT 
    time_dim_id::VARCHAR(50) AS time_dim_id,
    date_key,
    year_number,
    quarter_number,
    month_number,
    month_name::VARCHAR(20) AS month_name,
    week_number,
    day_of_year,
    day_of_month,
    day_of_week,
    TRIM(day_name)::VARCHAR(20) AS day_name,
    is_weekend,
    is_holiday,
    fiscal_year,
    fiscal_quarter,
    load_date,
    update_date,
    source_system::VARCHAR(100) AS source_system
FROM time_dimension
ORDER BY date_key
