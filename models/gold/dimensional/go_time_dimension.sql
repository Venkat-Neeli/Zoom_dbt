{{ config(
    materialized='table',
    cluster_by=['date_key'],
    tags=['dimension', 'gold'],
    pre_hook="INSERT INTO {{ ref('go_process_audit') }} (execution_id, pipeline_name, process_type, start_time, status, source_system, target_system, user_executed, server_name, load_date, update_date) SELECT '{{ dbt_utils.generate_surrogate_key(['invocation_id', 'this.name']) }}', 'GO_TIME_DIMENSION', 'DIMENSION_BUILD', CURRENT_TIMESTAMP(), 'STARTED', 'SILVER', 'GOLD', CURRENT_USER(), 'DBT_CLOUD', CURRENT_DATE(), CURRENT_DATE() WHERE '{{ this.name }}' != 'go_process_audit'",
    post_hook="UPDATE {{ ref('go_process_audit') }} SET end_time = CURRENT_TIMESTAMP(), status = 'COMPLETED', records_processed = (SELECT COUNT(*) FROM {{ this }}), records_successful = (SELECT COUNT(*) FROM {{ this }}), processing_duration_seconds = DATEDIFF('second', start_time, CURRENT_TIMESTAMP()) WHERE execution_id = '{{ dbt_utils.generate_surrogate_key(['invocation_id', 'this.name']) }}' AND '{{ this.name }}' != 'go_process_audit'"
) }}

-- Gold Time Dimension Table
-- Creates comprehensive time dimension from meeting dates

WITH date_spine AS (
    {{ dbt_utils.date_spine(
        datepart="day",
        start_date="cast('2020-01-01' as date)",
        end_date="cast('2030-12-31' as date)"
    )}}
),

meeting_dates AS (
    SELECT DISTINCT
        CAST(start_time AS DATE) AS date_key,
        source_system,
        load_date,
        update_date
    FROM {{ source('silver', 'si_meetings') }}
    WHERE record_status = 'ACTIVE'
        AND start_time IS NOT NULL
),

time_dimension AS (
    SELECT
        {{ dbt_utils.generate_surrogate_key(['ds.date_day']) }} AS time_dim_id,
        ds.date_day AS date_key,
        EXTRACT(YEAR FROM ds.date_day) AS year_number,
        EXTRACT(QUARTER FROM ds.date_day) AS quarter_number,
        EXTRACT(MONTH FROM ds.date_day) AS month_number,
        TO_VARCHAR(ds.date_day, 'MMMM') AS month_name,
        EXTRACT(WEEK FROM ds.date_day) AS week_number,
        EXTRACT(DOY FROM ds.date_day) AS day_of_year,
        EXTRACT(DAY FROM ds.date_day) AS day_of_month,
        EXTRACT(DOW FROM ds.date_day) AS day_of_week,
        TO_VARCHAR(ds.date_day, 'DAY') AS day_name,
        CASE WHEN EXTRACT(DOW FROM ds.date_day) IN (0,6) THEN TRUE ELSE FALSE END AS is_weekend,
        FALSE AS is_holiday,
        EXTRACT(YEAR FROM ds.date_day) AS fiscal_year,
        EXTRACT(QUARTER FROM ds.date_day) AS fiscal_quarter,
        COALESCE(md.load_date, CURRENT_DATE()) AS load_date,
        CURRENT_DATE() AS update_date,
        COALESCE(md.source_system, 'SYSTEM_GENERATED') AS source_system
    FROM date_spine ds
    LEFT JOIN meeting_dates md ON ds.date_day = md.date_key
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
