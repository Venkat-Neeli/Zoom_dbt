{{ config(
    materialized='table'
) }}

-- Gold Time Dimension Table
-- Creates comprehensive time dimension from meeting dates
WITH date_spine AS (
    SELECT DISTINCT
        CAST(start_time AS DATE) AS date_key
    FROM {{ source('silver', 'si_meetings') }}
    WHERE start_time IS NOT NULL
      AND record_status = 'ACTIVE'
    
    UNION
    
    SELECT DISTINCT
        CAST(start_time AS DATE) AS date_key
    FROM {{ source('silver', 'si_webinars') }}
    WHERE start_time IS NOT NULL
      AND record_status = 'ACTIVE'
),

time_dimension AS (
    SELECT
        -- Surrogate key generation
        CONCAT('TD_', date_key) AS time_dim_id,
        
        date_key,
        
        -- Date part extractions
        EXTRACT(YEAR FROM date_key) AS year_number,
        EXTRACT(QUARTER FROM date_key) AS quarter_number,
        EXTRACT(MONTH FROM date_key) AS month_number,
        TO_VARCHAR(date_key, 'MMMM') AS month_name,
        EXTRACT(WEEK FROM date_key) AS week_number,
        EXTRACT(DOY FROM date_key) AS day_of_year,
        EXTRACT(DAY FROM date_key) AS day_of_month,
        EXTRACT(DOW FROM date_key) AS day_of_week,
        TO_VARCHAR(date_key, 'DAY') AS day_name,
        
        -- Calculated fields
        CASE WHEN EXTRACT(DOW FROM date_key) IN (0, 6) THEN TRUE ELSE FALSE END AS is_weekend,
        FALSE AS is_holiday, -- Placeholder - no holiday calendar available
        
        -- Fiscal year calculations (assuming calendar year = fiscal year)
        EXTRACT(YEAR FROM date_key) AS fiscal_year,
        EXTRACT(QUARTER FROM date_key) AS fiscal_quarter,
        
        -- Audit fields
        CURRENT_DATE() AS load_date,
        CURRENT_DATE() AS update_date,
        'DERIVED' AS source_system
        
    FROM date_spine
)

SELECT * FROM time_dimension
