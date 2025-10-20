{{
  config(
    materialized='incremental',
    unique_key='meeting_fact_key',
    incremental_strategy='merge',
    on_schema_change='sync_all_columns',
    tags=['gold', 'fact', 'meeting'],
    pre_hook="{{ log('Starting meeting facts transformation', info=True) }}",
    post_hook=[
      "{{ log('Completed meeting facts transformation', info=True) }}"
    ]
  )
}}

-- Meeting Facts Transformation from Silver to Gold Layer
WITH meeting_base AS (
  SELECT 
    meeting_id,
    host_id,
    topic,
    start_time,
    end_time,
    duration,
    participants_count,
    meeting_type,
    timezone,
    created_at,
    updated_at,
    -- Generate surrogate key
    {{ dbt_utils.generate_surrogate_key(['meeting_id', 'host_id']) }} AS meeting_fact_key,
    -- Audit fields
    CURRENT_TIMESTAMP() AS dbt_loaded_at,
    CURRENT_USER() AS dbt_loaded_by
  FROM {{ ref('silver_meetings') }}
  WHERE meeting_id IS NOT NULL
    AND host_id IS NOT NULL
    AND start_time IS NOT NULL
  
  {% if is_incremental() %}
    -- Incremental logic: only process new or updated records
    AND (created_at > (SELECT MAX(created_at) FROM {{ this }})
         OR updated_at > (SELECT MAX(updated_at) FROM {{ this }}))
  {% endif %}
),

meeting_metrics AS (
  SELECT 
    mb.*,
    -- Calculate derived metrics
    CASE 
      WHEN duration > 0 THEN participants_count / duration * 60
      ELSE 0 
    END AS participants_per_minute,
    
    CASE 
      WHEN duration <= 30 THEN 'Short'
      WHEN duration <= 60 THEN 'Medium'
      WHEN duration <= 120 THEN 'Long'
      ELSE 'Extended'
    END AS duration_category,
    
    -- Time-based dimensions
    DATE(start_time) AS meeting_date,
    EXTRACT(HOUR FROM start_time) AS meeting_hour,
    EXTRACT(DOW FROM start_time) AS day_of_week,
    EXTRACT(WEEK FROM start_time) AS week_of_year,
    EXTRACT(MONTH FROM start_time) AS month_of_year,
    EXTRACT(QUARTER FROM start_time) AS quarter_of_year,
    EXTRACT(YEAR FROM start_time) AS year
    
  FROM meeting_base mb
),

final AS (
  SELECT 
    meeting_fact_key,
    meeting_id,
    host_id,
    topic,
    start_time,
    end_time,
    duration,
    participants_count,
    meeting_type,
    timezone,
    participants_per_minute,
    duration_category,
    meeting_date,
    meeting_hour,
    day_of_week,
    week_of_year,
    month_of_year,
    quarter_of_year,
    year,
    created_at,
    updated_at,
    dbt_loaded_at,
    dbt_loaded_by,
    -- Data quality score
    CASE 
      WHEN topic IS NOT NULL 
           AND duration > 0 
           AND participants_count > 0 
           AND timezone IS NOT NULL 
      THEN 1.0
      ELSE 0.8
    END AS data_quality_score
  FROM meeting_metrics
  WHERE data_quality_score >= {{ var('min_quality_score') }}
)

SELECT * FROM final
