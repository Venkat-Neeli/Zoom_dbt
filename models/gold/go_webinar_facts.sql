{{
  config(
    materialized='incremental',
    unique_key='webinar_fact_key',
    on_schema_change='fail',
    tags=['gold', 'fact_table']
  )
}}

WITH webinar_base AS (
  SELECT 
    webinar_id,
    host_id,
    webinar_topic,
    start_time,
    end_time,
    registrants,
    load_timestamp,
    update_timestamp,
    source_system,
    data_quality_score,
    record_status
  FROM {{ ref('si_webinars') }}
  WHERE record_status = 'ACTIVE'
    AND data_quality_score >= 0.8
    {% if is_incremental() %}
      AND update_timestamp > (SELECT COALESCE(MAX(last_updated), '1900-01-01') FROM {{ this }})
    {% endif %}
),

host_context AS (
  SELECT 
    user_id,
    user_name,
    company,
    plan_type
  FROM {{ ref('si_users') }}
  WHERE record_status = 'ACTIVE'
)

SELECT 
  {{ dbt_utils.generate_surrogate_key(['w.webinar_id', 'w.host_id', 'w.start_time']) }} as webinar_fact_key,
  w.webinar_id,
  w.host_id,
  w.webinar_topic,
  w.start_time,
  w.end_time,
  DATEDIFF('minute', w.start_time, w.end_time) as webinar_duration_minutes,
  w.registrants,
  CASE 
    WHEN w.registrants > 100 THEN 'Large'
    WHEN w.registrants > 25 THEN 'Medium'
    ELSE 'Small'
  END as webinar_size_category,
  CASE 
    WHEN DATEDIFF('minute', w.start_time, w.end_time) > 90 THEN 'Long'
    WHEN DATEDIFF('minute', w.start_time, w.end_time) > 45 THEN 'Medium'
    ELSE 'Short'
  END as webinar_duration_category,
  h.user_name as host_name,
  h.company as host_company,
  h.plan_type as host_plan_type,
  w.data_quality_score,
  w.source_system,
  CURRENT_TIMESTAMP() as created_at,
  w.update_timestamp as last_updated,
  DATE(w.start_time) as webinar_date,
  EXTRACT(HOUR FROM w.start_time) as webinar_hour,
  DAYNAME(w.start_time) as webinar_day_of_week
FROM webinar_base w
LEFT JOIN host_context h ON w.host_id = h.user_id
