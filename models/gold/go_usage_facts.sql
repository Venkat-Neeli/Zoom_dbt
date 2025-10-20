{{
  config(
    materialized='incremental',
    unique_key='usage_fact_key',
    on_schema_change='fail',
    tags=['gold', 'fact_table']
  )
}}

WITH usage_base AS (
  SELECT 
    usage_id,
    meeting_id,
    feature_name,
    usage_count,
    usage_date,
    load_timestamp,
    update_timestamp,
    source_system,
    data_quality_score,
    record_status
  FROM {{ ref('si_feature_usage') }}
  WHERE record_status = 'ACTIVE'
    AND data_quality_score >= 0.8
    {% if is_incremental() %}
      AND update_timestamp > (SELECT COALESCE(MAX(last_updated), '1900-01-01') FROM {{ this }})
    {% endif %}
),

meeting_context AS (
  SELECT 
    meeting_id,
    host_id,
    meeting_topic,
    duration_minutes
  FROM {{ ref('si_meetings') }}
  WHERE record_status = 'ACTIVE'
),

user_context AS (
  SELECT 
    user_id,
    user_name,
    company,
    plan_type
  FROM {{ ref('si_users') }}
  WHERE record_status = 'ACTIVE'
)

SELECT 
  {{ dbt_utils.generate_surrogate_key(['u.usage_id', 'u.meeting_id', 'u.feature_name']) }} as usage_fact_key,
  u.usage_id,
  u.meeting_id,
  u.feature_name,
  u.usage_count,
  u.usage_date,
  CASE 
    WHEN u.usage_count > 10 THEN 'Heavy'
    WHEN u.usage_count > 3 THEN 'Moderate'
    ELSE 'Light'
  END as usage_intensity,
  CASE 
    WHEN u.feature_name IN ('screen_share', 'recording', 'breakout_rooms') THEN 'Premium'
    WHEN u.feature_name IN ('chat', 'reactions', 'polls') THEN 'Interactive'
    ELSE 'Basic'
  END as feature_category,
  m.host_id,
  m.meeting_topic,
  m.duration_minutes as meeting_duration_minutes,
  usr.user_name as host_name,
  usr.company as host_company,
  usr.plan_type as host_plan_type,
  u.data_quality_score,
  u.source_system,
  CURRENT_TIMESTAMP() as created_at,
  u.update_timestamp as last_updated,
  EXTRACT(YEAR FROM u.usage_date) as usage_year,
  EXTRACT(MONTH FROM u.usage_date) as usage_month,
  EXTRACT(QUARTER FROM u.usage_date) as usage_quarter
FROM usage_base u
LEFT JOIN meeting_context m ON u.meeting_id = m.meeting_id
LEFT JOIN user_context usr ON m.host_id = usr.user_id
