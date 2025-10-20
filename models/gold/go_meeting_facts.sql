{{
  config(
    materialized='incremental',
    unique_key='meeting_fact_key',
    on_schema_change='fail',
    pre_hook="{{ log('Starting go_meeting_facts transformation', info=True) }}",
    post_hook="{{ log('Completed go_meeting_facts transformation', info=True) }}"
  )
}}

WITH meeting_base AS (
  SELECT 
    meeting_id,
    host_id,
    meeting_topic,
    start_time,
    end_time,
    duration_minutes,
    load_timestamp,
    update_timestamp,
    source_system,
    load_date,
    update_date,
    data_quality_score,
    record_status,
    -- Generate surrogate key
    {{ dbt_utils.generate_surrogate_key(['meeting_id', 'host_id']) }} AS meeting_fact_key
  FROM {{ ref('si_meetings') }}
  WHERE meeting_id IS NOT NULL
    AND host_id IS NOT NULL
    AND start_time IS NOT NULL
    AND record_status = 'ACTIVE'
    AND data_quality_score >= {{ var('min_quality_score') }}
    {% if is_incremental() %}
      AND update_timestamp > (SELECT MAX(update_timestamp) FROM {{ this }})
    {% endif %}
),

host_info AS (
  SELECT 
    user_id,
    user_name,
    email,
    company,
    plan_type
  FROM {{ ref('si_users') }}
  WHERE user_id IS NOT NULL
    AND record_status = 'ACTIVE'
),

meeting_metrics AS (
  SELECT 
    mb.*,
    hi.user_name AS host_name,
    hi.email AS host_email,
    hi.company AS host_company,
    hi.plan_type AS host_plan_type,
    -- Calculate derived metrics
    CASE 
      WHEN mb.duration_minutes > 0 THEN 1
      ELSE 0 
    END AS valid_duration_flag,
    CASE 
      WHEN mb.duration_minutes >= 60 THEN 'Long'
      WHEN mb.duration_minutes >= 30 THEN 'Medium'
      ELSE 'Short'
    END AS meeting_duration_category,
    CASE 
      WHEN mb.end_time IS NOT NULL THEN 'COMPLETED'
      WHEN mb.start_time <= CURRENT_TIMESTAMP() THEN 'IN_PROGRESS'
      ELSE 'SCHEDULED'
    END AS meeting_status,
    CURRENT_TIMESTAMP() AS dbt_updated_at
  FROM meeting_base mb
  LEFT JOIN host_info hi ON mb.host_id = hi.user_id
)

SELECT 
  meeting_fact_key,
  meeting_id,
  host_id,
  meeting_topic,
  start_time,
  end_time,
  duration_minutes,
  host_name,
  host_email,
  host_company,
  host_plan_type,
  valid_duration_flag,
  meeting_duration_category,
  meeting_status,
  data_quality_score,
  load_timestamp,
  update_timestamp,
  source_system,
  load_date,
  update_date,
  dbt_updated_at
FROM meeting_metrics
WHERE meeting_id IS NOT NULL
  AND host_id IS NOT NULL
