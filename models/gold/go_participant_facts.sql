{{
  config(
    materialized='incremental',
    unique_key='participant_fact_key',
    on_schema_change='fail',
    tags=['gold', 'fact_table']
  )
}}

WITH participant_base AS (
  SELECT 
    p.participant_id,
    p.meeting_id,
    p.user_id,
    p.join_time,
    p.leave_time,
    p.load_timestamp,
    p.update_timestamp,
    p.source_system,
    p.data_quality_score,
    p.record_status
  FROM {{ ref('si_participants') }} p
  WHERE p.record_status = 'ACTIVE'
    AND p.data_quality_score >= 0.8
    {% if is_incremental() %}
      AND p.update_timestamp > (SELECT COALESCE(MAX(last_updated), '1900-01-01') FROM {{ this }})
    {% endif %}
),

meeting_context AS (
  SELECT 
    meeting_id,
    host_id,
    meeting_topic,
    start_time,
    end_time,
    duration_minutes
  FROM {{ ref('si_meetings') }}
  WHERE record_status = 'ACTIVE'
),

user_context AS (
  SELECT 
    user_id,
    user_name,
    email,
    company,
    plan_type
  FROM {{ ref('si_users') }}
  WHERE record_status = 'ACTIVE'
)

SELECT 
  {{ dbt_utils.generate_surrogate_key(['p.participant_id', 'p.meeting_id', 'p.user_id']) }} as participant_fact_key,
  p.participant_id,
  p.meeting_id,
  p.user_id,
  p.join_time,
  p.leave_time,
  DATEDIFF('minute', p.join_time, p.leave_time) as participation_duration_minutes,
  CASE 
    WHEN p.join_time <= DATEADD('minute', 5, m.start_time) THEN 'On Time'
    WHEN p.join_time <= DATEADD('minute', 15, m.start_time) THEN 'Late'
    ELSE 'Very Late'
  END as attendance_category,
  CASE 
    WHEN p.leave_time >= DATEADD('minute', -5, m.end_time) THEN 'Full'
    WHEN DATEDIFF('minute', p.join_time, p.leave_time) >= (m.duration_minutes * 0.5) THEN 'Partial'
    ELSE 'Brief'
  END as participation_category,
  m.host_id,
  m.meeting_topic,
  m.duration_minutes as meeting_duration_minutes,
  u.user_name,
  u.company,
  u.plan_type,
  p.data_quality_score,
  p.source_system,
  CURRENT_TIMESTAMP() as created_at,
  p.update_timestamp as last_updated,
  DATE(p.join_time) as participation_date
FROM participant_base p
LEFT JOIN meeting_context m ON p.meeting_id = m.meeting_id
LEFT JOIN user_context u ON p.user_id = u.user_id
