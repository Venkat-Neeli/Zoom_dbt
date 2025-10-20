{{
  config(
    materialized='incremental',
    unique_key='participant_fact_key',
    on_schema_change='fail',
    pre_hook="{{ log('Starting go_participant_facts transformation', info=True) }}",
    post_hook="{{ log('Completed go_participant_facts transformation', info=True) }}"
  )
}}

WITH participant_base AS (
  SELECT 
    participant_id,
    meeting_id,
    user_id,
    join_time,
    leave_time,
    load_timestamp,
    update_timestamp,
    source_system,
    load_date,
    update_date,
    data_quality_score,
    record_status,
    -- Generate surrogate key
    {{ dbt_utils.generate_surrogate_key(['participant_id', 'meeting_id']) }} AS participant_fact_key
  FROM {{ ref('si_participants') }}
  WHERE participant_id IS NOT NULL
    AND meeting_id IS NOT NULL
    AND record_status = 'ACTIVE'
    AND data_quality_score >= {{ var('min_quality_score') }}
    {% if is_incremental() %}
      AND update_timestamp > (SELECT MAX(update_timestamp) FROM {{ this }})
    {% endif %}
),

meeting_context AS (
  SELECT 
    meeting_id,
    host_id,
    meeting_topic,
    start_time AS meeting_start_time,
    end_time AS meeting_end_time,
    duration_minutes AS meeting_duration
  FROM {{ ref('si_meetings') }}
  WHERE meeting_id IS NOT NULL
    AND record_status = 'ACTIVE'
),

user_context AS (
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

participant_metrics AS (
  SELECT 
    pb.*,
    mc.host_id,
    mc.meeting_topic,
    mc.meeting_start_time,
    mc.meeting_end_time,
    mc.meeting_duration,
    uc.user_name AS participant_name,
    uc.email AS participant_email,
    uc.company AS participant_company,
    uc.plan_type AS participant_plan_type,
    -- Calculate engagement metrics
    CASE 
      WHEN pb.join_time IS NOT NULL AND pb.leave_time IS NOT NULL THEN
        DATEDIFF('minute', pb.join_time, pb.leave_time)
      ELSE 0
    END AS participation_duration_minutes,
    CASE 
      WHEN pb.join_time <= mc.meeting_start_time + INTERVAL '5 minutes' THEN 'On Time'
      ELSE 'Late'
    END AS attendance_punctuality,
    CASE 
      WHEN pb.user_id = mc.host_id THEN 'Host'
      ELSE 'Participant'
    END AS participant_role,
    CURRENT_TIMESTAMP() AS dbt_updated_at
  FROM participant_base pb
  LEFT JOIN meeting_context mc ON pb.meeting_id = mc.meeting_id
  LEFT JOIN user_context uc ON pb.user_id = uc.user_id
)

SELECT 
  participant_fact_key,
  participant_id,
  meeting_id,
  user_id,
  join_time,
  leave_time,
  host_id,
  meeting_topic,
  meeting_start_time,
  meeting_end_time,
  meeting_duration,
  participant_name,
  participant_email,
  participant_company,
  participant_plan_type,
  participation_duration_minutes,
  attendance_punctuality,
  participant_role,
  data_quality_score,
  load_timestamp,
  update_timestamp,
  source_system,
  load_date,
  update_date,
  dbt_updated_at
FROM participant_metrics
WHERE participant_id IS NOT NULL
  AND meeting_id IS NOT NULL
