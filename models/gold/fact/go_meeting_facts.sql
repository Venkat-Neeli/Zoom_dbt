{{
  config(
    materialized='incremental',
    unique_key='meeting_fact_id',
    on_schema_change='sync_all_columns',
    pre_hook="INSERT INTO {{ target.schema }}.audit_log (table_name, operation, start_time, user_name) VALUES ('go_meeting_facts', 'transform_start', CURRENT_TIMESTAMP(), CURRENT_USER())",
    post_hook="INSERT INTO {{ target.schema }}.audit_log (table_name, operation, end_time, user_name, records_processed) VALUES ('go_meeting_facts', 'transform_end', CURRENT_TIMESTAMP(), CURRENT_USER(), (SELECT COUNT(*) FROM {{ this }}))"
  )
}}

WITH meeting_base AS (
  SELECT 
    meeting_id,
    host_id,
    TRIM(meeting_topic) AS meeting_topic,
    CONVERT_TIMEZONE('UTC', start_time) AS start_time,
    CONVERT_TIMEZONE('UTC', end_time) AS end_time,
    COALESCE(duration_minutes, 0) AS duration_minutes,
    load_timestamp,
    update_timestamp,
    source_system,
    load_date,
    update_date,
    data_quality_score
  FROM {{ source('silver', 'si_meetings') }}
  WHERE record_status = 'ACTIVE'
    AND data_quality_score >= {{ var('min_quality_score') }}
    AND meeting_id IS NOT NULL
    {% if is_incremental() %}
      AND update_timestamp > (SELECT MAX(update_date) FROM {{ this }})
    {% endif %}
),

participant_metrics AS (
  SELECT 
    meeting_id,
    COUNT(DISTINCT participant_id) AS participant_count,
    COUNT(DISTINCT user_id) AS unique_user_count,
    SUM(DATEDIFF('minute', join_time, leave_time)) AS total_attendance_minutes,
    AVG(DATEDIFF('minute', join_time, leave_time)) AS average_attendance_duration
  FROM {{ source('silver', 'si_participants') }}
  WHERE record_status = 'ACTIVE'
    AND data_quality_score >= {{ var('min_quality_score') }}
    AND join_time IS NOT NULL
    AND leave_time IS NOT NULL
  GROUP BY meeting_id
),

feature_metrics AS (
  SELECT 
    meeting_id,
    SUM(CASE WHEN feature_name = 'screen_share' THEN usage_count ELSE 0 END) AS screen_share_count,
    SUM(CASE WHEN feature_name = 'chat' THEN usage_count ELSE 0 END) AS chat_message_count,
    SUM(CASE WHEN feature_name = 'breakout_room' THEN usage_count ELSE 0 END) AS breakout_room_count,
    COUNT(DISTINCT feature_name) AS total_features_used
  FROM {{ source('silver', 'si_feature_usage') }}
  WHERE record_status = 'ACTIVE'
    AND data_quality_score >= {{ var('min_quality_score') }}
  GROUP BY meeting_id
)

SELECT 
  {{ dbt_utils.generate_surrogate_key(['mb.meeting_id', 'mb.host_id']) }} AS meeting_fact_id,
  mb.meeting_id,
  mb.host_id,
  mb.meeting_topic,
  mb.start_time,
  mb.end_time,
  mb.duration_minutes,
  COALESCE(pm.participant_count, 0) AS participant_count,
  COALESCE(pm.participant_count, 0) AS max_concurrent_participants,
  COALESCE(pm.total_attendance_minutes, 0) AS total_attendance_minutes,
  COALESCE(pm.average_attendance_duration, 0) AS average_attendance_duration,
  CASE 
    WHEN mb.duration_minutes > 60 THEN 'Long Meeting'
    WHEN mb.duration_minutes > 30 THEN 'Medium Meeting'
    ELSE 'Short Meeting'
  END AS meeting_type,
  CASE 
    WHEN mb.end_time IS NOT NULL THEN 'Completed'
    ELSE 'In Progress'
  END AS meeting_status,
  CASE WHEN fm.total_features_used > 0 THEN TRUE ELSE FALSE END AS recording_enabled,
  COALESCE(fm.screen_share_count, 0) AS screen_share_count,
  COALESCE(fm.chat_message_count, 0) AS chat_message_count,
  COALESCE(fm.breakout_room_count, 0) AS breakout_room_count,
  COALESCE(mb.data_quality_score, 0) AS quality_score_avg,
  CASE 
    WHEN COALESCE(pm.participant_count, 0) > 10 AND COALESCE(fm.total_features_used, 0) > 3 THEN 'High'
    WHEN COALESCE(pm.participant_count, 0) > 5 AND COALESCE(fm.total_features_used, 0) > 1 THEN 'Medium'
    ELSE 'Low'
  END AS engagement_score,
  mb.load_date,
  mb.update_date,
  mb.source_system,
  {{ dbt_utils.generate_surrogate_key(['mb.meeting_id', 'mb.host_id']) }} AS surrogate_key
FROM meeting_base mb
LEFT JOIN participant_metrics pm ON mb.meeting_id = pm.meeting_id
LEFT JOIN feature_metrics fm ON mb.meeting_id = fm.meeting_id
