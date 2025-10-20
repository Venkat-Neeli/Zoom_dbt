{{
  config(
    materialized='table',
    cluster_by=['load_date', 'meeting_date'],
    pre_hook="ALTER SESSION SET TIMEZONE = 'UTC'",
    post_hook=[
      "ALTER TABLE {{ this }} SET CHANGE_TRACKING = TRUE",
      "GRANT SELECT ON {{ this }} TO ROLE ANALYTICS_READER"
    ]
  )
}}

WITH meeting_base AS (
  SELECT 
    meeting_id,
    meeting_uuid,
    host_id,
    topic,
    meeting_type,
    start_time,
    end_time,
    duration_minutes,
    participants_count,
    timezone,
    created_at,
    updated_at,
    load_date
  FROM {{ ref('silver_meetings') }}
  WHERE load_date >= '{{ var("start_date") }}'
    AND load_date <= '{{ var("end_date") }}'
),

meeting_participants AS (
  SELECT 
    meeting_id,
    COUNT(DISTINCT participant_id) as actual_participants_count,
    SUM(duration_minutes) as total_participant_minutes,
    AVG(duration_minutes) as avg_participant_duration,
    MAX(duration_minutes) as max_participant_duration,
    MIN(duration_minutes) as min_participant_duration
  FROM {{ ref('silver_meeting_participants') }}
  GROUP BY meeting_id
),

meeting_quality AS (
  SELECT 
    meeting_id,
    AVG(audio_quality_score) as avg_audio_quality,
    AVG(video_quality_score) as avg_video_quality,
    AVG(screen_share_quality_score) as avg_screen_share_quality,
    COUNT(CASE WHEN connection_issues > 0 THEN 1 END) as participants_with_issues
  FROM {{ ref('silver_meeting_quality') }}
  GROUP BY meeting_id
)

SELECT 
  -- Primary Keys
  {{ dbt_utils.generate_surrogate_key(['mb.meeting_id', 'mb.load_date']) }} as meeting_fact_key,
  mb.meeting_id,
  mb.meeting_uuid,
  
  -- Foreign Keys
  mb.host_id,
  
  -- Meeting Attributes
  mb.topic,
  mb.meeting_type,
  mb.timezone,
  
  -- Date Dimensions
  DATE(mb.start_time) as meeting_date,
  EXTRACT(YEAR FROM mb.start_time) as meeting_year,
  EXTRACT(MONTH FROM mb.start_time) as meeting_month,
  EXTRACT(DAY FROM mb.start_time) as meeting_day,
  EXTRACT(HOUR FROM mb.start_time) as meeting_hour,
  DAYOFWEEK(mb.start_time) as meeting_day_of_week,
  
  -- Time Attributes
  mb.start_time,
  mb.end_time,
  mb.duration_minutes,
  
  -- Participant Metrics
  mb.participants_count as planned_participants_count,
  COALESCE(mp.actual_participants_count, 0) as actual_participants_count,
  COALESCE(mp.total_participant_minutes, 0) as total_participant_minutes,
  COALESCE(mp.avg_participant_duration, 0) as avg_participant_duration,
  COALESCE(mp.max_participant_duration, 0) as max_participant_duration,
  COALESCE(mp.min_participant_duration, 0) as min_participant_duration,
  
  -- Quality Metrics
  COALESCE(mq.avg_audio_quality, 0) as avg_audio_quality_score,
  COALESCE(mq.avg_video_quality, 0) as avg_video_quality_score,
  COALESCE(mq.avg_screen_share_quality, 0) as avg_screen_share_quality_score,
  COALESCE(mq.participants_with_issues, 0) as participants_with_connection_issues,
  
  -- Calculated Metrics
  CASE 
    WHEN mb.participants_count > 0 
    THEN ROUND((COALESCE(mp.actual_participants_count, 0) * 100.0) / mb.participants_count, 2)
    ELSE 0 
  END as attendance_rate_percent,
  
  CASE 
    WHEN mb.duration_minutes > 0 
    THEN ROUND(COALESCE(mp.total_participant_minutes, 0) / mb.duration_minutes, 2)
    ELSE 0 
  END as engagement_ratio,
  
  -- Audit Fields
  mb.created_at,
  mb.updated_at,
  mb.load_date,
  CURRENT_TIMESTAMP() as dbt_updated_at

FROM meeting_base mb
LEFT JOIN meeting_participants mp ON mb.meeting_id = mp.meeting_id
LEFT JOIN meeting_quality mq ON mb.meeting_id = mq.meeting_id

{{ dbt_utils.group_by(n=25) }}
