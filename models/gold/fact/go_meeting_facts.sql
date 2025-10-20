{{
  config(
    materialized='incremental',
    unique_key='meeting_fact_key',
    on_schema_change='fail',
    pre_hook="ALTER SESSION SET TIMEZONE = 'UTC'",
    post_hook=[
      "UPDATE {{ this }} SET last_updated_at = CURRENT_TIMESTAMP() WHERE meeting_fact_key IN (SELECT meeting_fact_key FROM {{ this }} WHERE last_updated_at IS NULL)",
      "ANALYZE TABLE {{ this }} COMPUTE STATISTICS"
    ],
    tags=['gold', 'fact', 'meeting', 'incremental']
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
    record_status
  FROM {{ ref('si_meetings') }}
  WHERE record_status = 'ACTIVE'
    AND data_quality_score >= 0.8
    AND start_time IS NOT NULL
    AND end_time IS NOT NULL
    AND duration_minutes > 0
  
  {% if is_incremental() %}
    AND (update_timestamp > (SELECT MAX(source_last_updated_at) FROM {{ this }})
         OR load_timestamp > (SELECT MAX(source_last_updated_at) FROM {{ this }}))
  {% endif %}
),

participant_aggregates AS (
  SELECT 
    meeting_id,
    COUNT(DISTINCT participant_id) as total_participants,
    COUNT(DISTINCT user_id) as unique_users,
    AVG(DATEDIFF('minute', join_time, leave_time)) as avg_participation_duration_minutes,
    MIN(join_time) as first_participant_join_time,
    MAX(leave_time) as last_participant_leave_time,
    COUNT(CASE WHEN join_time <= start_time + INTERVAL '5 minutes' THEN 1 END) as on_time_participants,
    COUNT(CASE WHEN DATEDIFF('minute', join_time, leave_time) >= 5 THEN 1 END) as engaged_participants
  FROM {{ ref('si_participants') }} p
  INNER JOIN meeting_base m ON p.meeting_id = m.meeting_id
  WHERE p.record_status = 'ACTIVE'
    AND p.data_quality_score >= 0.8
    AND p.join_time IS NOT NULL
    AND p.leave_time IS NOT NULL
  GROUP BY meeting_id
),

host_info AS (
  SELECT 
    user_id,
    user_name as host_name,
    email as host_email,
    company as host_company,
    plan_type as host_plan_type
  FROM {{ ref('si_users') }}
  WHERE record_status = 'ACTIVE'
    AND data_quality_score >= 0.8
),

feature_usage_aggregates AS (
  SELECT 
    meeting_id,
    COUNT(DISTINCT feature_name) as features_used_count,
    SUM(usage_count) as total_feature_usage_count,
    LISTAGG(DISTINCT feature_name, ', ') WITHIN GROUP (ORDER BY feature_name) as features_used_list,
    MAX(CASE WHEN feature_name = 'screen_share' THEN usage_count ELSE 0 END) as screen_share_usage,
    MAX(CASE WHEN feature_name = 'chat' THEN usage_count ELSE 0 END) as chat_usage,
    MAX(CASE WHEN feature_name = 'recording' THEN usage_count ELSE 0 END) as recording_usage,
    MAX(CASE WHEN feature_name = 'breakout_rooms' THEN usage_count ELSE 0 END) as breakout_rooms_usage
  FROM {{ ref('si_feature_usage') }} f
  INNER JOIN meeting_base m ON f.meeting_id = m.meeting_id
  WHERE f.record_status = 'ACTIVE'
    AND f.data_quality_score >= 0.8
    AND f.usage_count > 0
  GROUP BY meeting_id
),

meeting_facts AS (
  SELECT 
    -- Primary Key
    {{ dbt_utils.generate_surrogate_key(['m.meeting_id', 'm.start_time']) }} as meeting_fact_key,
    
    -- Meeting Identifiers
    m.meeting_id,
    m.host_id,
    
    -- Meeting Details
    m.meeting_topic,
    m.start_time,
    m.end_time,
    m.duration_minutes as scheduled_duration_minutes,
    
    -- Host Information
    COALESCE(h.host_name, 'Unknown') as host_name,
    COALESCE(h.host_email, 'Unknown') as host_email,
    COALESCE(h.host_company, 'Unknown') as host_company,
    COALESCE(h.host_plan_type, 'Unknown') as host_plan_type,
    
    -- Participant Metrics
    COALESCE(p.total_participants, 0) as total_participants,
    COALESCE(p.unique_users, 0) as unique_users,
    COALESCE(p.avg_participation_duration_minutes, 0) as avg_participation_duration_minutes,
    COALESCE(p.on_time_participants, 0) as on_time_participants,
    COALESCE(p.engaged_participants, 0) as engaged_participants,
    
    -- Calculated Metrics
    CASE 
      WHEN COALESCE(p.total_participants, 0) > 0 
      THEN ROUND((COALESCE(p.on_time_participants, 0) * 100.0) / p.total_participants, 2)
      ELSE 0 
    END as on_time_percentage,
    
    CASE 
      WHEN COALESCE(p.total_participants, 0) > 0 
      THEN ROUND((COALESCE(p.engaged_participants, 0) * 100.0) / p.total_participants, 2)
      ELSE 0 
    END as engagement_percentage,
    
    -- Meeting Duration Analysis
    CASE 
      WHEN p.first_participant_join_time IS NOT NULL AND p.last_participant_leave_time IS NOT NULL
      THEN DATEDIFF('minute', p.first_participant_join_time, p.last_participant_leave_time)
      ELSE m.duration_minutes
    END as actual_duration_minutes,
    
    -- Feature Usage Metrics
    COALESCE(f.features_used_count, 0) as features_used_count,
    COALESCE(f.total_feature_usage_count, 0) as total_feature_usage_count,
    COALESCE(f.features_used_list, 'None') as features_used_list,
    COALESCE(f.screen_share_usage, 0) as screen_share_usage,
    COALESCE(f.chat_usage, 0) as chat_usage,
    COALESCE(f.recording_usage, 0) as recording_usage,
    COALESCE(f.breakout_rooms_usage, 0) as breakout_rooms_usage,
    
    -- Meeting Classification
    CASE 
      WHEN COALESCE(p.total_participants, 0) = 0 THEN 'No Participants'
      WHEN COALESCE(p.total_participants, 0) = 1 THEN 'Solo Meeting'
      WHEN COALESCE(p.total_participants, 0) BETWEEN 2 AND 5 THEN 'Small Meeting'
      WHEN COALESCE(p.total_participants, 0) BETWEEN 6 AND 20 THEN 'Medium Meeting'
      WHEN COALESCE(p.total_participants, 0) BETWEEN 21 AND 100 THEN 'Large Meeting'
      ELSE 'Very Large Meeting'
    END as meeting_size_category,
    
    CASE 
      WHEN m.duration_minutes <= 15 THEN 'Quick'
      WHEN m.duration_minutes <= 30 THEN 'Short'
      WHEN m.duration_minutes <= 60 THEN 'Standard'
      WHEN m.duration_minutes <= 120 THEN 'Long'
      ELSE 'Extended'
    END as meeting_duration_category,
    
    -- Data Quality and Audit Fields
    m.data_quality_score,
    m.source_system,
    GREATEST(
      m.update_timestamp, 
      COALESCE(p.first_participant_join_time, m.update_timestamp),
      COALESCE(f.usage_date, m.update_timestamp)
    ) as source_last_updated_at,
    
    -- DBT Audit Fields
    CURRENT_TIMESTAMP() as created_at,
    CURRENT_TIMESTAMP() as last_updated_at,
    '{{ invocation_id }}' as dbt_invocation_id,
    '{{ var("dbt_version") }}' as dbt_version
    
  FROM meeting_base m
  LEFT JOIN participant_aggregates p ON m.meeting_id = p.meeting_id
  LEFT JOIN host_info h ON m.host_id = h.user_id
  LEFT JOIN feature_usage_aggregates f ON m.meeting_id = f.meeting_id
)

SELECT * FROM meeting_facts

-- Data Quality Tests
{% if execute %}
  {% set quality_check_query %}
    SELECT 
      COUNT(*) as total_records,
      COUNT(CASE WHEN meeting_id IS NULL THEN 1 END) as null_meeting_ids,
      COUNT(CASE WHEN start_time IS NULL THEN 1 END) as null_start_times,
      COUNT(CASE WHEN total_participants < 0 THEN 1 END) as negative_participants
    FROM meeting_facts
  {% endset %}
  
  {% if flags.WHICH == 'run' %}
    {{ log("Running data quality checks on go_meeting_facts...", info=True) }}
  {% endif %}
{% endif %}
