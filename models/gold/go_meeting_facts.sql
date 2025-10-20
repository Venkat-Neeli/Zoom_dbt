{{
  config(
    materialized='incremental',
    unique_key='meeting_fact_key',
    on_schema_change='fail',
    tags=['gold', 'fact_table']
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
    {% if is_incremental() %}
      AND update_timestamp > (SELECT COALESCE(MAX(last_updated), '1900-01-01') FROM {{ this }})
    {% endif %}
),

participant_agg AS (
  SELECT 
    meeting_id,
    COUNT(DISTINCT participant_id) as total_participants,
    AVG(DATEDIFF('minute', join_time, leave_time)) as avg_participation_minutes
  FROM {{ ref('si_participants') }}
  WHERE record_status = 'ACTIVE'
  GROUP BY meeting_id
),

usage_agg AS (
  SELECT 
    meeting_id,
    COUNT(DISTINCT feature_name) as features_used,
    SUM(usage_count) as total_feature_usage
  FROM {{ ref('si_feature_usage') }}
  WHERE record_status = 'ACTIVE'
  GROUP BY meeting_id
)

SELECT 
  {{ dbt_utils.generate_surrogate_key(['m.meeting_id', 'm.host_id', 'm.start_time']) }} as meeting_fact_key,
  m.meeting_id,
  m.host_id,
  m.meeting_topic,
  m.start_time,
  m.end_time,
  m.duration_minutes,
  COALESCE(p.total_participants, 0) as total_participants,
  COALESCE(p.avg_participation_minutes, 0) as avg_participation_minutes,
  COALESCE(u.features_used, 0) as features_used,
  COALESCE(u.total_feature_usage, 0) as total_feature_usage,
  CASE 
    WHEN m.duration_minutes > 60 THEN 'Long'
    WHEN m.duration_minutes > 30 THEN 'Medium' 
    ELSE 'Short'
  END as meeting_duration_category,
  CASE 
    WHEN COALESCE(p.total_participants, 0) > 10 THEN 'Large'
    WHEN COALESCE(p.total_participants, 0) > 3 THEN 'Medium'
    ELSE 'Small'
  END as meeting_size_category,
  m.data_quality_score,
  m.source_system,
  CURRENT_TIMESTAMP() as created_at,
  m.update_timestamp as last_updated,
  DATE(m.start_time) as meeting_date
FROM meeting_base m
LEFT JOIN participant_agg p ON m.meeting_id = p.meeting_id
LEFT JOIN usage_agg u ON m.meeting_id = u.meeting_id
