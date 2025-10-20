{{
  config(
    materialized='incremental',
    unique_key='participant_fact_id',
    on_schema_change='sync_all_columns',
    pre_hook="INSERT INTO {{ target.schema }}.audit_log (table_name, operation, start_time, user_name) VALUES ('go_participant_facts', 'transform_start', CURRENT_TIMESTAMP(), CURRENT_USER())",
    post_hook="INSERT INTO {{ target.schema }}.audit_log (table_name, operation, end_time, user_name, records_processed) VALUES ('go_participant_facts', 'transform_end', CURRENT_TIMESTAMP(), CURRENT_USER(), (SELECT COUNT(*) FROM {{ this }}))"
  )
}}

WITH participant_base AS (
  SELECT 
    participant_id,
    meeting_id,
    user_id,
    CONVERT_TIMEZONE('UTC', join_time) AS join_time,
    CONVERT_TIMEZONE('UTC', leave_time) AS leave_time,
    load_timestamp,
    update_timestamp,
    source_system,
    load_date,
    update_date,
    data_quality_score
  FROM {{ source('silver', 'si_participants') }}
  WHERE record_status = 'ACTIVE'
    AND data_quality_score >= {{ var('min_quality_score') }}
    AND participant_id IS NOT NULL
    {% if is_incremental() %}
      AND update_timestamp > (SELECT MAX(update_date) FROM {{ this }})
    {% endif %}
),

user_info AS (
  SELECT 
    user_id,
    user_name,
    plan_type
  FROM {{ source('silver', 'si_users') }}
  WHERE record_status = 'ACTIVE'
    AND data_quality_score >= {{ var('min_quality_score') }}
),

feature_usage AS (
  SELECT 
    meeting_id,
    COUNT(*) AS interaction_count
  FROM {{ source('silver', 'si_feature_usage') }}
  WHERE record_status = 'ACTIVE'
    AND data_quality_score >= {{ var('min_quality_score') }}
  GROUP BY meeting_id
)

SELECT 
  {{ dbt_utils.generate_surrogate_key(['pb.participant_id', 'pb.meeting_id']) }} AS participant_fact_id,
  pb.meeting_id,
  pb.participant_id,
  pb.user_id,
  pb.join_time,
  pb.leave_time,
  COALESCE(DATEDIFF('minute', pb.join_time, pb.leave_time), 0) AS attendance_duration,
  CASE 
    WHEN ui.plan_type = 'Pro' THEN 'Host'
    ELSE 'Participant'
  END AS participant_role,
  'VoIP' AS audio_connection_type,
  TRUE AS video_enabled,
  0 AS screen_share_duration,
  0 AS chat_messages_sent,
  COALESCE(fu.interaction_count, 0) AS interaction_count,
  CASE 
    WHEN pb.data_quality_score >= 0.9 THEN 'Excellent'
    WHEN pb.data_quality_score >= 0.8 THEN 'Good'
    WHEN pb.data_quality_score >= 0.7 THEN 'Fair'
    ELSE 'Poor'
  END AS connection_quality_rating,
  'Desktop' AS device_type,
  'Unknown' AS geographic_location,
  pb.load_date,
  pb.update_date,
  pb.source_system,
  {{ dbt_utils.generate_surrogate_key(['pb.participant_id', 'pb.meeting_id']) }} AS surrogate_key
FROM participant_base pb
LEFT JOIN user_info ui ON pb.user_id = ui.user_id
LEFT JOIN feature_usage fu ON pb.meeting_id = fu.meeting_id
