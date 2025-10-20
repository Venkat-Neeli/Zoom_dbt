{{
  config(
    materialized='incremental',
    unique_key='participant_fact_id',
    on_schema_change='fail',
    cluster_by=['load_date'],
    pre_hook="INSERT INTO {{ ref('si_process_audit') }} (execution_id, pipeline_name, status, start_time) VALUES (UUID_STRING(), 'go_participant_facts', 'STARTED', CURRENT_TIMESTAMP())",
    post_hook="INSERT INTO {{ ref('si_process_audit') }} (execution_id, pipeline_name, status, end_time) VALUES (UUID_STRING(), 'go_participant_facts', 'COMPLETED', CURRENT_TIMESTAMP())"
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
        p.load_date,
        p.update_date,
        p.data_quality_score,
        p.record_status
    FROM {{ ref('si_participants') }} p
    WHERE p.record_status = 'ACTIVE'
        AND p.data_quality_score >= {{ var('min_quality_score') }}
        AND p.join_time IS NOT NULL
    {% if is_incremental() %}
        AND p.load_date >= (SELECT MAX(load_date) FROM {{ this }})
    {% endif %}
),

feature_usage_agg AS (
    SELECT 
        f.meeting_id,
        SUM(CASE WHEN f.feature_name = 'screen_share' THEN f.usage_count ELSE 0 END) AS screen_share_duration,
        SUM(CASE WHEN f.feature_name = 'chat' THEN f.usage_count ELSE 0 END) AS chat_messages_sent,
        COUNT(DISTINCT f.feature_name) AS interaction_count
    FROM {{ ref('si_feature_usage') }} f
    WHERE f.record_status = 'ACTIVE'
    GROUP BY f.meeting_id
)

SELECT 
    UUID_STRING() AS participant_fact_id,
    p.meeting_id,
    p.participant_id,
    p.user_id,
    CONVERT_TIMEZONE('{{ var("default_timezone") }}', p.join_time) AS join_time,
    CONVERT_TIMEZONE('{{ var("default_timezone") }}', p.leave_time) AS leave_time,
    COALESCE(DATEDIFF('minute', p.join_time, p.leave_time), 0) AS attendance_duration,
    CASE 
        WHEN p.user_id IS NOT NULL THEN 'HOST'
        ELSE 'PARTICIPANT'
    END AS participant_role,
    'VOIP' AS audio_connection_type,
    TRUE AS video_enabled,
    COALESCE(f.screen_share_duration, 0) AS screen_share_duration,
    COALESCE(f.chat_messages_sent, 0) AS chat_messages_sent,
    COALESCE(f.interaction_count, 0) AS interaction_count,
    CASE 
        WHEN DATEDIFF('minute', p.join_time, p.leave_time) >= 30 THEN 5
        WHEN DATEDIFF('minute', p.join_time, p.leave_time) >= 15 THEN 4
        WHEN DATEDIFF('minute', p.join_time, p.leave_time) >= 5 THEN 3
        ELSE 2
    END AS connection_quality_rating,
    'DESKTOP' AS device_type,
    'US' AS geographic_location,
    p.load_date,
    CURRENT_TIMESTAMP() AS created_at,
    CURRENT_TIMESTAMP() AS updated_at
FROM participant_base p
LEFT JOIN feature_usage_agg f ON p.meeting_id = f.meeting_id
