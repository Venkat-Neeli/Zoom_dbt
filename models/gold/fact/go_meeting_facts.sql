{{
  config(
    materialized='incremental',
    unique_key='meeting_fact_id',
    on_schema_change='fail',
    cluster_by=['load_date'],
    pre_hook="INSERT INTO {{ ref('si_process_audit') }} (execution_id, pipeline_name, status, start_time) VALUES (UUID_STRING(), 'go_meeting_facts', 'STARTED', CURRENT_TIMESTAMP())",
    post_hook="INSERT INTO {{ ref('si_process_audit') }} (execution_id, pipeline_name, status, end_time) VALUES (UUID_STRING(), 'go_meeting_facts', 'COMPLETED', CURRENT_TIMESTAMP())"
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
        AND data_quality_score >= {{ var('min_quality_score') }}
        AND duration_minutes BETWEEN {{ var('min_meeting_duration_minutes') }} AND {{ var('max_meeting_duration_minutes') }}
    {% if is_incremental() %}
        AND load_date >= (SELECT MAX(load_date) FROM {{ this }})
    {% endif %}
),

participant_aggregates AS (
    SELECT 
        p.meeting_id,
        COUNT(DISTINCT p.participant_id) AS participant_count,
        COUNT(DISTINCT CASE WHEN p.join_time IS NOT NULL THEN p.participant_id END) AS max_concurrent_participants,
        SUM(DATEDIFF('minute', p.join_time, p.leave_time)) AS total_attendance_minutes,
        AVG(DATEDIFF('minute', p.join_time, p.leave_time)) AS average_attendance_duration
    FROM {{ ref('si_participants') }} p
    WHERE p.record_status = 'ACTIVE'
        AND p.join_time IS NOT NULL
        AND p.leave_time IS NOT NULL
    GROUP BY p.meeting_id
),

feature_aggregates AS (
    SELECT 
        f.meeting_id,
        SUM(CASE WHEN f.feature_name = 'screen_share' THEN f.usage_count ELSE 0 END) AS screen_share_count,
        SUM(CASE WHEN f.feature_name = 'chat' THEN f.usage_count ELSE 0 END) AS chat_message_count,
        SUM(CASE WHEN f.feature_name = 'breakout_room' THEN f.usage_count ELSE 0 END) AS breakout_room_count
    FROM {{ ref('si_feature_usage') }} f
    WHERE f.record_status = 'ACTIVE'
    GROUP BY f.meeting_id
)

SELECT 
    UUID_STRING() AS meeting_fact_id,
    m.meeting_id,
    m.host_id,
    TRIM(m.meeting_topic) AS meeting_topic,
    CONVERT_TIMEZONE('{{ var("default_timezone") }}', m.start_time) AS start_time,
    CONVERT_TIMEZONE('{{ var("default_timezone") }}', m.end_time) AS end_time,
    m.duration_minutes,
    COALESCE(pa.participant_count, 0) AS participant_count,
    COALESCE(pa.max_concurrent_participants, 0) AS max_concurrent_participants,
    COALESCE(pa.total_attendance_minutes, 0) AS total_attendance_minutes,
    COALESCE(pa.average_attendance_duration, 0) AS average_attendance_duration,
    CASE 
        WHEN m.duration_minutes <= 30 THEN 'SHORT'
        WHEN m.duration_minutes <= 120 THEN 'MEDIUM'
        ELSE 'LONG'
    END AS meeting_type,
    CASE 
        WHEN m.end_time IS NOT NULL THEN 'COMPLETED'
        WHEN m.start_time <= CURRENT_TIMESTAMP() THEN 'IN_PROGRESS'
        ELSE 'SCHEDULED'
    END AS meeting_status,
    CASE WHEN fa.screen_share_count > 0 THEN TRUE ELSE FALSE END AS recording_enabled,
    COALESCE(fa.screen_share_count, 0) AS screen_share_count,
    COALESCE(fa.chat_message_count, 0) AS chat_message_count,
    COALESCE(fa.breakout_room_count, 0) AS breakout_room_count,
    CASE 
        WHEN pa.participant_count > 0 THEN (pa.total_attendance_minutes / (pa.participant_count * m.duration_minutes)) * 10
        ELSE 0
    END AS quality_score_avg,
    CASE 
        WHEN pa.participant_count = 0 THEN 0
        WHEN (fa.chat_message_count + fa.screen_share_count) = 0 THEN 1
        ELSE LEAST(10, ((fa.chat_message_count + fa.screen_share_count) / pa.participant_count) * 2)
    END AS engagement_score,
    m.load_date,
    CURRENT_TIMESTAMP() AS created_at,
    CURRENT_TIMESTAMP() AS updated_at
FROM meeting_base m
LEFT JOIN participant_aggregates pa ON m.meeting_id = pa.meeting_id
LEFT JOIN feature_aggregates fa ON m.meeting_id = fa.meeting_id
