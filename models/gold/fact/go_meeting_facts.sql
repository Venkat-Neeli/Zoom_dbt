{{ config(
    materialized='incremental',
    on_schema_change='sync_all_columns',
    unique_key='meeting_fact_id'
) }}

WITH meeting_base AS (
    SELECT 
        meeting_id,
        host_id,
        meeting_topic,
        start_time,
        end_time,
        duration_minutes,
        data_quality_score,
        load_date,
        update_date,
        source_system
    FROM {{ ref('si_meetings') }}
    WHERE record_status = 'ACTIVE'
    {% if is_incremental() %}
        AND update_date > (SELECT COALESCE(MAX(update_date), '1900-01-01') FROM {{ this }})
    {% endif %}
),

participant_metrics AS (
    SELECT 
        p.meeting_id,
        COUNT(DISTINCT p.participant_id) AS participant_count,
        SUM(DATEDIFF('minute', p.join_time, COALESCE(p.leave_time, p.join_time))) AS total_attendance_minutes
    FROM {{ ref('si_participants') }} p
    WHERE p.record_status = 'ACTIVE'
    GROUP BY p.meeting_id
),

feature_metrics AS (
    SELECT 
        f.meeting_id,
        MAX(CASE WHEN f.feature_name = 'Recording' THEN 1 ELSE 0 END) AS recording_enabled,
        SUM(CASE WHEN f.feature_name = 'Screen Sharing' THEN f.usage_count ELSE 0 END) AS screen_share_count,
        SUM(CASE WHEN f.feature_name = 'Chat' THEN f.usage_count ELSE 0 END) AS chat_message_count,
        SUM(CASE WHEN f.feature_name = 'Breakout Rooms' THEN f.usage_count ELSE 0 END) AS breakout_room_count
    FROM {{ ref('si_feature_usage') }} f
    WHERE f.record_status = 'ACTIVE'
    GROUP BY f.meeting_id
),

final_meeting_facts AS (
    SELECT 
        CONCAT('MF_', mb.meeting_id, '_', DATE_PART('epoch', CURRENT_TIMESTAMP())::STRING) AS meeting_fact_id,
        COALESCE(mb.meeting_id, 'UNKNOWN') AS meeting_id,
        CASE WHEN mb.host_id IS NOT NULL THEN mb.host_id ELSE 'UNKNOWN_HOST' END AS host_id,
        TRIM(COALESCE(mb.meeting_topic, 'No Topic Specified')) AS meeting_topic,
        mb.start_time,
        mb.end_time,
        CASE 
            WHEN mb.duration_minutes > 0 THEN mb.duration_minutes 
            WHEN mb.end_time IS NOT NULL THEN DATEDIFF('minute', mb.start_time, mb.end_time)
            ELSE 0
        END AS duration_minutes,
        COALESCE(pm.participant_count, 0) AS participant_count,
        COALESCE(pm.participant_count, 0) AS max_concurrent_participants,
        COALESCE(pm.total_attendance_minutes, 0) AS total_attendance_minutes,
        CASE 
            WHEN pm.participant_count > 0 THEN pm.total_attendance_minutes / pm.participant_count 
            ELSE 0 
        END AS average_attendance_duration,
        CASE 
            WHEN COALESCE(mb.duration_minutes, 0) < 15 THEN 'Quick Meeting'
            WHEN COALESCE(mb.duration_minutes, 0) < 60 THEN 'Standard Meeting'
            ELSE 'Extended Meeting'
        END AS meeting_type,
        CASE 
            WHEN mb.end_time IS NOT NULL THEN 'Completed'
            WHEN mb.start_time <= CURRENT_TIMESTAMP() THEN 'In Progress'
            ELSE 'Scheduled'
        END AS meeting_status,
        CASE WHEN fm.recording_enabled = 1 THEN TRUE ELSE FALSE END AS recording_enabled,
        COALESCE(fm.screen_share_count, 0) AS screen_share_count,
        COALESCE(fm.chat_message_count, 0) AS chat_message_count,
        COALESCE(fm.breakout_room_count, 0) AS breakout_room_count,
        ROUND(COALESCE(mb.data_quality_score, 0), 2) AS quality_score_avg,
        ROUND((COALESCE(fm.chat_message_count, 0) * 0.3 + COALESCE(fm.screen_share_count, 0) * 0.4 + COALESCE(pm.participant_count, 0) * 0.3) / 10, 2) AS engagement_score,
        mb.load_date,
        CURRENT_DATE() AS update_date,
        mb.source_system
    FROM meeting_base mb
    LEFT JOIN participant_metrics pm ON mb.meeting_id = pm.meeting_id
    LEFT JOIN feature_metrics fm ON mb.meeting_id = fm.meeting_id
)

SELECT * FROM final_meeting_facts
