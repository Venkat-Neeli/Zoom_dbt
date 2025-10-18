{{ config(
    materialized='table',
    cluster_by=['meeting_date', 'host_id'],
    pre_hook="INSERT INTO {{ ref('go_process_audit') }} (process_name, table_name, start_time, status) VALUES ('go_meeting_facts_transform', 'go_meeting_facts', CURRENT_TIMESTAMP(), 'STARTED') WHERE '{{ this.name }}' != 'go_process_audit'",
    post_hook="INSERT INTO {{ ref('go_process_audit') }} (process_name, table_name, end_time, status, records_processed) VALUES ('go_meeting_facts_transform', 'go_meeting_facts', CURRENT_TIMESTAMP(), 'COMPLETED', (SELECT COUNT(*) FROM {{ this }})) WHERE '{{ this.name }}' != 'go_process_audit'"
) }}

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
    WHERE meeting_id IS NOT NULL
        AND record_status = 'ACTIVE'
),

participant_metrics AS (
    SELECT 
        meeting_id,
        COUNT(DISTINCT participant_id) AS total_participants,
        COUNT(DISTINCT user_id) AS unique_users,
        AVG(DATEDIFF('minute', join_time, leave_time)) AS avg_participation_duration,
        SUM(DATEDIFF('minute', join_time, leave_time)) AS total_attendance_minutes
    FROM {{ ref('si_participants') }}
    WHERE meeting_id IS NOT NULL
        AND record_status = 'ACTIVE'
    GROUP BY meeting_id
),

feature_usage_metrics AS (
    SELECT 
        meeting_id,
        COUNT(CASE WHEN feature_name = 'Recording' THEN 1 END) > 0 AS recording_enabled,
        COUNT(CASE WHEN feature_name = 'Screen Sharing' THEN 1 END) AS screen_share_count,
        SUM(CASE WHEN feature_name = 'Chat' THEN usage_count ELSE 0 END) AS chat_message_count,
        SUM(CASE WHEN feature_name = 'Breakout Rooms' THEN usage_count ELSE 0 END) AS breakout_room_count
    FROM {{ ref('si_feature_usage') }}
    WHERE meeting_id IS NOT NULL
        AND record_status = 'ACTIVE'
    GROUP BY meeting_id
),

host_info AS (
    SELECT 
        user_id,
        user_name,
        company,
        plan_type
    FROM {{ ref('si_users') }}
    WHERE user_id IS NOT NULL
        AND record_status = 'ACTIVE'
)

SELECT 
    CONCAT('MF_', mb.meeting_id, '_', CURRENT_TIMESTAMP()::STRING) AS meeting_fact_id,
    mb.meeting_id,
    mb.host_id,
    TRIM(COALESCE(mb.meeting_topic, 'No Topic Specified')) AS meeting_topic,
    CONVERT_TIMEZONE('UTC', mb.start_time) AS start_time,
    CONVERT_TIMEZONE('UTC', mb.end_time) AS end_time,
    CASE 
        WHEN mb.duration_minutes > 0 THEN mb.duration_minutes 
        ELSE DATEDIFF('minute', mb.start_time, mb.end_time) 
    END AS duration_minutes,
    COALESCE(pm.total_participants, 0) AS participant_count,
    -- Calculate max concurrent participants (simplified estimation)
    COALESCE(pm.total_participants, 0) AS max_concurrent_participants,
    COALESCE(pm.total_attendance_minutes, 0) AS total_attendance_minutes,
    CASE 
        WHEN COALESCE(pm.total_participants, 0) > 0 
        THEN COALESCE(pm.total_attendance_minutes, 0) / pm.total_participants
        ELSE 0 
    END AS average_attendance_duration,
    CASE 
        WHEN mb.duration_minutes < 15 THEN 'Quick Meeting'
        WHEN mb.duration_minutes < 60 THEN 'Standard Meeting'
        ELSE 'Extended Meeting'
    END AS meeting_type,
    CASE 
        WHEN mb.end_time IS NOT NULL THEN 'Completed'
        WHEN mb.start_time <= CURRENT_TIMESTAMP() THEN 'In Progress'
        ELSE 'Scheduled'
    END AS meeting_status,
    COALESCE(fum.recording_enabled, FALSE) AS recording_enabled,
    COALESCE(fum.screen_share_count, 0) AS screen_share_count,
    COALESCE(fum.chat_message_count, 0) AS chat_message_count,
    COALESCE(fum.breakout_room_count, 0) AS breakout_room_count,
    ROUND(mb.data_quality_score, 2) AS quality_score_avg,
    ROUND(
        (COALESCE(fum.chat_message_count, 0) * 0.3 + 
         COALESCE(fum.screen_share_count, 0) * 0.4 + 
         COALESCE(pm.total_participants, 0) * 0.3) / 10, 2
    ) AS engagement_score,
    mb.load_date,
    CURRENT_DATE() AS update_date,
    mb.source_system
FROM meeting_base mb
LEFT JOIN participant_metrics pm ON mb.meeting_id = pm.meeting_id
LEFT JOIN feature_usage_metrics fum ON mb.meeting_id = fum.meeting_id
LEFT JOIN host_info hi ON mb.host_id = hi.user_id
