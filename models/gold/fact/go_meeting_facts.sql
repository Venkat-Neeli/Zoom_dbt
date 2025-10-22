{{ config(
    materialized='table',
    cluster_by=['start_time', 'host_id']
) }}

-- Meeting Facts Transformation
WITH meeting_base AS (
    SELECT 
        meeting_id,
        host_id,
        meeting_topic,
        start_time,
        end_time,
        duration_minutes,
        load_date,
        source_system,
        data_quality_score,
        ROW_NUMBER() OVER (PARTITION BY meeting_id ORDER BY update_timestamp DESC) as rn
    FROM {{ source('silver', 'si_meetings') }}
    WHERE record_status = 'ACTIVE'
),

participant_metrics AS (
    SELECT 
        meeting_id,
        COUNT(DISTINCT participant_id) AS participant_count,
        SUM(DATEDIFF('minute', join_time, COALESCE(leave_time, CURRENT_TIMESTAMP()))) AS total_attendance_minutes,
        AVG(data_quality_score) AS avg_connection_quality
    FROM {{ source('silver', 'si_participants') }}
    WHERE record_status = 'ACTIVE'
      AND join_time IS NOT NULL
    GROUP BY meeting_id
),

feature_metrics AS (
    SELECT 
        meeting_id,
        SUM(CASE WHEN feature_name = 'Recording' THEN 1 ELSE 0 END) > 0 AS recording_enabled,
        SUM(CASE WHEN feature_name = 'Screen Sharing' THEN usage_count ELSE 0 END) AS screen_share_count,
        SUM(CASE WHEN feature_name = 'Chat' THEN usage_count ELSE 0 END) AS chat_message_count,
        SUM(CASE WHEN feature_name = 'Breakout Rooms' THEN usage_count ELSE 0 END) AS breakout_room_count
    FROM {{ source('silver', 'si_feature_usage') }}
    WHERE record_status = 'ACTIVE'
    GROUP BY meeting_id
)

SELECT 
    CONCAT('MF_', mb.meeting_id, '_', CURRENT_TIMESTAMP()::STRING)::VARCHAR(50) AS meeting_fact_id,
    COALESCE(mb.meeting_id, 'UNKNOWN')::VARCHAR(50) AS meeting_id,
    CASE WHEN mb.host_id IS NOT NULL THEN mb.host_id ELSE 'UNKNOWN_HOST' END::VARCHAR(50) AS host_id,
    TRIM(COALESCE(mb.meeting_topic, 'No Topic Specified'))::VARCHAR(500) AS meeting_topic,
    CONVERT_TIMEZONE('UTC', mb.start_time) AS start_time,
    CONVERT_TIMEZONE('UTC', mb.end_time) AS end_time,
    CASE 
        WHEN mb.duration_minutes > 0 THEN mb.duration_minutes 
        ELSE DATEDIFF('minute', mb.start_time, mb.end_time) 
    END AS duration_minutes,
    COALESCE(pm.participant_count, 0) AS participant_count,
    COALESCE(pm.participant_count, 0) AS max_concurrent_participants,
    COALESCE(pm.total_attendance_minutes, 0) AS total_attendance_minutes,
    CASE 
        WHEN pm.participant_count > 0 THEN pm.total_attendance_minutes / pm.participant_count 
        ELSE 0 
    END AS average_attendance_duration,
    CASE 
        WHEN mb.duration_minutes < 15 THEN 'Quick Meeting'
        WHEN mb.duration_minutes < 60 THEN 'Standard Meeting'
        ELSE 'Extended Meeting'
    END::VARCHAR(50) AS meeting_type,
    CASE 
        WHEN mb.end_time IS NOT NULL THEN 'Completed'
        WHEN mb.start_time <= CURRENT_TIMESTAMP() THEN 'In Progress'
        ELSE 'Scheduled'
    END::VARCHAR(50) AS meeting_status,
    COALESCE(fm.recording_enabled, FALSE) AS recording_enabled,
    COALESCE(fm.screen_share_count, 0) AS screen_share_count,
    COALESCE(fm.chat_message_count, 0) AS chat_message_count,
    COALESCE(fm.breakout_room_count, 0) AS breakout_room_count,
    ROUND(COALESCE(mb.data_quality_score, 0), 2) AS quality_score_avg,
    ROUND(
        (COALESCE(fm.chat_message_count, 0) * 0.3 + 
         COALESCE(fm.screen_share_count, 0) * 0.4 + 
         COALESCE(pm.participant_count, 0) * 0.3) / 10, 2
    ) AS engagement_score,
    mb.load_date,
    CURRENT_DATE() AS update_date,
    mb.source_system
FROM meeting_base mb
LEFT JOIN participant_metrics pm ON mb.meeting_id = pm.meeting_id
LEFT JOIN feature_metrics fm ON mb.meeting_id = fm.meeting_id
WHERE mb.rn = 1
