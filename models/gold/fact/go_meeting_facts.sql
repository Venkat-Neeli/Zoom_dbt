{{ config(
    materialized='table'
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
        source_system,
        ROW_NUMBER() OVER (PARTITION BY meeting_id ORDER BY update_timestamp DESC) as rn
    FROM {{ source('silver', 'si_meetings') }}
    WHERE record_status = 'ACTIVE'
),

participant_metrics AS (
    SELECT 
        meeting_id,
        COUNT(DISTINCT participant_id) as participant_count,
        SUM(DATEDIFF('minute', join_time, leave_time)) as total_attendance_minutes
    FROM {{ source('silver', 'si_participants') }}
    WHERE record_status = 'ACTIVE'
    AND join_time IS NOT NULL 
    AND leave_time IS NOT NULL
    GROUP BY meeting_id
),

feature_usage_metrics AS (
    SELECT 
        meeting_id,
        SUM(CASE WHEN feature_name = 'Screen Sharing' THEN usage_count ELSE 0 END) as screen_share_count,
        SUM(CASE WHEN feature_name = 'Chat' THEN usage_count ELSE 0 END) as chat_message_count,
        SUM(CASE WHEN feature_name = 'Breakout Rooms' THEN usage_count ELSE 0 END) as breakout_room_count,
        MAX(CASE WHEN feature_name = 'Recording' THEN 1 ELSE 0 END) as recording_enabled
    FROM {{ source('silver', 'si_feature_usage') }}
    WHERE record_status = 'ACTIVE'
    GROUP BY meeting_id
),

final_facts AS (
    SELECT 
        CONCAT('MF_', mb.meeting_id, '_', CURRENT_TIMESTAMP()::STRING) as meeting_fact_id,
        COALESCE(mb.meeting_id, 'UNKNOWN') as meeting_id,
        CASE WHEN mb.host_id IS NOT NULL THEN mb.host_id ELSE 'UNKNOWN_HOST' END as host_id,
        TRIM(COALESCE(mb.meeting_topic, 'No Topic Specified')) as meeting_topic,
        CONVERT_TIMEZONE('UTC', mb.start_time) as start_time,
        CONVERT_TIMEZONE('UTC', mb.end_time) as end_time,
        CASE 
            WHEN mb.duration_minutes > 0 THEN mb.duration_minutes 
            ELSE DATEDIFF('minute', mb.start_time, mb.end_time) 
        END as duration_minutes,
        COALESCE(pm.participant_count, 0) as participant_count,
        COALESCE(pm.participant_count, 0) as max_concurrent_participants,
        COALESCE(pm.total_attendance_minutes, 0) as total_attendance_minutes,
        CASE 
            WHEN pm.participant_count > 0 THEN pm.total_attendance_minutes / pm.participant_count 
            ELSE 0 
        END as average_attendance_duration,
        CASE 
            WHEN mb.duration_minutes < 15 THEN 'Quick Meeting'
            WHEN mb.duration_minutes < 60 THEN 'Standard Meeting'
            ELSE 'Extended Meeting'
        END as meeting_type,
        CASE 
            WHEN mb.end_time IS NOT NULL THEN 'Completed'
            WHEN mb.start_time <= CURRENT_TIMESTAMP() THEN 'In Progress'
            ELSE 'Scheduled'
        END as meeting_status,
        CASE WHEN fum.recording_enabled = 1 THEN TRUE ELSE FALSE END as recording_enabled,
        COALESCE(fum.screen_share_count, 0) as screen_share_count,
        COALESCE(fum.chat_message_count, 0) as chat_message_count,
        COALESCE(fum.breakout_room_count, 0) as breakout_room_count,
        COALESCE(mb.data_quality_score, 0.0) as quality_score_avg,
        ROUND(
            (COALESCE(fum.chat_message_count, 0) * 0.3 + 
             COALESCE(fum.screen_share_count, 0) * 0.4 + 
             COALESCE(pm.participant_count, 0) * 0.3) / 10, 2
        ) as engagement_score,
        mb.load_date,
        CURRENT_DATE() as update_date,
        mb.source_system
    FROM meeting_base mb
    LEFT JOIN participant_metrics pm ON mb.meeting_id = pm.meeting_id
    LEFT JOIN feature_usage_metrics fum ON mb.meeting_id = fum.meeting_id
    WHERE mb.rn = 1
)

SELECT * FROM final_facts
