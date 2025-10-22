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
        source_system,
        load_date,
        update_date,
        data_quality_score
    FROM {{ ref('si_meetings') }}
    WHERE record_status = 'ACTIVE'
),

participant_counts AS (
    SELECT 
        meeting_id,
        COUNT(DISTINCT participant_id) AS participant_count
    FROM {{ ref('si_participants') }}
    WHERE record_status = 'ACTIVE'
    GROUP BY meeting_id
)

SELECT 
    CONCAT('MF_', mb.meeting_id) AS meeting_fact_id,
    mb.meeting_id,
    mb.host_id,
    COALESCE(mb.meeting_topic, 'No Topic') AS meeting_topic,
    mb.start_time,
    mb.end_time,
    mb.duration_minutes,
    COALESCE(pc.participant_count, 0) AS participant_count,
    COALESCE(pc.participant_count, 0) AS max_concurrent_participants,
    0 AS total_attendance_minutes,
    0 AS average_attendance_duration,
    CASE 
        WHEN mb.duration_minutes < 15 THEN 'Quick Meeting'
        WHEN mb.duration_minutes < 60 THEN 'Standard Meeting'
        ELSE 'Extended Meeting'
    END AS meeting_type,
    'Completed' AS meeting_status,
    FALSE AS recording_enabled,
    0 AS screen_share_count,
    0 AS chat_message_count,
    0 AS breakout_room_count,
    ROUND(mb.data_quality_score, 2) AS quality_score_avg,
    0.0 AS engagement_score,
    mb.load_date,
    CURRENT_DATE() AS update_date,
    mb.source_system
FROM meeting_base mb
LEFT JOIN participant_counts pc ON mb.meeting_id = pc.meeting_id
