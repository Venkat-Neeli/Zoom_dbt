{{ config(
    materialized='table'
) }}

SELECT 
    CONCAT('MF_', meeting_id) AS meeting_fact_id,
    meeting_id,
    host_id,
    COALESCE(meeting_topic, 'No Topic') AS meeting_topic,
    start_time,
    end_time,
    duration_minutes,
    0 AS participant_count,
    0 AS max_concurrent_participants,
    0 AS total_attendance_minutes,
    0 AS average_attendance_duration,
    'Standard Meeting' AS meeting_type,
    'Completed' AS meeting_status,
    FALSE AS recording_enabled,
    0 AS screen_share_count,
    0 AS chat_message_count,
    0 AS breakout_room_count,
    ROUND(data_quality_score, 2) AS quality_score_avg,
    0.0 AS engagement_score,
    load_date,
    CURRENT_DATE() AS update_date,
    source_system
FROM {{ ref('si_meetings') }}
WHERE record_status = 'ACTIVE'
