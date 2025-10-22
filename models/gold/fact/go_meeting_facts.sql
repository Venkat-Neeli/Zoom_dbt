{{ config(
    materialized='table'
) }}

SELECT 
    meeting_id AS meeting_fact_id,
    meeting_id,
    host_id,
    meeting_topic,
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
    data_quality_score AS quality_score_avg,
    0.0 AS engagement_score,
    load_date,
    update_date,
    source_system
FROM {{ ref('si_meetings') }}
