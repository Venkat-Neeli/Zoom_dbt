{{ config(
    materialized='table'
) }}

SELECT 
    'MF_SAMPLE_001' AS meeting_fact_id,
    'SAMPLE_MEETING_001' AS meeting_id,
    'HOST_001' AS host_id,
    'Sample Meeting Topic' AS meeting_topic,
    CURRENT_TIMESTAMP() AS start_time,
    DATEADD('minute', 60, CURRENT_TIMESTAMP()) AS end_time,
    60 AS duration_minutes,
    5 AS participant_count,
    5 AS max_concurrent_participants,
    300 AS total_attendance_minutes,
    60.0 AS average_attendance_duration,
    'Standard Meeting' AS meeting_type,
    'Completed' AS meeting_status,
    TRUE AS recording_enabled,
    2 AS screen_share_count,
    15 AS chat_message_count,
    0 AS breakout_room_count,
    8.5 AS quality_score_avg,
    7.2 AS engagement_score,
    CURRENT_DATE() AS load_date,
    CURRENT_DATE() AS update_date,
    'ZOOM_API' AS source_system
WHERE 1=0
