{{ config(
    materialized='table'
) }}

SELECT 
    'PF_SAMPLE_001' AS participant_fact_id,
    'SAMPLE_MEETING_001' AS meeting_id,
    'PARTICIPANT_001' AS participant_id,
    'USER_001' AS user_id,
    CURRENT_TIMESTAMP() AS join_time,
    DATEADD('minute', 45, CURRENT_TIMESTAMP()) AS leave_time,
    45 AS attendance_duration,
    'Participant' AS participant_role,
    'Computer Audio' AS audio_connection_type,
    TRUE AS video_enabled,
    5 AS screen_share_duration,
    3 AS chat_messages_sent,
    8 AS interaction_count,
    8.2 AS connection_quality_rating,
    'Desktop' AS device_type,
    'US-East' AS geographic_location,
    CURRENT_DATE() AS load_date,
    CURRENT_DATE() AS update_date,
    'ZOOM_API' AS source_system
WHERE 1=0
