{{ config(
    materialized='table'
) }}

WITH participant_base AS (
    SELECT 
        participant_id,
        meeting_id,
        user_id,
        join_time,
        leave_time,
        source_system,
        load_date,
        update_date,
        data_quality_score
    FROM {{ ref('si_participants') }}
    WHERE record_status = 'ACTIVE'
)

SELECT 
    CONCAT('PF_', participant_id, '_', meeting_id) AS participant_fact_id,
    meeting_id,
    participant_id,
    COALESCE(user_id, 'GUEST_USER') AS user_id,
    join_time,
    leave_time,
    DATEDIFF('minute', join_time, leave_time) AS attendance_duration,
    'Participant' AS participant_role,
    'Computer Audio' AS audio_connection_type,
    TRUE AS video_enabled,
    0 AS screen_share_duration,
    0 AS chat_messages_sent,
    0 AS interaction_count,
    ROUND(data_quality_score, 2) AS connection_quality_rating,
    'Desktop' AS device_type,
    'Unknown' AS geographic_location,
    load_date,
    CURRENT_DATE() AS update_date,
    source_system
FROM participant_base
