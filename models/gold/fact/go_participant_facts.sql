{{ config(
    materialized='table',
    cluster_by=['join_time', 'meeting_id']
) }}

-- Participant Facts Transformation
WITH participant_base AS (
    SELECT 
        p.participant_id,
        p.meeting_id,
        p.user_id,
        p.join_time,
        p.leave_time,
        p.data_quality_score,
        p.load_date,
        p.source_system,
        m.host_id AS meeting_host_id,
        ROW_NUMBER() OVER (PARTITION BY p.participant_id, p.meeting_id ORDER BY p.update_timestamp DESC) as rn
    FROM {{ source('silver', 'si_participants') }} p
    LEFT JOIN {{ source('silver', 'si_meetings') }} m ON p.meeting_id = m.meeting_id
    WHERE p.record_status = 'ACTIVE'
      AND p.join_time IS NOT NULL
),

feature_usage AS (
    SELECT 
        fu.meeting_id,
        p.participant_id,
        SUM(CASE WHEN fu.feature_name = 'Screen Sharing' THEN fu.usage_count ELSE 0 END) AS screen_share_duration,
        SUM(CASE WHEN fu.feature_name = 'Chat' THEN fu.usage_count ELSE 0 END) AS chat_messages_sent,
        COUNT(*) AS interaction_count
    FROM {{ source('silver', 'si_feature_usage') }} fu
    JOIN {{ source('silver', 'si_participants') }} p ON fu.meeting_id = p.meeting_id
    WHERE fu.record_status = 'ACTIVE'
    GROUP BY fu.meeting_id, p.participant_id
)

SELECT 
    CONCAT('PF_', pb.participant_id, '_', pb.meeting_id)::VARCHAR(50) AS participant_fact_id,
    COALESCE(pb.meeting_id, 'UNKNOWN')::VARCHAR(50) AS meeting_id,
    pb.participant_id::VARCHAR(50) AS participant_id,
    COALESCE(pb.user_id, 'GUEST_USER')::VARCHAR(50) AS user_id,
    CONVERT_TIMEZONE('UTC', pb.join_time) AS join_time,
    CONVERT_TIMEZONE('UTC', pb.leave_time) AS leave_time,
    DATEDIFF('minute', pb.join_time, COALESCE(pb.leave_time, CURRENT_TIMESTAMP())) AS attendance_duration,
    CASE 
        WHEN pb.user_id = pb.meeting_host_id THEN 'Host' 
        ELSE 'Participant' 
    END::VARCHAR(50) AS participant_role,
    'Computer Audio'::VARCHAR(50) AS audio_connection_type,
    TRUE AS video_enabled,
    COALESCE(fu.screen_share_duration, 0) AS screen_share_duration,
    COALESCE(fu.chat_messages_sent, 0) AS chat_messages_sent,
    COALESCE(fu.interaction_count, 0) AS interaction_count,
    ROUND(COALESCE(pb.data_quality_score, 0), 2) AS connection_quality_rating,
    'Desktop'::VARCHAR(100) AS device_type,
    'Unknown'::VARCHAR(100) AS geographic_location,
    pb.load_date,
    CURRENT_DATE() AS update_date,
    pb.source_system
FROM participant_base pb
LEFT JOIN feature_usage fu ON pb.meeting_id = fu.meeting_id AND pb.participant_id = fu.participant_id
WHERE pb.rn = 1
