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
        data_quality_score,
        load_date,
        source_system,
        ROW_NUMBER() OVER (PARTITION BY participant_id, meeting_id ORDER BY update_timestamp DESC) as rn
    FROM {{ source('silver', 'si_participants') }}
    WHERE record_status = 'ACTIVE'
    AND join_time IS NOT NULL
    AND leave_time IS NOT NULL
),

meeting_hosts AS (
    SELECT 
        meeting_id,
        host_id
    FROM {{ source('silver', 'si_meetings') }}
    WHERE record_status = 'ACTIVE'
),

user_feature_usage AS (
    SELECT 
        meeting_id,
        SUM(CASE WHEN feature_name = 'Screen Sharing' THEN usage_count ELSE 0 END) as screen_share_duration,
        SUM(CASE WHEN feature_name = 'Chat' THEN usage_count ELSE 0 END) as chat_messages_sent,
        COUNT(*) as interaction_count
    FROM {{ source('silver', 'si_feature_usage') }}
    WHERE record_status = 'ACTIVE'
    GROUP BY meeting_id
),

final_participant_facts AS (
    SELECT 
        CONCAT('PF_', pb.participant_id, '_', pb.meeting_id) as participant_fact_id,
        COALESCE(pb.meeting_id, 'UNKNOWN') as meeting_id,
        pb.participant_id,
        COALESCE(pb.user_id, 'GUEST_USER') as user_id,
        CONVERT_TIMEZONE('UTC', pb.join_time) as join_time,
        CONVERT_TIMEZONE('UTC', pb.leave_time) as leave_time,
        DATEDIFF('minute', pb.join_time, pb.leave_time) as attendance_duration,
        CASE 
            WHEN pb.user_id = mh.host_id THEN 'Host' 
            ELSE 'Participant' 
        END as participant_role,
        'Computer Audio' as audio_connection_type,
        FALSE as video_enabled,
        COALESCE(ufu.screen_share_duration, 0) as screen_share_duration,
        COALESCE(ufu.chat_messages_sent, 0) as chat_messages_sent,
        COALESCE(ufu.interaction_count, 0) as interaction_count,
        ROUND(COALESCE(pb.data_quality_score, 0.0), 2) as connection_quality_rating,
        'Desktop' as device_type,
        'Unknown' as geographic_location,
        pb.load_date,
        CURRENT_DATE() as update_date,
        pb.source_system
    FROM participant_base pb
    LEFT JOIN meeting_hosts mh ON pb.meeting_id = mh.meeting_id
    LEFT JOIN user_feature_usage ufu ON pb.meeting_id = ufu.meeting_id
    WHERE pb.rn = 1
)

SELECT * FROM final_participant_facts
