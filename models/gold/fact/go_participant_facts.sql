{{ config(
    materialized='table',
    cluster_by=['join_time', 'meeting_id'],
    pre_hook="INSERT INTO {{ ref('go_process_audit') }} (execution_id, pipeline_name, start_time, status, source_system, target_system, process_type) VALUES (UUID_STRING(), 'go_participant_facts', CURRENT_TIMESTAMP(), 'STARTED', 'SILVER', 'GOLD', 'FACT_LOAD')",
    post_hook="INSERT INTO {{ ref('go_process_audit') }} (execution_id, pipeline_name, end_time, status, records_processed, source_system, target_system, process_type) VALUES (UUID_STRING(), 'go_participant_facts', CURRENT_TIMESTAMP(), 'COMPLETED', (SELECT COUNT(*) FROM {{ this }}), 'SILVER', 'GOLD', 'FACT_LOAD')"
) }}

WITH participant_base AS (
    SELECT 
        p.participant_id,
        p.meeting_id,
        p.user_id,
        p.join_time,
        p.leave_time,
        p.load_date,
        p.update_date,
        p.source_system,
        p.data_quality_score,
        p.record_status
    FROM {{ ref('si_participants') }} p
    WHERE p.record_status = 'ACTIVE' 
      AND p.data_quality_score >= 0.7
),

meeting_context AS (
    SELECT 
        m.meeting_id,
        m.host_id
    FROM {{ ref('si_meetings') }} m
    WHERE m.record_status = 'ACTIVE'
),

feature_interactions AS (
    SELECT 
        fu.meeting_id,
        p.participant_id,
        SUM(CASE WHEN fu.feature_name = 'Screen Sharing' THEN fu.usage_count ELSE 0 END) AS screen_share_duration,
        SUM(CASE WHEN fu.feature_name = 'Chat' THEN fu.usage_count ELSE 0 END) AS chat_messages_sent,
        COUNT(*) AS interaction_count,
        MAX(CASE WHEN fu.feature_name = 'Video' THEN TRUE ELSE FALSE END) AS video_enabled,
        MAX(CASE WHEN fu.feature_name LIKE '%Audio%' THEN 'Computer Audio' ELSE 'Phone' END) AS audio_connection_type
    FROM {{ ref('si_feature_usage') }} fu
    INNER JOIN participant_base p ON fu.meeting_id = p.meeting_id
    WHERE fu.record_status = 'ACTIVE'
    GROUP BY fu.meeting_id, p.participant_id
),

final_participant_facts AS (
    SELECT 
        CONCAT('PF_', pb.participant_id, '_', pb.meeting_id) AS participant_fact_id,
        COALESCE(pb.meeting_id, 'UNKNOWN') AS meeting_id,
        pb.participant_id,
        COALESCE(pb.user_id, 'GUEST_USER') AS user_id,
        CONVERT_TIMEZONE('UTC', pb.join_time) AS join_time,
        CONVERT_TIMEZONE('UTC', pb.leave_time) AS leave_time,
        DATEDIFF('minute', pb.join_time, pb.leave_time) AS attendance_duration,
        CASE 
            WHEN pb.user_id = mc.host_id THEN 'Host' 
            ELSE 'Participant' 
        END AS participant_role,
        COALESCE(fi.audio_connection_type, 'Computer Audio') AS audio_connection_type,
        COALESCE(fi.video_enabled, FALSE) AS video_enabled,
        COALESCE(fi.screen_share_duration, 0) AS screen_share_duration,
        COALESCE(fi.chat_messages_sent, 0) AS chat_messages_sent,
        COALESCE(fi.interaction_count, 0) AS interaction_count,
        ROUND(pb.data_quality_score, 2) AS connection_quality_rating,
        'Desktop' AS device_type,
        'Unknown' AS geographic_location,
        pb.load_date,
        CURRENT_DATE() AS update_date,
        pb.source_system,
        CURRENT_TIMESTAMP() AS created_at,
        CURRENT_TIMESTAMP() AS updated_at,
        'ACTIVE' AS process_status
    FROM participant_base pb
    LEFT JOIN meeting_context mc ON pb.meeting_id = mc.meeting_id
    LEFT JOIN feature_interactions fi ON pb.meeting_id = fi.meeting_id AND pb.participant_id = fi.participant_id
)

SELECT * FROM final_participant_facts
