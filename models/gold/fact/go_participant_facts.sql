{{ config(
    materialized='table',
    cluster_by=['join_time', 'meeting_id'],
    pre_hook="INSERT INTO {{ ref('go_process_audit') }} (execution_id, pipeline_name, start_time, status, source_system, target_system, process_type, user_executed) VALUES (UUID_STRING(), 'go_participant_facts', CURRENT_TIMESTAMP(), 'STARTED', 'SILVER', 'GOLD', 'FACT_LOAD', CURRENT_USER()) WHERE '{{ this.name }}' != 'go_process_audit'",
    post_hook="UPDATE {{ ref('go_process_audit') }} SET end_time = CURRENT_TIMESTAMP(), status = 'COMPLETED', records_processed = (SELECT COUNT(*) FROM {{ this }}) WHERE pipeline_name = 'go_participant_facts' AND status = 'STARTED' AND '{{ this.name }}' != 'go_process_audit'"
) }}

WITH participant_base AS (
    SELECT 
        p.participant_id,
        p.meeting_id,
        p.user_id,
        p.join_time,
        p.leave_time,
        p.data_quality_score,
        p.load_date,
        p.update_date,
        p.source_system
    FROM {{ ref('si_participants') }} p
    WHERE p.record_status = 'ACTIVE'
),

meeting_context AS (
    SELECT 
        m.meeting_id,
        m.host_id
    FROM {{ ref('si_meetings') }} m
    WHERE m.record_status = 'ACTIVE'
),

feature_usage AS (
    SELECT 
        f.meeting_id,
        SUM(CASE WHEN f.feature_name = 'Screen Sharing' THEN f.usage_count ELSE 0 END) AS screen_share_duration,
        SUM(CASE WHEN f.feature_name = 'Chat' THEN f.usage_count ELSE 0 END) AS chat_messages_sent,
        COUNT(*) AS interaction_count,
        MAX(CASE WHEN f.feature_name = 'Video' THEN TRUE ELSE FALSE END) AS video_enabled
    FROM {{ ref('si_feature_usage') }} f
    WHERE f.record_status = 'ACTIVE'
    GROUP BY f.meeting_id
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
        'Computer Audio' AS audio_connection_type,
        COALESCE(fu.video_enabled, FALSE) AS video_enabled,
        COALESCE(fu.screen_share_duration, 0) AS screen_share_duration,
        COALESCE(fu.chat_messages_sent, 0) AS chat_messages_sent,
        COALESCE(fu.interaction_count, 0) AS interaction_count,
        ROUND(pb.data_quality_score, 2) AS connection_quality_rating,
        'Desktop' AS device_type,
        'Unknown' AS geographic_location,
        pb.load_date,
        CURRENT_DATE() AS update_date,
        pb.source_system
    FROM participant_base pb
    LEFT JOIN meeting_context mc ON pb.meeting_id = mc.meeting_id
    LEFT JOIN feature_usage fu ON pb.meeting_id = fu.meeting_id
)

SELECT * FROM final_participant_facts
