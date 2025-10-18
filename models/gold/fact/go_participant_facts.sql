{{ config(
    materialized='table',
    cluster_by=['join_time', 'meeting_id'],
    pre_hook="INSERT INTO {{ ref('go_process_audit') }} (process_name, table_name, start_time, status) VALUES ('go_participant_facts_transform', 'go_participant_facts', CURRENT_TIMESTAMP(), 'STARTED') WHERE '{{ this.name }}' != 'go_process_audit'",
    post_hook="INSERT INTO {{ ref('go_process_audit') }} (process_name, table_name, end_time, status, records_processed) VALUES ('go_participant_facts_transform', 'go_participant_facts', CURRENT_TIMESTAMP(), 'COMPLETED', (SELECT COUNT(*) FROM {{ this }})) WHERE '{{ this.name }}' != 'go_process_audit'"
) }}

WITH participant_base AS (
    SELECT 
        participant_id,
        meeting_id,
        user_id,
        join_time,
        leave_time,
        load_timestamp,
        update_timestamp,
        source_system,
        load_date,
        update_date,
        data_quality_score,
        record_status
    FROM {{ ref('si_participants') }}
    WHERE participant_id IS NOT NULL
        AND record_status = 'ACTIVE'
),

meeting_context AS (
    SELECT 
        meeting_id,
        host_id,
        meeting_topic,
        start_time AS meeting_start_time,
        end_time AS meeting_end_time,
        duration_minutes AS meeting_duration_minutes
    FROM {{ ref('si_meetings') }}
    WHERE meeting_id IS NOT NULL
        AND record_status = 'ACTIVE'
),

user_context AS (
    SELECT 
        user_id,
        user_name,
        email,
        company,
        plan_type
    FROM {{ ref('si_users') }}
    WHERE user_id IS NOT NULL
        AND record_status = 'ACTIVE'
),

participant_feature_usage AS (
    SELECT 
        pb.participant_id,
        pb.meeting_id,
        SUM(CASE WHEN fu.feature_name = 'Screen Sharing' THEN fu.usage_count ELSE 0 END) AS screen_share_duration,
        SUM(CASE WHEN fu.feature_name = 'Chat' THEN fu.usage_count ELSE 0 END) AS chat_messages_sent,
        COUNT(DISTINCT fu.feature_name) AS interaction_count
    FROM participant_base pb
    LEFT JOIN {{ ref('si_feature_usage') }} fu ON pb.meeting_id = fu.meeting_id
    WHERE fu.record_status = 'ACTIVE' OR fu.record_status IS NULL
    GROUP BY pb.participant_id, pb.meeting_id
)

SELECT 
    CONCAT('PF_', pb.participant_id, '_', pb.meeting_id) AS participant_fact_id,
    pb.meeting_id,
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
    TRUE AS video_enabled,
    COALESCE(pfu.screen_share_duration, 0) AS screen_share_duration,
    COALESCE(pfu.chat_messages_sent, 0) AS chat_messages_sent,
    COALESCE(pfu.interaction_count, 0) AS interaction_count,
    ROUND(pb.data_quality_score, 2) AS connection_quality_rating,
    'Desktop' AS device_type,
    'Unknown' AS geographic_location,
    pb.load_date,
    CURRENT_DATE() AS update_date,
    pb.source_system
FROM participant_base pb
LEFT JOIN meeting_context mc ON pb.meeting_id = mc.meeting_id
LEFT JOIN user_context uc ON pb.user_id = uc.user_id
LEFT JOIN participant_feature_usage pfu ON pb.participant_id = pfu.participant_id AND pb.meeting_id = pfu.meeting_id
