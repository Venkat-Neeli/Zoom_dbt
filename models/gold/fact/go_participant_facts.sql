{{ config(
    materialized='incremental',
    on_schema_change='sync_all_columns',
    unique_key='participant_fact_id',
    pre_hook="INSERT INTO {{ ref('go_process_audit') }} (execution_id, pipeline_name, start_time, status, source_system, target_system, process_type) SELECT UUID_STRING(), 'go_participant_facts', CURRENT_TIMESTAMP(), 'STARTED', 'SILVER', 'GOLD', 'FACT_LOAD' WHERE '{{ this.name }}' != 'go_process_audit'",
    post_hook="INSERT INTO {{ ref('go_process_audit') }} (execution_id, pipeline_name, end_time, status, records_processed, source_system, target_system, process_type) SELECT UUID_STRING(), 'go_participant_facts', CURRENT_TIMESTAMP(), 'COMPLETED', (SELECT COUNT(*) FROM {{ this }}), 'SILVER', 'GOLD', 'FACT_LOAD' WHERE '{{ this.name }}' != 'go_process_audit'"
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
        p.source_system,
        m.host_id AS meeting_host_id
    FROM {{ ref('si_participants') }} p
    LEFT JOIN {{ ref('si_meetings') }} m ON p.meeting_id = m.meeting_id
    WHERE p.record_status = 'ACTIVE'
    {% if is_incremental() %}
        AND p.update_date > (SELECT MAX(update_date) FROM {{ this }})
    {% endif %}
),

feature_usage_agg AS (
    SELECT 
        f.meeting_id,
        COALESCE(u.user_id, 'GUEST_USER') AS user_id,
        SUM(CASE WHEN f.feature_name = 'Screen Sharing' THEN f.usage_count ELSE 0 END) AS screen_share_duration,
        SUM(CASE WHEN f.feature_name = 'Chat' THEN f.usage_count ELSE 0 END) AS chat_messages_sent,
        COUNT(*) AS interaction_count,
        MAX(CASE WHEN f.feature_name = 'Video' THEN TRUE ELSE FALSE END) AS video_enabled
    FROM {{ ref('si_feature_usage') }} f
    LEFT JOIN {{ ref('si_users') }} u ON f.meeting_id IN (
        SELECT meeting_id FROM {{ ref('si_meetings') }} WHERE host_id = u.user_id
    )
    WHERE f.record_status = 'ACTIVE'
    GROUP BY f.meeting_id, u.user_id
),

final_transform AS (
    SELECT 
        CONCAT('PF_', pb.participant_id, '_', pb.meeting_id) AS participant_fact_id,
        COALESCE(pb.meeting_id, 'UNKNOWN') AS meeting_id,
        pb.participant_id,
        COALESCE(pb.user_id, 'GUEST_USER') AS user_id,
        CONVERT_TIMEZONE('UTC', pb.join_time) AS join_time,
        CONVERT_TIMEZONE('UTC', pb.leave_time) AS leave_time,
        DATEDIFF('minute', pb.join_time, pb.leave_time) AS attendance_duration,
        CASE 
            WHEN pb.user_id = pb.meeting_host_id THEN 'Host' 
            ELSE 'Participant' 
        END AS participant_role,
        'Computer Audio' AS audio_connection_type,
        COALESCE(fua.video_enabled, FALSE) AS video_enabled,
        COALESCE(fua.screen_share_duration, 0) AS screen_share_duration,
        COALESCE(fua.chat_messages_sent, 0) AS chat_messages_sent,
        COALESCE(fua.interaction_count, 0) AS interaction_count,
        ROUND(pb.data_quality_score, 2) AS connection_quality_rating,
        'Desktop' AS device_type,
        'Unknown' AS geographic_location,
        pb.load_date,
        CURRENT_DATE() AS update_date,
        pb.source_system
    FROM participant_base pb
    LEFT JOIN feature_usage_agg fua ON pb.meeting_id = fua.meeting_id AND pb.user_id = fua.user_id
)

SELECT * FROM final_transform
