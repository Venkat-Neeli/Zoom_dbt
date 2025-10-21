{{ config(
    materialized='incremental',
    unique_key='participant_fact_id',
    on_schema_change='sync_all_columns',
    pre_hook="INSERT INTO {{ ref('go_process_audit') }} (execution_id, pipeline_name, start_time, status, process_type, source_system, target_system, user_executed) VALUES (UUID_STRING(), 'go_participant_facts_load', CURRENT_TIMESTAMP(), 'STARTED', 'FACT_LOAD', 'SILVER', 'GOLD', CURRENT_USER()) WHERE '{{ this.name }}' != 'go_process_audit'",
    post_hook="INSERT INTO {{ ref('go_process_audit') }} (execution_id, pipeline_name, end_time, status, process_type, source_system, target_system, user_executed, records_processed) VALUES (UUID_STRING(), 'go_participant_facts_load', CURRENT_TIMESTAMP(), 'COMPLETED', 'FACT_LOAD', 'SILVER', 'GOLD', CURRENT_USER(), (SELECT COUNT(*) FROM {{ this }})) WHERE '{{ this.name }}' != 'go_process_audit'"
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
        update_date,
        source_system
    FROM {{ ref('si_participants') }}
    WHERE record_status = 'ACTIVE'
    {% if is_incremental() %}
        AND update_date > (SELECT MAX(update_date) FROM {{ this }})
    {% endif %}
),

meeting_info AS (
    SELECT 
        meeting_id,
        host_id
    FROM {{ ref('si_meetings') }}
    WHERE record_status = 'ACTIVE'
),

feature_usage AS (
    SELECT 
        meeting_id,
        SUM(CASE WHEN feature_name = 'Screen Sharing' THEN usage_count ELSE 0 END) AS screen_share_duration,
        SUM(CASE WHEN feature_name = 'Chat' THEN usage_count ELSE 0 END) AS chat_messages_sent,
        COUNT(*) AS interaction_count,
        MAX(CASE WHEN feature_name = 'Video' THEN 1 ELSE 0 END) AS video_enabled
    FROM {{ ref('si_feature_usage') }}
    WHERE record_status = 'ACTIVE'
    GROUP BY meeting_id
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
            WHEN pb.user_id = mi.host_id THEN 'Host' 
            ELSE 'Participant' 
        END AS participant_role,
        'Computer Audio' AS audio_connection_type,
        CASE WHEN fu.video_enabled = 1 THEN TRUE ELSE FALSE END AS video_enabled,
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
    LEFT JOIN meeting_info mi ON pb.meeting_id = mi.meeting_id
    LEFT JOIN feature_usage fu ON pb.meeting_id = fu.meeting_id
)

SELECT * FROM final_transform
