{{ config(
    materialized='incremental',
    on_schema_change='sync_all_columns',
    unique_key='participant_fact_id',
    pre_hook="INSERT INTO {{ ref('go_process_audit') }} (execution_id, pipeline_name, start_time, status, process_type, user_executed, load_date) SELECT UUID_STRING(), 'go_participant_facts', CURRENT_TIMESTAMP(), 'STARTED', 'FACT_LOAD', 'DBT_USER', CURRENT_DATE() WHERE '{{ this.name }}' != 'go_process_audit'",
    post_hook="INSERT INTO {{ ref('go_process_audit') }} (execution_id, pipeline_name, end_time, status, records_processed, process_type, user_executed, load_date) SELECT UUID_STRING(), 'go_participant_facts', CURRENT_TIMESTAMP(), 'COMPLETED', (SELECT COUNT(*) FROM {{ this }}), 'FACT_LOAD', 'DBT_USER', CURRENT_DATE() WHERE '{{ this.name }}' != 'go_process_audit'"
) }}

WITH participant_base AS (
    SELECT 
        p.participant_id,
        p.meeting_id,
        p.user_id,
        p.join_time,
        p.leave_time,
        p.load_timestamp,
        p.update_timestamp,
        p.source_system,
        p.load_date,
        p.update_date,
        p.data_quality_score,
        p.record_status
    FROM {{ ref('si_participants') }} p
    WHERE p.record_status = 'ACTIVE'
        AND p.data_quality_score >= 0.7
        {% if is_incremental() %}
        AND p.update_date > (SELECT MAX(update_date) FROM {{ this }})
        {% endif %}
),

meeting_info AS (
    SELECT 
        meeting_id,
        host_id
    FROM {{ ref('si_meetings') }}
    WHERE record_status = 'ACTIVE'
),

feature_participant_usage AS (
    SELECT 
        fu.meeting_id,
        p.user_id,
        SUM(CASE WHEN fu.feature_name = 'Screen Sharing' THEN fu.usage_count ELSE 0 END) AS screen_share_duration,
        SUM(CASE WHEN fu.feature_name = 'Chat' THEN fu.usage_count ELSE 0 END) AS chat_messages_sent,
        COUNT(*) AS interaction_count,
        MAX(CASE WHEN fu.feature_name = 'Video' THEN TRUE ELSE FALSE END) AS video_enabled
    FROM {{ ref('si_feature_usage') }} fu
    INNER JOIN participant_base p ON fu.meeting_id = p.meeting_id
    WHERE fu.record_status = 'ACTIVE'
    GROUP BY fu.meeting_id, p.user_id
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
            WHEN pb.user_id = mi.host_id THEN 'Host' 
            ELSE 'Participant' 
        END AS participant_role,
        'Computer Audio' AS audio_connection_type,
        COALESCE(fpu.video_enabled, FALSE) AS video_enabled,
        COALESCE(fpu.screen_share_duration, 0) AS screen_share_duration,
        COALESCE(fpu.chat_messages_sent, 0) AS chat_messages_sent,
        COALESCE(fpu.interaction_count, 0) AS interaction_count,
        ROUND(pb.data_quality_score, 2) AS connection_quality_rating,
        'Desktop' AS device_type,
        'Unknown' AS geographic_location,
        pb.load_date,
        CURRENT_DATE() AS update_date,
        pb.source_system
    FROM participant_base pb
    LEFT JOIN meeting_info mi ON pb.meeting_id = mi.meeting_id
    LEFT JOIN feature_participant_usage fpu ON pb.meeting_id = fpu.meeting_id AND pb.user_id = fpu.user_id
)

SELECT * FROM final_participant_facts
