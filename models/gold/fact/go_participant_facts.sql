{{ config(
    materialized='incremental',
    unique_key='participant_fact_id',
    on_schema_change='sync_all_columns',
    pre_hook="INSERT INTO {{ ref('go_process_audit') }} (execution_id, pipeline_name, start_time, status, process_type, user_executed, source_system, target_system) SELECT UUID_STRING(), 'go_participant_facts_load', CURRENT_TIMESTAMP(), 'STARTED', 'FACT_LOAD', 'DBT_USER', 'SILVER', 'GOLD' WHERE '{{ this.name }}' != 'go_process_audit'",
    post_hook="UPDATE {{ ref('go_process_audit') }} SET end_time = CURRENT_TIMESTAMP(), status = 'COMPLETED', records_processed = (SELECT COUNT(*) FROM {{ this }}) WHERE pipeline_name = 'go_participant_facts_load' AND status = 'STARTED' AND '{{ this.name }}' != 'go_process_audit'"
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
    WHERE record_status = 'ACTIVE'
        AND data_quality_score >= 0.7
        {% if is_incremental() %}
        AND update_date >= (SELECT MAX(update_date) FROM {{ this }})
        {% endif %}
),

meeting_info AS (
    SELECT 
        meeting_id,
        host_id
    FROM {{ ref('si_meetings') }}
    WHERE record_status = 'ACTIVE'
),

feature_usage_by_participant AS (
    SELECT 
        meeting_id,
        SUM(CASE WHEN feature_name = 'Screen Sharing' THEN usage_count ELSE 0 END) AS screen_share_duration,
        SUM(CASE WHEN feature_name = 'Chat' THEN usage_count ELSE 0 END) AS chat_messages_sent,
        COUNT(*) AS interaction_count
    FROM {{ ref('si_feature_usage') }}
    WHERE record_status = 'ACTIVE'
    GROUP BY meeting_id
),

final_facts AS (
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
        TRUE AS video_enabled,
        COALESCE(fup.screen_share_duration, 0) AS screen_share_duration,
        COALESCE(fup.chat_messages_sent, 0) AS chat_messages_sent,
        COALESCE(fup.interaction_count, 0) AS interaction_count,
        ROUND(pb.data_quality_score, 2) AS connection_quality_rating,
        'Desktop' AS device_type,
        'Unknown' AS geographic_location,
        pb.load_date,
        CURRENT_DATE() AS update_date,
        pb.source_system
    FROM participant_base pb
    LEFT JOIN meeting_info mi ON pb.meeting_id = mi.meeting_id
    LEFT JOIN feature_usage_by_participant fup ON pb.meeting_id = fup.meeting_id
)

SELECT * FROM final_facts
