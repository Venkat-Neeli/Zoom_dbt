{{ config(
    materialized='table',
    cluster_by=['join_time', 'meeting_id'],
    tags=['fact', 'gold'],
    pre_hook="INSERT INTO {{ ref('go_process_audit') }} (execution_id, pipeline_name, process_type, start_time, status, source_system, target_system, user_executed, server_name, load_date, update_date) SELECT '{{ dbt_utils.generate_surrogate_key(['invocation_id', 'this.name']) }}', 'GO_PARTICIPANT_FACTS', 'FACT_BUILD', CURRENT_TIMESTAMP(), 'STARTED', 'SILVER', 'GOLD', CURRENT_USER(), 'DBT_CLOUD', CURRENT_DATE(), CURRENT_DATE() WHERE '{{ this.name }}' != 'go_process_audit'",
    post_hook="UPDATE {{ ref('go_process_audit') }} SET end_time = CURRENT_TIMESTAMP(), status = 'COMPLETED', records_processed = (SELECT COUNT(*) FROM {{ this }}), records_successful = (SELECT COUNT(*) FROM {{ this }}), processing_duration_seconds = DATEDIFF('second', start_time, CURRENT_TIMESTAMP()) WHERE execution_id = '{{ dbt_utils.generate_surrogate_key(['invocation_id', 'this.name']) }}' AND '{{ this.name }}' != 'go_process_audit'"
) }}

-- Gold Participant Facts Table
-- Detailed participant engagement and interaction metrics

WITH participant_base AS (
    SELECT
        participant_id,
        meeting_id,
        user_id,
        join_time,
        leave_time,
        data_quality_score,
        source_system,
        load_date,
        update_date
    FROM {{ source('silver', 'si_participants') }}
    WHERE record_status = 'ACTIVE'
),

meeting_hosts AS (
    SELECT
        meeting_id,
        host_id
    FROM {{ source('silver', 'si_meetings') }}
    WHERE record_status = 'ACTIVE'
),

feature_usage_by_participant AS (
    SELECT
        fu.meeting_id,
        p.participant_id,
        SUM(CASE WHEN UPPER(fu.feature_name) LIKE '%SCREEN%' THEN fu.usage_count ELSE 0 END) AS screen_share_duration,
        SUM(CASE WHEN UPPER(fu.feature_name) LIKE '%CHAT%' THEN fu.usage_count ELSE 0 END) AS chat_messages_sent,
        COUNT(*) AS interaction_count,
        MAX(CASE WHEN UPPER(fu.feature_name) LIKE '%VIDEO%' THEN 1 ELSE 0 END) AS video_enabled
    FROM {{ source('silver', 'si_feature_usage') }} fu
    INNER JOIN participant_base p ON fu.meeting_id = p.meeting_id
    WHERE fu.record_status = 'ACTIVE'
    GROUP BY fu.meeting_id, p.participant_id
),

participant_facts AS (
    SELECT
        CONCAT('PF_', pb.participant_id, '_', pb.meeting_id) AS participant_fact_id,
        COALESCE(pb.meeting_id, 'UNKNOWN') AS meeting_id,
        pb.participant_id,
        COALESCE(pb.user_id, 'GUEST_USER') AS user_id,
        CONVERT_TIMEZONE('UTC', pb.join_time) AS join_time,
        CONVERT_TIMEZONE('UTC', pb.leave_time) AS leave_time,
        DATEDIFF('minute', pb.join_time, COALESCE(pb.leave_time, CURRENT_TIMESTAMP())) AS attendance_duration,
        CASE 
            WHEN pb.user_id = mh.host_id THEN 'Host' 
            ELSE 'Participant' 
        END AS participant_role,
        'Computer Audio' AS audio_connection_type,
        CASE WHEN fup.video_enabled = 1 THEN TRUE ELSE FALSE END AS video_enabled,
        COALESCE(fup.screen_share_duration, 0) AS screen_share_duration,
        COALESCE(fup.chat_messages_sent, 0) AS chat_messages_sent,
        COALESCE(fup.interaction_count, 0) AS interaction_count,
        ROUND(COALESCE(pb.data_quality_score, 0), 2) AS connection_quality_rating,
        'Desktop' AS device_type,
        'Unknown' AS geographic_location,
        pb.load_date,
        CURRENT_DATE() AS update_date,
        pb.source_system
    FROM participant_base pb
    LEFT JOIN meeting_hosts mh ON pb.meeting_id = mh.meeting_id
    LEFT JOIN feature_usage_by_participant fup ON pb.participant_id = fup.participant_id AND pb.meeting_id = fup.meeting_id
)

SELECT
    participant_fact_id::VARCHAR(50) AS participant_fact_id,
    meeting_id::VARCHAR(50) AS meeting_id,
    participant_id::VARCHAR(50) AS participant_id,
    user_id::VARCHAR(50) AS user_id,
    join_time,
    leave_time,
    attendance_duration,
    participant_role::VARCHAR(50) AS participant_role,
    audio_connection_type::VARCHAR(50) AS audio_connection_type,
    video_enabled,
    screen_share_duration,
    chat_messages_sent,
    interaction_count,
    connection_quality_rating,
    device_type::VARCHAR(100) AS device_type,
    geographic_location::VARCHAR(100) AS geographic_location,
    load_date,
    update_date,
    source_system::VARCHAR(100) AS source_system
FROM participant_facts
