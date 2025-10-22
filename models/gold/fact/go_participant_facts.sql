{{ config(
    materialized='table',
    cluster_by=['join_time', 'meeting_id'],
    pre_hook="INSERT INTO {{ this.schema }}.go_process_audit (execution_id, pipeline_name, process_type, start_time, status, source_system, target_system, user_executed, load_date) SELECT UUID_STRING(), 'Participant Facts Load', 'DBT_MODEL', CURRENT_TIMESTAMP(), 'STARTED', 'SILVER', 'GOLD', 'DBT_CLOUD', CURRENT_DATE() WHERE '{{ this.name }}' != 'go_process_audit'",
    post_hook="INSERT INTO {{ this.schema }}.go_process_audit (execution_id, pipeline_name, process_type, start_time, end_time, status, records_processed, source_system, target_system, user_executed, processing_duration_seconds, load_date) SELECT UUID_STRING(), 'Participant Facts Load', 'DBT_MODEL', CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP(), 'COMPLETED', (SELECT COUNT(*) FROM {{ this }}), 'SILVER', 'GOLD', 'DBT_CLOUD', 0, CURRENT_DATE() WHERE '{{ this.name }}' != 'go_process_audit'"
) }}

-- Gold Participant Facts
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
        m.host_id AS meeting_host_id
    FROM {{ source('silver', 'si_participants') }} p
    LEFT JOIN {{ source('silver', 'si_meetings') }} m ON p.meeting_id = m.meeting_id
    WHERE p.record_status = 'ACTIVE'
    AND p.participant_id IS NOT NULL
),

feature_usage AS (
    SELECT 
        meeting_id,
        SUM(CASE WHEN feature_name = 'Screen Sharing' THEN usage_count ELSE 0 END) AS screen_share_duration,
        SUM(CASE WHEN feature_name = 'Chat' THEN usage_count ELSE 0 END) AS chat_messages_sent,
        COUNT(*) AS interaction_count
    FROM {{ source('silver', 'si_feature_usage') }}
    WHERE record_status = 'ACTIVE'
    GROUP BY meeting_id
),

participant_facts AS (
    SELECT 
        CONCAT('PF_', p.participant_id, '_', p.meeting_id) AS participant_fact_id,
        COALESCE(p.meeting_id, 'UNKNOWN') AS meeting_id,
        p.participant_id,
        COALESCE(p.user_id, 'GUEST_USER') AS user_id,
        CONVERT_TIMEZONE('UTC', p.join_time) AS join_time,
        CONVERT_TIMEZONE('UTC', p.leave_time) AS leave_time,
        DATEDIFF('minute', p.join_time, COALESCE(p.leave_time, CURRENT_TIMESTAMP())) AS attendance_duration,
        CASE 
            WHEN p.user_id = p.meeting_host_id THEN 'Host' 
            ELSE 'Participant' 
        END AS participant_role,
        'Computer Audio' AS audio_connection_type,
        FALSE AS video_enabled,
        COALESCE(f.screen_share_duration, 0) AS screen_share_duration,
        COALESCE(f.chat_messages_sent, 0) AS chat_messages_sent,
        COALESCE(f.interaction_count, 0) AS interaction_count,
        ROUND(COALESCE(p.data_quality_score, 0), 2) AS connection_quality_rating,
        'Desktop' AS device_type,
        'Unknown' AS geographic_location,
        p.load_date,
        CURRENT_DATE() AS update_date,
        p.source_system
    FROM participant_base p
    LEFT JOIN feature_usage f ON p.meeting_id = f.meeting_id
)

SELECT * FROM participant_facts
