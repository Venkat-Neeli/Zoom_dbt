{{ config(
    materialized='table',
    cluster_by=['start_time', 'host_id'],
    pre_hook="INSERT INTO {{ this.schema }}.go_process_audit (execution_id, pipeline_name, process_type, start_time, status, source_system, target_system, user_executed, load_date) SELECT UUID_STRING(), 'Meeting Facts Load', 'DBT_MODEL', CURRENT_TIMESTAMP(), 'STARTED', 'SILVER', 'GOLD', 'DBT_CLOUD', CURRENT_DATE() WHERE '{{ this.name }}' != 'go_process_audit'",
    post_hook="INSERT INTO {{ this.schema }}.go_process_audit (execution_id, pipeline_name, process_type, start_time, end_time, status, records_processed, source_system, target_system, user_executed, processing_duration_seconds, load_date) SELECT UUID_STRING(), 'Meeting Facts Load', 'DBT_MODEL', CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP(), 'COMPLETED', (SELECT COUNT(*) FROM {{ this }}), 'SILVER', 'GOLD', 'DBT_CLOUD', 0, CURRENT_DATE() WHERE '{{ this.name }}' != 'go_process_audit'"
) }}

-- Gold Meeting Facts
WITH meeting_base AS (
    SELECT 
        meeting_id,
        host_id,
        meeting_topic,
        start_time,
        end_time,
        duration_minutes,
        data_quality_score,
        load_date,
        update_date,
        source_system
    FROM {{ source('silver', 'si_meetings') }}
    WHERE record_status = 'ACTIVE'
    AND meeting_id IS NOT NULL
),

participant_metrics AS (
    SELECT 
        meeting_id,
        COUNT(DISTINCT participant_id) AS participant_count,
        SUM(DATEDIFF('minute', join_time, COALESCE(leave_time, CURRENT_TIMESTAMP()))) AS total_attendance_minutes
    FROM {{ source('silver', 'si_participants') }}
    WHERE record_status = 'ACTIVE'
    AND meeting_id IS NOT NULL
    GROUP BY meeting_id
),

feature_metrics AS (
    SELECT 
        meeting_id,
        SUM(CASE WHEN feature_name = 'Screen Sharing' THEN usage_count ELSE 0 END) AS screen_share_count,
        SUM(CASE WHEN feature_name = 'Chat' THEN usage_count ELSE 0 END) AS chat_message_count,
        SUM(CASE WHEN feature_name = 'Breakout Rooms' THEN usage_count ELSE 0 END) AS breakout_room_count,
        MAX(CASE WHEN feature_name = 'Recording' THEN 1 ELSE 0 END) AS recording_enabled
    FROM {{ source('silver', 'si_feature_usage') }}
    WHERE record_status = 'ACTIVE'
    AND meeting_id IS NOT NULL
    GROUP BY meeting_id
),

meeting_facts AS (
    SELECT 
        CONCAT('MF_', m.meeting_id, '_', CURRENT_TIMESTAMP()::STRING) AS meeting_fact_id,
        COALESCE(m.meeting_id, 'UNKNOWN') AS meeting_id,
        CASE WHEN m.host_id IS NOT NULL THEN m.host_id ELSE 'UNKNOWN_HOST' END AS host_id,
        TRIM(COALESCE(m.meeting_topic, 'No Topic Specified')) AS meeting_topic,
        CONVERT_TIMEZONE('UTC', m.start_time) AS start_time,
        CONVERT_TIMEZONE('UTC', m.end_time) AS end_time,
        CASE 
            WHEN m.duration_minutes > 0 THEN m.duration_minutes 
            ELSE DATEDIFF('minute', m.start_time, m.end_time) 
        END AS duration_minutes,
        COALESCE(p.participant_count, 0) AS participant_count,
        COALESCE(p.participant_count, 0) AS max_concurrent_participants,
        COALESCE(p.total_attendance_minutes, 0) AS total_attendance_minutes,
        CASE 
            WHEN p.participant_count > 0 THEN p.total_attendance_minutes / p.participant_count 
            ELSE 0 
        END AS average_attendance_duration,
        CASE 
            WHEN m.duration_minutes < 15 THEN 'Quick Meeting'
            WHEN m.duration_minutes < 60 THEN 'Standard Meeting'
            ELSE 'Extended Meeting'
        END AS meeting_type,
        CASE 
            WHEN m.end_time IS NOT NULL THEN 'Completed'
            WHEN m.start_time <= CURRENT_TIMESTAMP() THEN 'In Progress'
            ELSE 'Scheduled'
        END AS meeting_status,
        CASE WHEN f.recording_enabled = 1 THEN TRUE ELSE FALSE END AS recording_enabled,
        COALESCE(f.screen_share_count, 0) AS screen_share_count,
        COALESCE(f.chat_message_count, 0) AS chat_message_count,
        COALESCE(f.breakout_room_count, 0) AS breakout_room_count,
        ROUND(COALESCE(m.data_quality_score, 0), 2) AS quality_score_avg,
        ROUND((COALESCE(f.chat_message_count, 0) * 0.3 + COALESCE(f.screen_share_count, 0) * 0.4 + COALESCE(p.participant_count, 0) * 0.3) / 10, 2) AS engagement_score,
        m.load_date,
        CURRENT_DATE() AS update_date,
        m.source_system
    FROM meeting_base m
    LEFT JOIN participant_metrics p ON m.meeting_id = p.meeting_id
    LEFT JOIN feature_metrics f ON m.meeting_id = f.meeting_id
)

SELECT * FROM meeting_facts
