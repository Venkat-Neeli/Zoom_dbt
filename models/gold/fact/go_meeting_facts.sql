{{ config(
    materialized='table',
    cluster_by=['start_time', 'host_id'],
    tags=['fact', 'gold'],
    pre_hook="INSERT INTO {{ ref('go_process_audit') }} (execution_id, pipeline_name, process_type, start_time, status, source_system, target_system, user_executed, server_name, load_date, update_date) SELECT '{{ dbt_utils.generate_surrogate_key(['invocation_id', 'this.name']) }}', 'GO_MEETING_FACTS', 'FACT_BUILD', CURRENT_TIMESTAMP(), 'STARTED', 'SILVER', 'GOLD', CURRENT_USER(), 'DBT_CLOUD', CURRENT_DATE(), CURRENT_DATE() WHERE '{{ this.name }}' != 'go_process_audit'",
    post_hook="UPDATE {{ ref('go_process_audit') }} SET end_time = CURRENT_TIMESTAMP(), status = 'COMPLETED', records_processed = (SELECT COUNT(*) FROM {{ this }}), records_successful = (SELECT COUNT(*) FROM {{ this }}), processing_duration_seconds = DATEDIFF('second', start_time, CURRENT_TIMESTAMP()) WHERE execution_id = '{{ dbt_utils.generate_surrogate_key(['invocation_id', 'this.name']) }}' AND '{{ this.name }}' != 'go_process_audit'"
) }}

-- Gold Meeting Facts Table
-- Comprehensive meeting analytics with participant and feature usage metrics

WITH meeting_base AS (
    SELECT
        meeting_id,
        host_id,
        meeting_topic,
        start_time,
        end_time,
        duration_minutes,
        data_quality_score,
        source_system,
        load_date,
        update_date
    FROM {{ source('silver', 'si_meetings') }}
    WHERE record_status = 'ACTIVE'
        AND data_quality_score >= 0.7
),

participant_metrics AS (
    SELECT
        meeting_id,
        COUNT(DISTINCT participant_id) AS participant_count,
        SUM(DATEDIFF('minute', join_time, COALESCE(leave_time, CURRENT_TIMESTAMP()))) AS total_attendance_minutes
    FROM {{ source('silver', 'si_participants') }}
    WHERE record_status = 'ACTIVE'
    GROUP BY meeting_id
),

feature_metrics AS (
    SELECT
        meeting_id,
        SUM(CASE WHEN UPPER(feature_name) LIKE '%RECORDING%' THEN usage_count ELSE 0 END) > 0 AS recording_enabled,
        SUM(CASE WHEN UPPER(feature_name) LIKE '%SCREEN%' THEN usage_count ELSE 0 END) AS screen_share_count,
        SUM(CASE WHEN UPPER(feature_name) LIKE '%CHAT%' THEN usage_count ELSE 0 END) AS chat_message_count,
        SUM(CASE WHEN UPPER(feature_name) LIKE '%BREAKOUT%' THEN usage_count ELSE 0 END) AS breakout_room_count
    FROM {{ source('silver', 'si_feature_usage') }}
    WHERE record_status = 'ACTIVE'
    GROUP BY meeting_id
),

meeting_facts AS (
    SELECT
        CONCAT('MF_', mb.meeting_id, '_', CURRENT_TIMESTAMP()::STRING) AS meeting_fact_id,
        COALESCE(mb.meeting_id, 'UNKNOWN') AS meeting_id,
        CASE WHEN mb.host_id IS NOT NULL THEN mb.host_id ELSE 'UNKNOWN_HOST' END AS host_id,
        TRIM(COALESCE(mb.meeting_topic, 'No Topic Specified')) AS meeting_topic,
        CONVERT_TIMEZONE('UTC', mb.start_time) AS start_time,
        CONVERT_TIMEZONE('UTC', mb.end_time) AS end_time,
        CASE 
            WHEN mb.duration_minutes > 0 THEN mb.duration_minutes 
            ELSE DATEDIFF('minute', mb.start_time, COALESCE(mb.end_time, CURRENT_TIMESTAMP())) 
        END AS duration_minutes,
        COALESCE(pm.participant_count, 0) AS participant_count,
        COALESCE(pm.participant_count, 0) AS max_concurrent_participants,
        COALESCE(pm.total_attendance_minutes, 0) AS total_attendance_minutes,
        CASE 
            WHEN pm.participant_count > 0 THEN pm.total_attendance_minutes / pm.participant_count 
            ELSE 0 
        END AS average_attendance_duration,
        CASE 
            WHEN mb.duration_minutes < 15 THEN 'Quick Meeting'
            WHEN mb.duration_minutes < 60 THEN 'Standard Meeting'
            ELSE 'Extended Meeting'
        END AS meeting_type,
        CASE 
            WHEN mb.end_time IS NOT NULL THEN 'Completed'
            WHEN mb.start_time <= CURRENT_TIMESTAMP() THEN 'In Progress'
            ELSE 'Scheduled'
        END AS meeting_status,
        COALESCE(fm.recording_enabled, FALSE) AS recording_enabled,
        COALESCE(fm.screen_share_count, 0) AS screen_share_count,
        COALESCE(fm.chat_message_count, 0) AS chat_message_count,
        COALESCE(fm.breakout_room_count, 0) AS breakout_room_count,
        ROUND(COALESCE(mb.data_quality_score, 0), 2) AS quality_score_avg,
        ROUND(
            (COALESCE(fm.chat_message_count, 0) * 0.3 + 
             COALESCE(fm.screen_share_count, 0) * 0.4 + 
             COALESCE(pm.participant_count, 0) * 0.3) / 10, 2
        ) AS engagement_score,
        mb.load_date,
        CURRENT_DATE() AS update_date,
        mb.source_system
    FROM meeting_base mb
    LEFT JOIN participant_metrics pm ON mb.meeting_id = pm.meeting_id
    LEFT JOIN feature_metrics fm ON mb.meeting_id = fm.meeting_id
)

SELECT
    meeting_fact_id::VARCHAR(50) AS meeting_fact_id,
    meeting_id::VARCHAR(50) AS meeting_id,
    host_id::VARCHAR(50) AS host_id,
    meeting_topic::VARCHAR(500) AS meeting_topic,
    start_time,
    end_time,
    duration_minutes,
    participant_count,
    max_concurrent_participants,
    total_attendance_minutes,
    average_attendance_duration,
    meeting_type::VARCHAR(50) AS meeting_type,
    meeting_status::VARCHAR(50) AS meeting_status,
    recording_enabled,
    screen_share_count,
    chat_message_count,
    breakout_room_count,
    quality_score_avg,
    engagement_score,
    load_date,
    update_date,
    source_system::VARCHAR(100) AS source_system
FROM meeting_facts
