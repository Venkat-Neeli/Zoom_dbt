{{ config(
    materialized='incremental',
    unique_key='meeting_fact_id',
    on_schema_change='sync_all_columns',
    pre_hook="INSERT INTO {{ ref('go_process_audit') }} (execution_id, pipeline_name, start_time, status, process_type, user_executed, source_system, target_system) VALUES (UUID_STRING(), 'go_meeting_facts_load', CURRENT_TIMESTAMP(), 'STARTED', 'FACT_LOAD', 'DBT_USER', 'SILVER', 'GOLD') WHERE '{{ this.name }}' != 'go_process_audit'",
    post_hook="UPDATE {{ ref('go_process_audit') }} SET end_time = CURRENT_TIMESTAMP(), status = 'COMPLETED', records_processed = (SELECT COUNT(*) FROM {{ this }}) WHERE pipeline_name = 'go_meeting_facts_load' AND status = 'STARTED' AND '{{ this.name }}' != 'go_process_audit'"
) }}

WITH meeting_base AS (
    SELECT 
        meeting_id,
        host_id,
        meeting_topic,
        start_time,
        end_time,
        duration_minutes,
        load_timestamp,
        update_timestamp,
        source_system,
        load_date,
        update_date,
        data_quality_score,
        record_status
    FROM {{ ref('si_meetings') }}
    WHERE record_status = 'ACTIVE'
        AND data_quality_score >= 0.7
        {% if is_incremental() %}
        AND update_date >= (SELECT MAX(update_date) FROM {{ this }})
        {% endif %}
),

participant_aggregates AS (
    SELECT 
        meeting_id,
        COUNT(DISTINCT participant_id) AS participant_count,
        SUM(DATEDIFF('minute', join_time, leave_time)) AS total_attendance_minutes
    FROM {{ ref('si_participants') }}
    WHERE record_status = 'ACTIVE'
    GROUP BY meeting_id
),

feature_usage_aggregates AS (
    SELECT 
        meeting_id,
        SUM(CASE WHEN feature_name = 'Recording' THEN 1 ELSE 0 END) > 0 AS recording_enabled,
        SUM(CASE WHEN feature_name = 'Screen Sharing' THEN usage_count ELSE 0 END) AS screen_share_count,
        SUM(CASE WHEN feature_name = 'Chat' THEN usage_count ELSE 0 END) AS chat_message_count,
        SUM(CASE WHEN feature_name = 'Breakout Rooms' THEN usage_count ELSE 0 END) AS breakout_room_count
    FROM {{ ref('si_feature_usage') }}
    WHERE record_status = 'ACTIVE'
    GROUP BY meeting_id
),

final_facts AS (
    SELECT 
        CONCAT('MF_', mb.meeting_id, '_', CURRENT_TIMESTAMP()::STRING) AS meeting_fact_id,
        COALESCE(mb.meeting_id, 'UNKNOWN') AS meeting_id,
        CASE WHEN mb.host_id IS NOT NULL THEN mb.host_id ELSE 'UNKNOWN_HOST' END AS host_id,
        TRIM(COALESCE(mb.meeting_topic, 'No Topic Specified')) AS meeting_topic,
        CONVERT_TIMEZONE('UTC', mb.start_time) AS start_time,
        CONVERT_TIMEZONE('UTC', mb.end_time) AS end_time,
        CASE 
            WHEN mb.duration_minutes > 0 THEN mb.duration_minutes 
            ELSE DATEDIFF('minute', mb.start_time, mb.end_time) 
        END AS duration_minutes,
        COALESCE(pa.participant_count, 0) AS participant_count,
        COALESCE(pa.participant_count, 0) AS max_concurrent_participants,
        COALESCE(pa.total_attendance_minutes, 0) AS total_attendance_minutes,
        CASE 
            WHEN pa.participant_count > 0 THEN pa.total_attendance_minutes / pa.participant_count 
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
        COALESCE(fua.recording_enabled, FALSE) AS recording_enabled,
        COALESCE(fua.screen_share_count, 0) AS screen_share_count,
        COALESCE(fua.chat_message_count, 0) AS chat_message_count,
        COALESCE(fua.breakout_room_count, 0) AS breakout_room_count,
        ROUND(mb.data_quality_score, 2) AS quality_score_avg,
        ROUND(
            (COALESCE(fua.chat_message_count, 0) * 0.3 + 
             COALESCE(fua.screen_share_count, 0) * 0.4 + 
             COALESCE(pa.participant_count, 0) * 0.3) / 10, 2
        ) AS engagement_score,
        mb.load_date,
        CURRENT_DATE() AS update_date,
        mb.source_system
    FROM meeting_base mb
    LEFT JOIN participant_aggregates pa ON mb.meeting_id = pa.meeting_id
    LEFT JOIN feature_usage_aggregates fua ON mb.meeting_id = fua.meeting_id
)

SELECT * FROM final_facts
