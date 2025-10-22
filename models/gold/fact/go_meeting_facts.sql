{{ config(
    materialized='incremental',
    on_schema_change='sync_all_columns',
    unique_key='meeting_fact_id',
    pre_hook="INSERT INTO {{ ref('go_process_audit') }} (execution_id, pipeline_name, start_time, status, process_type, user_executed, load_date) SELECT UUID_STRING(), 'go_meeting_facts', CURRENT_TIMESTAMP(), 'STARTED', 'FACT_LOAD', 'DBT_USER', CURRENT_DATE() WHERE '{{ this.name }}' != 'go_process_audit'",
    post_hook="INSERT INTO {{ ref('go_process_audit') }} (execution_id, pipeline_name, end_time, status, records_processed, process_type, user_executed, load_date) SELECT UUID_STRING(), 'go_meeting_facts', CURRENT_TIMESTAMP(), 'COMPLETED', (SELECT COUNT(*) FROM {{ this }}), 'FACT_LOAD', 'DBT_USER', CURRENT_DATE() WHERE '{{ this.name }}' != 'go_process_audit'"
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
        AND update_date > (SELECT MAX(update_date) FROM {{ this }})
        {% endif %}
),

participant_aggregates AS (
    SELECT 
        p.meeting_id,
        COUNT(DISTINCT p.participant_id) AS participant_count,
        SUM(DATEDIFF('minute', p.join_time, p.leave_time)) AS total_attendance_minutes,
        AVG(p.data_quality_score) AS avg_participant_quality
    FROM {{ ref('si_participants') }} p
    WHERE p.record_status = 'ACTIVE'
    GROUP BY p.meeting_id
),

feature_aggregates AS (
    SELECT 
        fu.meeting_id,
        SUM(CASE WHEN fu.feature_name = 'Recording' THEN 1 ELSE 0 END) > 0 AS recording_enabled,
        SUM(CASE WHEN fu.feature_name = 'Screen Sharing' THEN fu.usage_count ELSE 0 END) AS screen_share_count,
        SUM(CASE WHEN fu.feature_name = 'Chat' THEN fu.usage_count ELSE 0 END) AS chat_message_count,
        SUM(CASE WHEN fu.feature_name = 'Breakout Rooms' THEN fu.usage_count ELSE 0 END) AS breakout_room_count
    FROM {{ ref('si_feature_usage') }} fu
    WHERE fu.record_status = 'ACTIVE'
    GROUP BY fu.meeting_id
),

final_meeting_facts AS (
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
        COALESCE(fa.recording_enabled, FALSE) AS recording_enabled,
        COALESCE(fa.screen_share_count, 0) AS screen_share_count,
        COALESCE(fa.chat_message_count, 0) AS chat_message_count,
        COALESCE(fa.breakout_room_count, 0) AS breakout_room_count,
        ROUND(COALESCE(mb.data_quality_score, 0), 2) AS quality_score_avg,
        ROUND(
            (COALESCE(fa.chat_message_count, 0) * 0.3 + 
             COALESCE(fa.screen_share_count, 0) * 0.4 + 
             COALESCE(pa.participant_count, 0) * 0.3) / 10, 2
        ) AS engagement_score,
        mb.load_date,
        CURRENT_DATE() AS update_date,
        mb.source_system
    FROM meeting_base mb
    LEFT JOIN participant_aggregates pa ON mb.meeting_id = pa.meeting_id
    LEFT JOIN feature_aggregates fa ON mb.meeting_id = fa.meeting_id
)

SELECT * FROM final_meeting_facts
