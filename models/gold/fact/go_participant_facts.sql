{{ config(
    materialized='incremental',
    unique_key='participant_fact_key',
    on_schema_change='sync_all_columns',
    pre_hook="INSERT INTO {{ ref('go_process_audit') }} (process_name, start_time, status) VALUES ('go_participant_facts', CURRENT_TIMESTAMP(), 'STARTED')",
    post_hook="INSERT INTO {{ ref('go_process_audit') }} (process_name, end_time, status) VALUES ('go_participant_facts', CURRENT_TIMESTAMP(), 'COMPLETED')"
) }}

WITH participant_base AS (
    SELECT 
        participant_id,
        meeting_id,
        user_id,
        CONVERT_TIMEZONE('UTC', join_time) AS join_time_utc,
        CONVERT_TIMEZONE('UTC', leave_time) AS leave_time_utc,
        load_date,
        update_date,
        source_system
    FROM {{ ref('si_participants') }}
    WHERE record_status = 'ACTIVE'
        AND data_quality_score >= 0.7
        {% if is_incremental() %}
        AND update_date > (SELECT MAX(update_date) FROM {{ this }})
        {% endif %}
),

meeting_info AS (
    SELECT 
        meeting_id,
        host_id,
        meeting_topic,
        CONVERT_TIMEZONE('UTC', start_time) AS meeting_start_time,
        CONVERT_TIMEZONE('UTC', end_time) AS meeting_end_time,
        duration_minutes AS meeting_duration
    FROM {{ ref('si_meetings') }}
    WHERE record_status = 'ACTIVE'
        AND data_quality_score >= 0.7
),

user_info AS (
    SELECT 
        user_id,
        user_name,
        email,
        company,
        plan_type
    FROM {{ ref('si_users') }}
    WHERE record_status = 'ACTIVE'
        AND data_quality_score >= 0.7
)

SELECT 
    UUID_STRING() AS participant_fact_key,
    pb.participant_id,
    pb.meeting_id,
    pb.user_id,
    ui.user_name,
    ui.email,
    ui.company,
    ui.plan_type,
    mi.meeting_topic,
    mi.host_id,
    pb.join_time_utc,
    pb.leave_time_utc,
    mi.meeting_start_time,
    mi.meeting_end_time,
    COALESCE(DATEDIFF('minute', pb.join_time_utc, pb.leave_time_utc), 0) AS participation_duration_minutes,
    COALESCE(mi.meeting_duration, 0) AS meeting_duration_minutes,
    CASE 
        WHEN DATEDIFF('minute', pb.join_time_utc, pb.leave_time_utc) > 0 AND mi.meeting_duration > 0 THEN
            ROUND((DATEDIFF('minute', pb.join_time_utc, pb.leave_time_utc)::FLOAT / mi.meeting_duration::FLOAT) * 100, 2)
        ELSE 0
    END AS attendance_rate,
    CASE 
        WHEN pb.join_time_utc <= DATEADD('minute', 5, mi.meeting_start_time) THEN 'On Time'
        WHEN pb.join_time_utc <= DATEADD('minute', 15, mi.meeting_start_time) THEN 'Late'
        ELSE 'Very Late'
    END AS join_timeliness,
    CASE 
        WHEN pb.leave_time_utc >= DATEADD('minute', -5, mi.meeting_end_time) THEN 'Stayed Until End'
        WHEN DATEDIFF('minute', pb.join_time_utc, pb.leave_time_utc) >= 30 THEN 'Partial Attendance'
        ELSE 'Early Leave'
    END AS leave_pattern,
    CASE 
        WHEN DATEDIFF('minute', pb.join_time_utc, pb.leave_time_utc) >= 30 THEN 1
        ELSE 0
    END AS is_engaged_participant,
    pb.load_date,
    pb.update_date,
    pb.source_system,
    CURRENT_TIMESTAMP() AS created_at
FROM participant_base pb
LEFT JOIN meeting_info mi ON pb.meeting_id = mi.meeting_id
LEFT JOIN user_info ui ON pb.user_id = ui.user_id
