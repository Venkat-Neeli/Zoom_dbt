{{ config(
    materialized='table',
    schema='gold',
    cluster_by=['load_date'],
    pre_hook="ALTER SESSION SET TIMEZONE = 'UTC'",
    post_hook=[
        "ALTER TABLE {{ this }} SET CHANGE_TRACKING = TRUE",
        "GRANT SELECT ON {{ this }} TO ROLE ANALYTICS_READER"
    ]
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
        AND data_quality_score >= {{ var('min_quality_score', 3.0) }}
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
),

meeting_info AS (
    SELECT 
        meeting_id,
        host_id,
        meeting_topic,
        start_time,
        end_time,
        duration_minutes
    FROM {{ ref('si_meetings') }}
    WHERE record_status = 'ACTIVE'
),

participant_metrics AS (
    SELECT 
        pb.*,
        DATEDIFF('minute', pb.join_time, pb.leave_time) as participation_duration_minutes,
        DATEDIFF('minute', mi.start_time, pb.join_time) as join_delay_minutes,
        CASE 
            WHEN pb.leave_time >= mi.end_time THEN 'Full'
            WHEN DATEDIFF('minute', pb.join_time, pb.leave_time) >= (mi.duration_minutes * 0.8) THEN 'Mostly'
            WHEN DATEDIFF('minute', pb.join_time, pb.leave_time) >= (mi.duration_minutes * 0.5) THEN 'Partial'
            ELSE 'Brief'
        END as participation_level
    FROM participant_base pb
    JOIN meeting_info mi ON pb.meeting_id = mi.meeting_id
)

SELECT 
    UUID_STRING() as participant_fact_key,
    pm.participant_id,
    pm.meeting_id,
    pm.user_id,
    ui.user_name,
    ui.email,
    ui.company,
    ui.plan_type,
    mi.meeting_topic,
    mi.host_id,
    pm.join_time,
    pm.leave_time,
    pm.participation_duration_minutes,
    pm.join_delay_minutes,
    pm.participation_level,
    CASE 
        WHEN pm.join_delay_minutes <= 2 THEN 'On Time'
        WHEN pm.join_delay_minutes <= 5 THEN 'Slightly Late'
        ELSE 'Late'
    END as punctuality_category,
    CASE 
        WHEN pm.participation_duration_minutes >= 60 THEN 'Long'
        WHEN pm.participation_duration_minutes >= 30 THEN 'Medium'
        ELSE 'Short'
    END as participation_duration_category,
    pm.data_quality_score,
    pm.source_system,
    pm.load_date,
    pm.update_date,
    CURRENT_TIMESTAMP() as created_at,
    CURRENT_TIMESTAMP() as updated_at,
    'SUCCESS' as process_status
FROM participant_metrics pm
LEFT JOIN user_info ui ON pm.user_id = ui.user_id
LEFT JOIN meeting_info mi ON pm.meeting_id = mi.meeting_id
