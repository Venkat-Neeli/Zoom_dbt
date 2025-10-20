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
        AND data_quality_score >= {{ var('min_quality_score', 3.0) }}
        AND duration_minutes BETWEEN {{ var('min_meeting_duration_minutes', 1) }} 
            AND {{ var('max_meeting_duration_minutes', 1440) }}
),

host_info AS (
    SELECT 
        user_id,
        user_name,
        company,
        plan_type
    FROM {{ ref('si_users') }}
    WHERE record_status = 'ACTIVE'
),

participant_counts AS (
    SELECT 
        meeting_id,
        COUNT(DISTINCT user_id) as participant_count,
        AVG(DATEDIFF('minute', join_time, leave_time)) as avg_participation_duration
    FROM {{ ref('si_participants') }}
    WHERE record_status = 'ACTIVE'
    GROUP BY meeting_id
),

feature_usage_agg AS (
    SELECT 
        meeting_id,
        COUNT(DISTINCT feature_name) as features_used_count,
        SUM(usage_count) as total_feature_usage
    FROM {{ ref('si_feature_usage') }}
    WHERE record_status = 'ACTIVE'
    GROUP BY meeting_id
)

SELECT 
    UUID_STRING() as meeting_fact_key,
    mb.meeting_id,
    mb.host_id,
    hi.user_name as host_name,
    hi.company as host_company,
    hi.plan_type as host_plan_type,
    mb.meeting_topic,
    mb.start_time,
    mb.end_time,
    mb.duration_minutes,
    COALESCE(pc.participant_count, 0) as participant_count,
    COALESCE(pc.avg_participation_duration, 0) as avg_participation_duration_minutes,
    COALESCE(fua.features_used_count, 0) as features_used_count,
    COALESCE(fua.total_feature_usage, 0) as total_feature_usage,
    CASE 
        WHEN mb.duration_minutes >= 60 THEN 'Long'
        WHEN mb.duration_minutes >= 30 THEN 'Medium'
        ELSE 'Short'
    END as meeting_duration_category,
    CASE 
        WHEN COALESCE(pc.participant_count, 0) >= 10 THEN 'Large'
        WHEN COALESCE(pc.participant_count, 0) >= 3 THEN 'Medium'
        ELSE 'Small'
    END as meeting_size_category,
    mb.data_quality_score,
    mb.source_system,
    mb.load_date,
    mb.update_date,
    CURRENT_TIMESTAMP() as created_at,
    CURRENT_TIMESTAMP() as updated_at,
    'SUCCESS' as process_status
FROM meeting_base mb
LEFT JOIN host_info hi ON mb.host_id = hi.user_id
LEFT JOIN participant_counts pc ON mb.meeting_id = pc.meeting_id
LEFT JOIN feature_usage_agg fua ON mb.meeting_id = fua.meeting_id
