{{ config(
    materialized='incremental',
    unique_key='meeting_fact_key',
    on_schema_change='sync_all_columns',
    pre_hook="INSERT INTO {{ ref('go_process_audit') }} (process_name, start_time, status) VALUES ('go_meeting_facts', CURRENT_TIMESTAMP(), 'STARTED')",
    post_hook="INSERT INTO {{ ref('go_process_audit') }} (process_name, end_time, status) VALUES ('go_meeting_facts', CURRENT_TIMESTAMP(), 'COMPLETED')"
) }}

WITH meeting_base AS (
    SELECT 
        meeting_id,
        host_id,
        meeting_topic,
        CONVERT_TIMEZONE('UTC', start_time) AS start_time_utc,
        CONVERT_TIMEZONE('UTC', end_time) AS end_time_utc,
        duration_minutes,
        load_date,
        update_date,
        source_system
    FROM {{ ref('si_meetings') }}
    WHERE record_status = 'ACTIVE'
        AND data_quality_score >= 0.7
        {% if is_incremental() %}
        AND update_date > (SELECT MAX(update_date) FROM {{ this }})
        {% endif %}
),

participant_aggregates AS (
    SELECT 
        meeting_id,
        COUNT(DISTINCT participant_id) AS total_participants,
        AVG(DATEDIFF('minute', join_time, leave_time)) AS avg_participation_duration,
        SUM(CASE WHEN DATEDIFF('minute', join_time, leave_time) > 5 THEN 1 ELSE 0 END) AS engaged_participants
    FROM {{ ref('si_participants') }}
    WHERE record_status = 'ACTIVE'
        AND data_quality_score >= 0.7
    GROUP BY meeting_id
),

feature_aggregates AS (
    SELECT 
        meeting_id,
        COUNT(DISTINCT feature_name) AS features_used,
        SUM(usage_count) AS total_feature_usage
    FROM {{ ref('si_feature_usage') }}
    WHERE record_status = 'ACTIVE'
        AND data_quality_score >= 0.7
    GROUP BY meeting_id
),

host_info AS (
    SELECT 
        user_id,
        user_name,
        company,
        plan_type
    FROM {{ ref('si_users') }}
    WHERE record_status = 'ACTIVE'
        AND data_quality_score >= 0.7
)

SELECT 
    UUID_STRING() AS meeting_fact_key,
    mb.meeting_id,
    mb.host_id,
    hi.user_name AS host_name,
    hi.company AS host_company,
    hi.plan_type AS host_plan_type,
    mb.meeting_topic,
    mb.start_time_utc,
    mb.end_time_utc,
    COALESCE(mb.duration_minutes, 0) AS duration_minutes,
    COALESCE(pa.total_participants, 0) AS total_participants,
    COALESCE(pa.avg_participation_duration, 0) AS avg_participation_duration,
    COALESCE(pa.engaged_participants, 0) AS engaged_participants,
    COALESCE(fa.features_used, 0) AS features_used,
    COALESCE(fa.total_feature_usage, 0) AS total_feature_usage,
    CASE 
        WHEN pa.total_participants > 0 THEN 
            ROUND((pa.engaged_participants::FLOAT / pa.total_participants::FLOAT) * 100, 2)
        ELSE 0 
    END AS engagement_rate,
    CASE 
        WHEN mb.duration_minutes > 60 THEN 'Long'
        WHEN mb.duration_minutes > 30 THEN 'Medium'
        ELSE 'Short'
    END AS meeting_duration_category,
    mb.load_date,
    mb.update_date,
    mb.source_system,
    CURRENT_TIMESTAMP() AS created_at
FROM meeting_base mb
LEFT JOIN participant_aggregates pa ON mb.meeting_id = pa.meeting_id
LEFT JOIN feature_aggregates fa ON mb.meeting_id = fa.meeting_id
LEFT JOIN host_info hi ON mb.host_id = hi.user_id
