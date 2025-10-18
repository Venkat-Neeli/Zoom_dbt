{{ config(
    materialized='table',
    cluster_by=['meeting_date', 'host_id'],
    pre_hook="INSERT INTO {{ ref('go_process_audit') }} (process_name, table_name, start_time, status) VALUES ('go_meeting_facts_transform', 'go_meeting_facts', CURRENT_TIMESTAMP(), 'STARTED') WHERE '{{ this.name }}' != 'go_process_audit'",
    post_hook="INSERT INTO {{ ref('go_process_audit') }} (process_name, table_name, end_time, status, records_processed) VALUES ('go_meeting_facts_transform', 'go_meeting_facts', CURRENT_TIMESTAMP(), 'COMPLETED', (SELECT COUNT(*) FROM {{ this }})) WHERE '{{ this.name }}' != 'go_process_audit'"
) }}

WITH meeting_base AS (
    SELECT 
        meeting_id,
        host_id,
        meeting_topic,
        start_time,
        end_time,
        duration_minutes,
        meeting_type,
        timezone,
        created_at AS meeting_created_at,
        updated_at AS meeting_updated_at
    FROM {{ ref('si_meetings') }}
    WHERE meeting_id IS NOT NULL
),

participant_metrics AS (
    SELECT 
        meeting_id,
        COUNT(DISTINCT participant_id) AS total_participants,
        COUNT(DISTINCT user_id) AS unique_users,
        AVG(DATEDIFF('minute', join_time, leave_time)) AS avg_participation_duration,
        MAX(DATEDIFF('minute', join_time, leave_time)) AS max_participation_duration,
        MIN(DATEDIFF('minute', join_time, leave_time)) AS min_participation_duration
    FROM {{ ref('si_participants') }}
    WHERE meeting_id IS NOT NULL
    GROUP BY meeting_id
),

feature_usage_metrics AS (
    SELECT 
        meeting_id,
        COUNT(DISTINCT feature_name) AS features_used_count,
        SUM(usage_count) AS total_feature_usage
    FROM {{ ref('si_feature_usage') }}
    WHERE meeting_id IS NOT NULL
    GROUP BY meeting_id
),

host_info AS (
    SELECT 
        user_id,
        company,
        plan_type
    FROM {{ ref('si_users') }}
    WHERE user_id IS NOT NULL
)

SELECT 
    {{ dbt_utils.generate_surrogate_key(['mb.meeting_id']) }} AS meeting_fact_key,
    mb.meeting_id,
    mb.host_id,
    hi.company AS host_company,
    hi.plan_type AS host_plan_type,
    mb.meeting_topic,
    DATE(mb.start_time) AS meeting_date,
    mb.start_time,
    mb.end_time,
    mb.duration_minutes,
    mb.meeting_type,
    mb.timezone,
    COALESCE(pm.total_participants, 0) AS total_participants,
    COALESCE(pm.unique_users, 0) AS unique_users,
    COALESCE(pm.avg_participation_duration, 0) AS avg_participation_duration_minutes,
    COALESCE(pm.max_participation_duration, 0) AS max_participation_duration_minutes,
    COALESCE(pm.min_participation_duration, 0) AS min_participation_duration_minutes,
    COALESCE(fum.features_used_count, 0) AS features_used_count,
    COALESCE(fum.total_feature_usage, 0) AS total_feature_usage,
    CASE 
        WHEN mb.duration_minutes >= 60 THEN 'Long'
        WHEN mb.duration_minutes >= 30 THEN 'Medium'
        ELSE 'Short'
    END AS meeting_duration_category,
    CASE 
        WHEN COALESCE(pm.total_participants, 0) >= 10 THEN 'Large'
        WHEN COALESCE(pm.total_participants, 0) >= 5 THEN 'Medium'
        ELSE 'Small'
    END AS meeting_size_category,
    CURRENT_TIMESTAMP() AS created_at,
    CURRENT_TIMESTAMP() AS updated_at,
    'SUCCESS' AS process_status
FROM meeting_base mb
LEFT JOIN participant_metrics pm ON mb.meeting_id = pm.meeting_id
LEFT JOIN feature_usage_metrics fum ON mb.meeting_id = fum.meeting_id
LEFT JOIN host_info hi ON mb.host_id = hi.user_id
