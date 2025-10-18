{{ config(
    materialized='table',
    cluster_by=['quality_date', 'meeting_id'],
    pre_hook="INSERT INTO {{ ref('go_process_audit') }} (process_name, table_name, start_time, status) VALUES ('go_quality_facts_transform', 'go_quality_facts', CURRENT_TIMESTAMP(), 'STARTED') WHERE '{{ this.name }}' != 'go_process_audit'",
    post_hook="INSERT INTO {{ ref('go_process_audit') }} (process_name, table_name, end_time, status, records_processed) VALUES ('go_quality_facts_transform', 'go_quality_facts', CURRENT_TIMESTAMP(), 'COMPLETED', (SELECT COUNT(*) FROM {{ this }})) WHERE '{{ this.name }}' != 'go_process_audit'"
) }}

WITH meeting_quality_base AS (
    SELECT 
        meeting_id,
        host_id,
        meeting_topic,
        start_time,
        end_time,
        duration_minutes,
        meeting_type,
        created_at AS meeting_created_at
    FROM {{ ref('si_meetings') }}
    WHERE meeting_id IS NOT NULL
),

participant_quality_metrics AS (
    SELECT 
        meeting_id,
        COUNT(DISTINCT participant_id) AS total_participants,
        COUNT(DISTINCT user_id) AS unique_users,
        AVG(DATEDIFF('minute', join_time, leave_time)) AS avg_participation_duration,
        COUNT(CASE WHEN DATEDIFF('minute', join_time, leave_time) < 5 THEN 1 END) AS short_participation_count,
        COUNT(CASE WHEN join_time > start_time + INTERVAL '10 minutes' THEN 1 END) AS late_joiners_count,
        COUNT(CASE WHEN leave_time < end_time - INTERVAL '10 minutes' THEN 1 END) AS early_leavers_count
    FROM {{ ref('si_participants') }} p
    JOIN meeting_quality_base m ON p.meeting_id = m.meeting_id
    GROUP BY meeting_id
),

support_quality_metrics AS (
    SELECT 
        st.user_id,
        COUNT(*) AS total_tickets,
        COUNT(CASE WHEN resolution_status = 'RESOLVED' THEN 1 END) AS resolved_tickets,
        COUNT(CASE WHEN resolution_status = 'OPEN' THEN 1 END) AS open_tickets,
        AVG(DATEDIFF('day', open_date, close_date)) AS avg_resolution_days
    FROM {{ ref('si_support_tickets') }} st
    GROUP BY st.user_id
),

feature_usage_quality AS (
    SELECT 
        meeting_id,
        COUNT(DISTINCT feature_name) AS features_used,
        SUM(usage_count) AS total_feature_usage,
        COUNT(CASE WHEN feature_name IN ('screen_share', 'recording', 'chat') THEN 1 END) AS core_features_used
    FROM {{ ref('si_feature_usage') }}
    GROUP BY meeting_id
),

user_context AS (
    SELECT 
        user_id,
        user_name,
        company,
        plan_type
    FROM {{ ref('si_users') }}
    WHERE user_id IS NOT NULL
)

SELECT 
    {{ dbt_utils.generate_surrogate_key(['mqb.meeting_id']) }} AS quality_fact_key,
    mqb.meeting_id,
    mqb.host_id,
    uc.user_name AS host_name,
    uc.company AS host_company,
    uc.plan_type AS host_plan_type,
    mqb.meeting_topic,
    DATE(mqb.start_time) AS quality_date,
    mqb.start_time,
    mqb.end_time,
    mqb.duration_minutes,
    mqb.meeting_type,
    COALESCE(pqm.total_participants, 0) AS total_participants,
    COALESCE(pqm.unique_users, 0) AS unique_users,
    COALESCE(pqm.avg_participation_duration, 0) AS avg_participation_duration_minutes,
    COALESCE(pqm.short_participation_count, 0) AS short_participation_count,
    COALESCE(pqm.late_joiners_count, 0) AS late_joiners_count,
    COALESCE(pqm.early_leavers_count, 0) AS early_leavers_count,
    COALESCE(fuq.features_used, 0) AS features_used_count,
    COALESCE(fuq.total_feature_usage, 0) AS total_feature_usage,
    COALESCE(fuq.core_features_used, 0) AS core_features_used_count,
    COALESCE(sqm.total_tickets, 0) AS host_total_support_tickets,
    COALESCE(sqm.resolved_tickets, 0) AS host_resolved_support_tickets,
    COALESCE(sqm.open_tickets, 0) AS host_open_support_tickets,
    COALESCE(sqm.avg_resolution_days, 0) AS host_avg_ticket_resolution_days,
    -- Quality Metrics Calculations
    ROUND(
        (COALESCE(pqm.avg_participation_duration, 0) * 100.0) / NULLIF(mqb.duration_minutes, 0), 2
    ) AS participation_quality_percentage,
    ROUND(
        (COALESCE(pqm.short_participation_count, 0) * 100.0) / NULLIF(pqm.total_participants, 0), 2
    ) AS short_participation_rate,
    ROUND(
        (COALESCE(pqm.late_joiners_count, 0) * 100.0) / NULLIF(pqm.total_participants, 0), 2
    ) AS late_joiner_rate,
    ROUND(
        (COALESCE(pqm.early_leavers_count, 0) * 100.0) / NULLIF(pqm.total_participants, 0), 2
    ) AS early_leaver_rate,
    ROUND(
        (COALESCE(fuq.core_features_used, 0) * 100.0) / NULLIF(fuq.features_used, 0), 2
    ) AS core_feature_adoption_rate,
    -- Quality Categories
    CASE 
        WHEN ROUND((COALESCE(pqm.avg_participation_duration, 0) * 100.0) / NULLIF(mqb.duration_minutes, 0), 2) >= 80 THEN 'High'
        WHEN ROUND((COALESCE(pqm.avg_participation_duration, 0) * 100.0) / NULLIF(mqb.duration_minutes, 0), 2) >= 60 THEN 'Medium'
        ELSE 'Low'
    END AS participation_quality_category,
    CASE 
        WHEN COALESCE(fuq.features_used, 0) >= 5 THEN 'High Feature Usage'
        WHEN COALESCE(fuq.features_used, 0) >= 3 THEN 'Medium Feature Usage'
        ELSE 'Low Feature Usage'
    END AS feature_usage_category,
    CASE 
        WHEN COALESCE(sqm.open_tickets, 0) = 0 THEN 'No Issues'
        WHEN COALESCE(sqm.open_tickets, 0) <= 2 THEN 'Minor Issues'
        ELSE 'Major Issues'
    END AS support_quality_category,
    -- Overall Quality Score (0-100)
    ROUND(
        (
            LEAST(ROUND((COALESCE(pqm.avg_participation_duration, 0) * 100.0) / NULLIF(mqb.duration_minutes, 0), 2), 100) * 0.4 +
            LEAST((COALESCE(fuq.features_used, 0) * 10), 100) * 0.3 +
            GREATEST(100 - (COALESCE(pqm.late_joiners_count, 0) + COALESCE(pqm.early_leavers_count, 0)) * 10, 0) * 0.3
        ), 2
    ) AS overall_quality_score,
    CURRENT_TIMESTAMP() AS created_at,
    CURRENT_TIMESTAMP() AS updated_at,
    'SUCCESS' AS process_status
FROM meeting_quality_base mqb
LEFT JOIN participant_quality_metrics pqm ON mqb.meeting_id = pqm.meeting_id
LEFT JOIN support_quality_metrics sqm ON mqb.host_id = sqm.user_id
LEFT JOIN feature_usage_quality fuq ON mqb.meeting_id = fuq.meeting_id
LEFT JOIN user_context uc ON mqb.host_id = uc.user_id
