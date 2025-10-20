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

WITH support_base AS (
    SELECT 
        ticket_id,
        user_id,
        ticket_type,
        resolution_status,
        open_date,
        load_timestamp,
        update_timestamp,
        source_system,
        load_date,
        update_date,
        data_quality_score,
        record_status
    FROM {{ ref('si_support_tickets') }}
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

user_meeting_stats AS (
    SELECT 
        host_id as user_id,
        COUNT(*) as total_meetings_hosted,
        AVG(duration_minutes) as avg_meeting_duration,
        AVG(data_quality_score) as avg_meeting_quality_score
    FROM {{ ref('si_meetings') }}
    WHERE record_status = 'ACTIVE'
        AND load_date >= DATEADD('day', -30, CURRENT_DATE())
    GROUP BY host_id
),

user_participation_stats AS (
    SELECT 
        user_id,
        COUNT(*) as total_meetings_attended,
        AVG(DATEDIFF('minute', join_time, leave_time)) as avg_participation_duration,
        AVG(data_quality_score) as avg_participation_quality_score
    FROM {{ ref('si_participants') }}
    WHERE record_status = 'ACTIVE'
        AND load_date >= DATEADD('day', -30, CURRENT_DATE())
    GROUP BY user_id
),

quality_metrics AS (
    SELECT 
        sb.*,
        CASE 
            WHEN sb.ticket_type IN ('AUDIO_ISSUE', 'VIDEO_ISSUE', 'CONNECTION_ISSUE') THEN 'Technical'
            WHEN sb.ticket_type IN ('BILLING_ISSUE', 'ACCOUNT_ISSUE') THEN 'Account'
            WHEN sb.ticket_type IN ('FEATURE_REQUEST', 'HOW_TO') THEN 'Support'
            ELSE 'Other'
        END as issue_category,
        CASE 
            WHEN sb.resolution_status = 'RESOLVED' THEN 'Resolved'
            WHEN sb.resolution_status = 'IN_PROGRESS' THEN 'In Progress'
            WHEN sb.resolution_status = 'OPEN' THEN 'Open'
            ELSE 'Unknown'
        END as resolution_category
    FROM support_base sb
)

SELECT 
    UUID_STRING() as quality_fact_key,
    qm.ticket_id,
    qm.user_id,
    ui.user_name,
    ui.email,
    ui.company,
    ui.plan_type,
    qm.ticket_type,
    qm.issue_category,
    qm.resolution_status,
    qm.resolution_category,
    qm.open_date,
    COALESCE(ums.total_meetings_hosted, 0) as total_meetings_hosted_30d,
    COALESCE(ums.avg_meeting_duration, 0) as avg_meeting_duration_30d,
    COALESCE(ums.avg_meeting_quality_score, 0) as avg_meeting_quality_score_30d,
    COALESCE(ups.total_meetings_attended, 0) as total_meetings_attended_30d,
    COALESCE(ups.avg_participation_duration, 0) as avg_participation_duration_30d,
    COALESCE(ups.avg_participation_quality_score, 0) as avg_participation_quality_score_30d,
    CASE 
        WHEN COALESCE(ums.avg_meeting_quality_score, 0) >= 4.0 AND COALESCE(ups.avg_participation_quality_score, 0) >= 4.0 THEN 'High'
        WHEN COALESCE(ums.avg_meeting_quality_score, 0) >= 3.0 AND COALESCE(ups.avg_participation_quality_score, 0) >= 3.0 THEN 'Medium'
        ELSE 'Low'
    END as overall_quality_rating,
    qm.data_quality_score,
    qm.source_system,
    qm.load_date,
    qm.update_date,
    CURRENT_TIMESTAMP() as created_at,
    CURRENT_TIMESTAMP() as updated_at,
    'SUCCESS' as process_status
FROM quality_metrics qm
LEFT JOIN user_info ui ON qm.user_id = ui.user_id
LEFT JOIN user_meeting_stats ums ON qm.user_id = ums.user_id
LEFT JOIN user_participation_stats ups ON qm.user_id = ups.user_id
