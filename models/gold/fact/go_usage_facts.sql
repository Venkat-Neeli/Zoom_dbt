{{ config(
    materialized='table',
    cluster_by=['usage_date', 'user_id'],
    pre_hook="INSERT INTO {{ ref('go_process_audit') }} (process_name, table_name, start_time, status) VALUES ('go_usage_facts_transform', 'go_usage_facts', CURRENT_TIMESTAMP(), 'STARTED') WHERE '{{ this.name }}' != 'go_process_audit'",
    post_hook="INSERT INTO {{ ref('go_process_audit') }} (process_name, table_name, end_time, status, records_processed) VALUES ('go_usage_facts_transform', 'go_usage_facts', CURRENT_TIMESTAMP(), 'COMPLETED', (SELECT COUNT(*) FROM {{ this }})) WHERE '{{ this.name }}' != 'go_process_audit'"
) }}

WITH usage_base AS (
    SELECT 
        usage_id,
        meeting_id,
        feature_name,
        usage_count,
        usage_date,
        load_timestamp,
        update_timestamp,
        source_system,
        load_date,
        update_date,
        data_quality_score,
        record_status
    FROM {{ ref('si_feature_usage') }}
    WHERE usage_id IS NOT NULL
        AND record_status = 'ACTIVE'
),

meeting_context AS (
    SELECT 
        meeting_id,
        host_id,
        duration_minutes AS meeting_duration_minutes
    FROM {{ ref('si_meetings') }}
    WHERE meeting_id IS NOT NULL
        AND record_status = 'ACTIVE'
),

user_context AS (
    SELECT 
        user_id,
        user_name,
        email,
        company,
        plan_type
    FROM {{ ref('si_users') }}
    WHERE user_id IS NOT NULL
        AND record_status = 'ACTIVE'
),

user_daily_usage AS (
    SELECT 
        mc.host_id AS user_id,
        ub.usage_date,
        COUNT(DISTINCT ub.meeting_id) AS meeting_count,
        SUM(mc.meeting_duration_minutes) AS total_meeting_minutes,
        COUNT(DISTINCT CASE WHEN ub.feature_name = 'Recording' THEN ub.meeting_id END) AS webinar_count,
        SUM(CASE WHEN ub.feature_name = 'Recording' THEN ub.usage_count * 0.1 ELSE 0 END) AS recording_storage_gb,
        SUM(ub.usage_count) AS feature_usage_count,
        COUNT(DISTINCT p.user_id) AS unique_participants_hosted
    FROM usage_base ub
    LEFT JOIN meeting_context mc ON ub.meeting_id = mc.meeting_id
    LEFT JOIN {{ ref('si_participants') }} p ON ub.meeting_id = p.meeting_id AND p.record_status = 'ACTIVE'
    GROUP BY mc.host_id, ub.usage_date
)

SELECT 
    CONCAT('UF_', mc.host_id, '_', ub.usage_date::STRING) AS usage_fact_id,
    mc.host_id AS user_id,
    COALESCE(uc.company, 'INDIVIDUAL') AS organization_id,
    ub.usage_date,
    COALESCE(udu.meeting_count, 0) AS meeting_count,
    COALESCE(udu.total_meeting_minutes, 0) AS total_meeting_minutes,
    COALESCE(udu.webinar_count, 0) AS webinar_count,
    COALESCE(udu.total_meeting_minutes, 0) AS total_webinar_minutes,
    COALESCE(udu.recording_storage_gb, 0) AS recording_storage_gb,
    COALESCE(udu.feature_usage_count, 0) AS feature_usage_count,
    COALESCE(udu.unique_participants_hosted, 0) AS unique_participants_hosted,
    ub.load_date,
    CURRENT_DATE() AS update_date,
    ub.source_system
FROM usage_base ub
LEFT JOIN meeting_context mc ON ub.meeting_id = mc.meeting_id
LEFT JOIN user_context uc ON mc.host_id = uc.user_id
LEFT JOIN user_daily_usage udu ON mc.host_id = udu.user_id AND ub.usage_date = udu.usage_date
GROUP BY 
    mc.host_id, uc.company, ub.usage_date, udu.meeting_count, udu.total_meeting_minutes,
    udu.webinar_count, udu.recording_storage_gb, udu.feature_usage_count, 
    udu.unique_participants_hosted, ub.load_date, ub.source_system
