{{ config(
    materialized='table',
    cluster_by=['usage_date', 'organization_id'],
    pre_hook="INSERT INTO {{ ref('go_process_audit') }} (execution_id, pipeline_name, start_time, status, source_system, target_system, process_type, user_executed) VALUES (UUID_STRING(), 'go_usage_facts', CURRENT_TIMESTAMP(), 'STARTED', 'SILVER', 'GOLD', 'FACT_LOAD', CURRENT_USER()) WHERE '{{ this.name }}' != 'go_process_audit'",
    post_hook="UPDATE {{ ref('go_process_audit') }} SET end_time = CURRENT_TIMESTAMP(), status = 'COMPLETED', records_processed = (SELECT COUNT(*) FROM {{ this }}) WHERE pipeline_name = 'go_usage_facts' AND status = 'STARTED' AND '{{ this.name }}' != 'go_process_audit'"
) }}

WITH user_base AS (
    SELECT 
        u.user_id,
        COALESCE(u.company, 'INDIVIDUAL') AS organization_id
    FROM {{ ref('si_users') }} u
    WHERE u.record_status = 'ACTIVE'
),

meeting_usage AS (
    SELECT 
        m.host_id AS user_id,
        DATE(m.start_time) AS usage_date,
        COUNT(DISTINCT m.meeting_id) AS meeting_count,
        SUM(m.duration_minutes) AS total_meeting_minutes,
        m.load_date,
        m.source_system
    FROM {{ ref('si_meetings') }} m
    WHERE m.record_status = 'ACTIVE'
    GROUP BY m.host_id, DATE(m.start_time), m.load_date, m.source_system
),

webinar_usage AS (
    SELECT 
        w.host_id AS user_id,
        DATE(w.start_time) AS usage_date,
        COUNT(DISTINCT w.webinar_id) AS webinar_count,
        SUM(DATEDIFF('minute', w.start_time, w.end_time)) AS total_webinar_minutes
    FROM {{ ref('si_webinars') }} w
    WHERE w.record_status = 'ACTIVE'
    GROUP BY w.host_id, DATE(w.start_time)
),

feature_usage AS (
    SELECT 
        f.usage_date,
        SUM(f.usage_count) AS feature_usage_count,
        SUM(CASE WHEN f.feature_name = 'Recording' THEN f.usage_count * 0.1 ELSE 0 END) AS recording_storage_gb
    FROM {{ ref('si_feature_usage') }} f
    WHERE f.record_status = 'ACTIVE'
    GROUP BY f.usage_date
),

participant_interactions AS (
    SELECT 
        m.host_id AS user_id,
        DATE(p.join_time) AS usage_date,
        COUNT(DISTINCT p.user_id) AS unique_participants_hosted
    FROM {{ ref('si_participants') }} p
    JOIN {{ ref('si_meetings') }} m ON p.meeting_id = m.meeting_id
    WHERE p.record_status = 'ACTIVE' AND m.record_status = 'ACTIVE'
    GROUP BY m.host_id, DATE(p.join_time)
),

final_usage_facts AS (
    SELECT 
        CONCAT('UF_', ub.user_id, '_', mu.usage_date::STRING) AS usage_fact_id,
        ub.user_id,
        ub.organization_id,
        mu.usage_date,
        COALESCE(mu.meeting_count, 0) AS meeting_count,
        COALESCE(mu.total_meeting_minutes, 0) AS total_meeting_minutes,
        COALESCE(wu.webinar_count, 0) AS webinar_count,
        COALESCE(wu.total_webinar_minutes, 0) AS total_webinar_minutes,
        COALESCE(fu.recording_storage_gb, 0) AS recording_storage_gb,
        COALESCE(fu.feature_usage_count, 0) AS feature_usage_count,
        COALESCE(pi.unique_participants_hosted, 0) AS unique_participants_hosted,
        mu.load_date,
        CURRENT_DATE() AS update_date,
        mu.source_system
    FROM user_base ub
    JOIN meeting_usage mu ON ub.user_id = mu.user_id
    LEFT JOIN webinar_usage wu ON ub.user_id = wu.user_id AND mu.usage_date = wu.usage_date
    LEFT JOIN feature_usage fu ON mu.usage_date = fu.usage_date
    LEFT JOIN participant_interactions pi ON ub.user_id = pi.user_id AND mu.usage_date = pi.usage_date
)

SELECT * FROM final_usage_facts
