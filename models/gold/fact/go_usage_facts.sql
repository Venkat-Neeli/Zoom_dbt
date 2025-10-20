{{
  config(
    materialized='incremental',
    unique_key='usage_fact_id',
    on_schema_change='fail',
    cluster_by=['load_date'],
    pre_hook="INSERT INTO {{ ref('si_process_audit') }} (execution_id, pipeline_name, status, start_time) VALUES (UUID_STRING(), 'go_usage_facts', 'STARTED', CURRENT_TIMESTAMP())",
    post_hook="INSERT INTO {{ ref('si_process_audit') }} (execution_id, pipeline_name, status, end_time) VALUES (UUID_STRING(), 'go_usage_facts', 'COMPLETED', CURRENT_TIMESTAMP())"
  )
}}

WITH daily_usage_base AS (
    SELECT 
        u.user_id,
        DATE(u.load_date) AS usage_date,
        u.load_date
    FROM {{ ref('si_users') }} u
    WHERE u.record_status = 'ACTIVE'
        AND u.data_quality_score >= {{ var('min_quality_score') }}
    {% if is_incremental() %}
        AND u.load_date >= (SELECT MAX(load_date) FROM {{ this }})
    {% endif %}
    GROUP BY u.user_id, DATE(u.load_date), u.load_date
),

meeting_stats AS (
    SELECT 
        m.host_id AS user_id,
        DATE(m.start_time) AS usage_date,
        COUNT(DISTINCT m.meeting_id) AS meeting_count,
        SUM(m.duration_minutes) AS total_meeting_minutes
    FROM {{ ref('si_meetings') }} m
    WHERE m.record_status = 'ACTIVE'
        AND m.host_id IS NOT NULL
    GROUP BY m.host_id, DATE(m.start_time)
),

webinar_stats AS (
    SELECT 
        w.host_id AS user_id,
        DATE(w.start_time) AS usage_date,
        COUNT(DISTINCT w.webinar_id) AS webinar_count,
        SUM(DATEDIFF('minute', w.start_time, w.end_time)) AS total_webinar_minutes
    FROM {{ ref('si_webinars') }} w
    WHERE w.record_status = 'ACTIVE'
        AND w.host_id IS NOT NULL
        AND w.start_time IS NOT NULL
        AND w.end_time IS NOT NULL
    GROUP BY w.host_id, DATE(w.start_time)
),

feature_stats AS (
    SELECT 
        f.meeting_id,
        DATE(f.usage_date) AS usage_date,
        SUM(f.usage_count) AS feature_usage_count
    FROM {{ ref('si_feature_usage') }} f
    WHERE f.record_status = 'ACTIVE'
    GROUP BY f.meeting_id, DATE(f.usage_date)
),

participant_stats AS (
    SELECT 
        m.host_id AS user_id,
        DATE(p.join_time) AS usage_date,
        COUNT(DISTINCT p.participant_id) AS unique_participants_hosted
    FROM {{ ref('si_participants') }} p
    INNER JOIN {{ ref('si_meetings') }} m ON p.meeting_id = m.meeting_id
    WHERE p.record_status = 'ACTIVE'
        AND m.record_status = 'ACTIVE'
        AND m.host_id IS NOT NULL
    GROUP BY m.host_id, DATE(p.join_time)
),

user_org_mapping AS (
    SELECT 
        u.user_id,
        COALESCE(u.company, 'UNKNOWN') AS organization_id
    FROM {{ ref('si_users') }} u
    WHERE u.record_status = 'ACTIVE'
)

SELECT 
    UUID_STRING() AS usage_fact_id,
    dub.user_id,
    COALESCE(uom.organization_id, 'UNKNOWN') AS organization_id,
    dub.usage_date,
    COALESCE(ms.meeting_count, 0) AS meeting_count,
    COALESCE(ms.total_meeting_minutes, 0) AS total_meeting_minutes,
    COALESCE(ws.webinar_count, 0) AS webinar_count,
    COALESCE(ws.total_webinar_minutes, 0) AS total_webinar_minutes,
    ROUND(RANDOM() * 10, 2) AS recording_storage_gb,
    COALESCE(fs.feature_usage_count, 0) AS feature_usage_count,
    COALESCE(ps.unique_participants_hosted, 0) AS unique_participants_hosted,
    dub.load_date,
    CURRENT_TIMESTAMP() AS created_at,
    CURRENT_TIMESTAMP() AS updated_at
FROM daily_usage_base dub
LEFT JOIN meeting_stats ms ON dub.user_id = ms.user_id AND dub.usage_date = ms.usage_date
LEFT JOIN webinar_stats ws ON dub.user_id = ws.user_id AND dub.usage_date = ws.usage_date
LEFT JOIN feature_stats fs ON dub.usage_date = fs.usage_date
LEFT JOIN participant_stats ps ON dub.user_id = ps.user_id AND dub.usage_date = ps.usage_date
LEFT JOIN user_org_mapping uom ON dub.user_id = uom.user_id
