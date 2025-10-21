{{ config(
    materialized='incremental',
    on_schema_change='sync_all_columns',
    unique_key='usage_fact_id'
) }}

WITH user_daily_usage AS (
    SELECT 
        u.user_id,
        COALESCE(u.company, 'INDIVIDUAL') AS organization_id,
        fu.usage_date,
        u.load_date,
        u.source_system
    FROM {{ ref('si_users') }} u
    CROSS JOIN (
        SELECT DISTINCT usage_date 
        FROM {{ ref('si_feature_usage') }}
        WHERE record_status = 'ACTIVE'
        {% if is_incremental() %}
            AND update_date > (SELECT MAX(update_date) FROM {{ this }})
        {% endif %}
    ) fu
    WHERE u.record_status = 'ACTIVE'
),

meeting_stats AS (
    SELECT 
        m.host_id AS user_id,
        DATE(m.start_time) AS usage_date,
        COUNT(DISTINCT m.meeting_id) AS meeting_count,
        SUM(m.duration_minutes) AS total_meeting_minutes
    FROM {{ ref('si_meetings') }} m
    WHERE m.record_status = 'ACTIVE'
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
    GROUP BY w.host_id, DATE(w.start_time)
),

feature_stats AS (
    SELECT 
        m.host_id AS user_id,
        f.usage_date,
        SUM(f.usage_count) AS feature_usage_count,
        SUM(CASE WHEN f.feature_name = 'Recording' THEN f.usage_count * 0.1 ELSE 0 END) AS recording_storage_gb
    FROM {{ ref('si_feature_usage') }} f
    LEFT JOIN {{ ref('si_meetings') }} m ON f.meeting_id = m.meeting_id
    WHERE f.record_status = 'ACTIVE' AND m.record_status = 'ACTIVE'
    GROUP BY m.host_id, f.usage_date
),

participant_stats AS (
    SELECT 
        m.host_id AS user_id,
        DATE(p.join_time) AS usage_date,
        COUNT(DISTINCT p.user_id) AS unique_participants_hosted
    FROM {{ ref('si_participants') }} p
    LEFT JOIN {{ ref('si_meetings') }} m ON p.meeting_id = m.meeting_id
    WHERE p.record_status = 'ACTIVE' AND m.record_status = 'ACTIVE'
    GROUP BY m.host_id, DATE(p.join_time)
),

final_transform AS (
    SELECT 
        CONCAT('UF_', udu.user_id, '_', udu.usage_date::STRING) AS usage_fact_id,
        udu.user_id,
        udu.organization_id,
        udu.usage_date,
        COALESCE(ms.meeting_count, 0) AS meeting_count,
        COALESCE(ms.total_meeting_minutes, 0) AS total_meeting_minutes,
        COALESCE(ws.webinar_count, 0) AS webinar_count,
        COALESCE(ws.total_webinar_minutes, 0) AS total_webinar_minutes,
        COALESCE(fs.recording_storage_gb, 0) AS recording_storage_gb,
        COALESCE(fs.feature_usage_count, 0) AS feature_usage_count,
        COALESCE(ps.unique_participants_hosted, 0) AS unique_participants_hosted,
        udu.load_date,
        CURRENT_DATE() AS update_date,
        udu.source_system
    FROM user_daily_usage udu
    LEFT JOIN meeting_stats ms ON udu.user_id = ms.user_id AND udu.usage_date = ms.usage_date
    LEFT JOIN webinar_stats ws ON udu.user_id = ws.user_id AND udu.usage_date = ws.usage_date
    LEFT JOIN feature_stats fs ON udu.user_id = fs.user_id AND udu.usage_date = fs.usage_date
    LEFT JOIN participant_stats ps ON udu.user_id = ps.user_id AND udu.usage_date = ps.usage_date
    WHERE (ms.meeting_count > 0 OR ws.webinar_count > 0 OR fs.feature_usage_count > 0)
)

SELECT * FROM final_transform
