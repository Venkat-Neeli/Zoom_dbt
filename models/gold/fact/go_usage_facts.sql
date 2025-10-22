{{ config(
    materialized='table',
    cluster_by=['usage_date', 'user_id']
) }}

-- Usage Facts Transformation
WITH user_daily_usage AS (
    SELECT 
        u.user_id,
        COALESCE(u.company, 'INDIVIDUAL') AS organization_id,
        fu.usage_date,
        u.load_date,
        u.source_system
    FROM {{ source('silver', 'si_users') }} u
    CROSS JOIN (
        SELECT DISTINCT usage_date 
        FROM {{ source('silver', 'si_feature_usage') }}
        WHERE record_status = 'ACTIVE'
    ) fu
    WHERE u.record_status = 'ACTIVE'
),

meeting_metrics AS (
    SELECT 
        host_id AS user_id,
        CAST(start_time AS DATE) AS usage_date,
        COUNT(DISTINCT meeting_id) AS meeting_count,
        SUM(duration_minutes) AS total_meeting_minutes
    FROM {{ source('silver', 'si_meetings') }}
    WHERE record_status = 'ACTIVE'
      AND start_time IS NOT NULL
    GROUP BY host_id, CAST(start_time AS DATE)
),

webinar_metrics AS (
    SELECT 
        host_id AS user_id,
        CAST(start_time AS DATE) AS usage_date,
        COUNT(DISTINCT webinar_id) AS webinar_count,
        SUM(DATEDIFF('minute', start_time, end_time)) AS total_webinar_minutes
    FROM {{ source('silver', 'si_webinars') }}
    WHERE record_status = 'ACTIVE'
      AND start_time IS NOT NULL
      AND end_time IS NOT NULL
    GROUP BY host_id, CAST(start_time AS DATE)
),

feature_metrics AS (
    SELECT 
        m.host_id AS user_id,
        fu.usage_date,
        SUM(CASE WHEN fu.feature_name = 'Recording' THEN fu.usage_count * 0.1 ELSE 0 END) AS recording_storage_gb,
        SUM(fu.usage_count) AS feature_usage_count
    FROM {{ source('silver', 'si_feature_usage') }} fu
    JOIN {{ source('silver', 'si_meetings') }} m ON fu.meeting_id = m.meeting_id
    WHERE fu.record_status = 'ACTIVE'
      AND m.record_status = 'ACTIVE'
    GROUP BY m.host_id, fu.usage_date
),

participant_metrics AS (
    SELECT 
        m.host_id AS user_id,
        CAST(m.start_time AS DATE) AS usage_date,
        COUNT(DISTINCT p.user_id) AS unique_participants_hosted
    FROM {{ source('silver', 'si_meetings') }} m
    JOIN {{ source('silver', 'si_participants') }} p ON m.meeting_id = p.meeting_id
    WHERE m.record_status = 'ACTIVE'
      AND p.record_status = 'ACTIVE'
      AND m.start_time IS NOT NULL
    GROUP BY m.host_id, CAST(m.start_time AS DATE)
)

SELECT 
    CONCAT('UF_', udu.user_id, '_', udu.usage_date::STRING)::VARCHAR(50) AS usage_fact_id,
    udu.user_id::VARCHAR(50) AS user_id,
    UPPER(TRIM(udu.organization_id))::VARCHAR(50) AS organization_id,
    udu.usage_date,
    COALESCE(mm.meeting_count, 0) AS meeting_count,
    COALESCE(mm.total_meeting_minutes, 0) AS total_meeting_minutes,
    COALESCE(wm.webinar_count, 0) AS webinar_count,
    COALESCE(wm.total_webinar_minutes, 0) AS total_webinar_minutes,
    COALESCE(fm.recording_storage_gb, 0) AS recording_storage_gb,
    COALESCE(fm.feature_usage_count, 0) AS feature_usage_count,
    COALESCE(pm.unique_participants_hosted, 0) AS unique_participants_hosted,
    udu.load_date,
    CURRENT_DATE() AS update_date,
    udu.source_system
FROM user_daily_usage udu
LEFT JOIN meeting_metrics mm ON udu.user_id = mm.user_id AND udu.usage_date = mm.usage_date
LEFT JOIN webinar_metrics wm ON udu.user_id = wm.user_id AND udu.usage_date = wm.usage_date
LEFT JOIN feature_metrics fm ON udu.user_id = fm.user_id AND udu.usage_date = fm.usage_date
LEFT JOIN participant_metrics pm ON udu.user_id = pm.user_id AND udu.usage_date = pm.usage_date
WHERE (mm.meeting_count > 0 OR wm.webinar_count > 0 OR fm.feature_usage_count > 0)
