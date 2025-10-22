{{ config(
    materialized='table',
    cluster_by=['usage_date', 'user_id']
) }}

-- Gold Usage Facts
WITH user_base AS (
    SELECT 
        user_id,
        company,
        load_date,
        source_system
    FROM {{ source('silver', 'si_users') }}
    WHERE record_status = 'ACTIVE'
),

meeting_usage AS (
    SELECT 
        host_id AS user_id,
        DATE(start_time) AS usage_date,
        COUNT(DISTINCT meeting_id) AS meeting_count,
        SUM(duration_minutes) AS total_meeting_minutes
    FROM {{ source('silver', 'si_meetings') }}
    WHERE record_status = 'ACTIVE'
    AND host_id IS NOT NULL
    GROUP BY host_id, DATE(start_time)
),

webinar_usage AS (
    SELECT 
        host_id AS user_id,
        DATE(start_time) AS usage_date,
        COUNT(DISTINCT webinar_id) AS webinar_count,
        SUM(DATEDIFF('minute', start_time, end_time)) AS total_webinar_minutes
    FROM {{ source('silver', 'si_webinars') }}
    WHERE record_status = 'ACTIVE'
    AND host_id IS NOT NULL
    GROUP BY host_id, DATE(start_time)
),

participant_interactions AS (
    SELECT 
        m.host_id AS user_id,
        DATE(p.join_time) AS usage_date,
        COUNT(DISTINCT p.user_id) AS unique_participants_hosted
    FROM {{ source('silver', 'si_participants') }} p
    JOIN {{ source('silver', 'si_meetings') }} m ON p.meeting_id = m.meeting_id
    WHERE p.record_status = 'ACTIVE'
    AND m.record_status = 'ACTIVE'
    GROUP BY m.host_id, DATE(p.join_time)
),

usage_facts AS (
    SELECT 
        CONCAT('UF_', u.user_id, '_', COALESCE(m.usage_date, w.usage_date, CURRENT_DATE())::STRING) AS usage_fact_id,
        u.user_id,
        COALESCE(u.company, 'INDIVIDUAL') AS organization_id,
        COALESCE(m.usage_date, w.usage_date, CURRENT_DATE()) AS usage_date,
        COALESCE(m.meeting_count, 0) AS meeting_count,
        COALESCE(m.total_meeting_minutes, 0) AS total_meeting_minutes,
        COALESCE(w.webinar_count, 0) AS webinar_count,
        COALESCE(w.total_webinar_minutes, 0) AS total_webinar_minutes,
        0.0 AS recording_storage_gb,
        0 AS feature_usage_count,
        COALESCE(pi.unique_participants_hosted, 0) AS unique_participants_hosted,
        u.load_date,
        CURRENT_DATE() AS update_date,
        u.source_system
    FROM user_base u
    LEFT JOIN meeting_usage m ON u.user_id = m.user_id
    LEFT JOIN webinar_usage w ON u.user_id = w.user_id AND m.usage_date = w.usage_date
    LEFT JOIN participant_interactions pi ON u.user_id = pi.user_id AND COALESCE(m.usage_date, w.usage_date) = pi.usage_date
    WHERE COALESCE(m.usage_date, w.usage_date) IS NOT NULL
)

SELECT * FROM usage_facts
