{{ config(
    materialized='incremental',
    on_schema_change='sync_all_columns',
    unique_key='usage_fact_id'
) }}

WITH user_base AS (
    SELECT 
        user_id,
        company AS organization_id,
        load_date,
        update_date,
        source_system
    FROM {{ ref('si_users') }}
    WHERE record_status = 'ACTIVE'
),

usage_dates AS (
    SELECT DISTINCT usage_date
    FROM {{ ref('si_feature_usage') }}
    WHERE record_status = 'ACTIVE'
    {% if is_incremental() %}
        AND update_date > (SELECT COALESCE(MAX(update_date), '1900-01-01') FROM {{ this }})
    {% endif %}
),

meeting_usage AS (
    SELECT 
        m.host_id AS user_id,
        DATE(m.start_time) AS usage_date,
        COUNT(DISTINCT m.meeting_id) AS meeting_count,
        SUM(COALESCE(m.duration_minutes, 0)) AS total_meeting_minutes
    FROM {{ ref('si_meetings') }} m
    WHERE m.record_status = 'ACTIVE'
    GROUP BY m.host_id, DATE(m.start_time)
),

webinar_usage AS (
    SELECT 
        w.host_id AS user_id,
        DATE(w.start_time) AS usage_date,
        COUNT(DISTINCT w.webinar_id) AS webinar_count,
        SUM(CASE WHEN w.end_time IS NOT NULL THEN DATEDIFF('minute', w.start_time, w.end_time) ELSE 0 END) AS total_webinar_minutes
    FROM {{ ref('si_webinars') }} w
    WHERE w.record_status = 'ACTIVE'
    GROUP BY w.host_id, DATE(w.start_time)
),

feature_usage_agg AS (
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
        CONCAT('UF_', ub.user_id, '_', ud.usage_date::STRING) AS usage_fact_id,
        ub.user_id,
        COALESCE(ub.organization_id, 'INDIVIDUAL') AS organization_id,
        ud.usage_date,
        COALESCE(mu.meeting_count, 0) AS meeting_count,
        COALESCE(mu.total_meeting_minutes, 0) AS total_meeting_minutes,
        COALESCE(wu.webinar_count, 0) AS webinar_count,
        COALESCE(wu.total_webinar_minutes, 0) AS total_webinar_minutes,
        COALESCE(fua.recording_storage_gb, 0) AS recording_storage_gb,
        COALESCE(fua.feature_usage_count, 0) AS feature_usage_count,
        COALESCE(pi.unique_participants_hosted, 0) AS unique_participants_hosted,
        ub.load_date,
        CURRENT_DATE() AS update_date,
        ub.source_system
    FROM user_base ub
    CROSS JOIN usage_dates ud
    LEFT JOIN meeting_usage mu ON ub.user_id = mu.user_id AND ud.usage_date = mu.usage_date
    LEFT JOIN webinar_usage wu ON ub.user_id = wu.user_id AND ud.usage_date = wu.usage_date
    LEFT JOIN feature_usage_agg fua ON ud.usage_date = fua.usage_date
    LEFT JOIN participant_interactions pi ON ub.user_id = pi.user_id AND ud.usage_date = pi.usage_date
    WHERE (COALESCE(mu.meeting_count, 0) > 0 OR COALESCE(wu.webinar_count, 0) > 0 OR COALESCE(fua.feature_usage_count, 0) > 0)
)

SELECT * FROM final_usage_facts
