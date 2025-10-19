{{ config(
    materialized='table',
    cluster_by=['usage_date', 'organization_id'],
    pre_hook="INSERT INTO {{ ref('go_process_audit') }} (execution_id, pipeline_name, start_time, status, source_system, target_system, process_type) VALUES (UUID_STRING(), 'go_usage_facts', CURRENT_TIMESTAMP(), 'STARTED', 'SILVER', 'GOLD', 'FACT_LOAD')",
    post_hook="INSERT INTO {{ ref('go_process_audit') }} (execution_id, pipeline_name, end_time, status, records_processed, source_system, target_system, process_type) VALUES (UUID_STRING(), 'go_usage_facts', CURRENT_TIMESTAMP(), 'COMPLETED', (SELECT COUNT(*) FROM {{ this }}), 'SILVER', 'GOLD', 'FACT_LOAD')"
) }}

WITH user_organization AS (
    SELECT 
        u.user_id,
        COALESCE(u.company, 'INDIVIDUAL') AS organization_id
    FROM {{ ref('si_users') }} u
    WHERE u.record_status = 'ACTIVE'
),

daily_meeting_usage AS (
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

daily_webinar_usage AS (
    SELECT 
        w.host_id AS user_id,
        DATE(w.start_time) AS usage_date,
        COUNT(DISTINCT w.webinar_id) AS webinar_count,
        SUM(DATEDIFF('minute', w.start_time, w.end_time)) AS total_webinar_minutes
    FROM {{ ref('si_webinars') }} w
    WHERE w.record_status = 'ACTIVE'
    GROUP BY w.host_id, DATE(w.start_time)
),

daily_feature_usage AS (
    SELECT 
        fu.usage_date,
        m.host_id AS user_id,
        SUM(fu.usage_count) AS feature_usage_count,
        SUM(CASE WHEN fu.feature_name = 'Recording' THEN fu.usage_count * 0.1 ELSE 0 END) AS recording_storage_gb
    FROM {{ ref('si_feature_usage') }} fu
    INNER JOIN {{ ref('si_meetings') }} m ON fu.meeting_id = m.meeting_id
    WHERE fu.record_status = 'ACTIVE' AND m.record_status = 'ACTIVE'
    GROUP BY fu.usage_date, m.host_id
),

unique_participants_hosted AS (
    SELECT 
        m.host_id AS user_id,
        DATE(m.start_time) AS usage_date,
        COUNT(DISTINCT p.user_id) AS unique_participants_hosted
    FROM {{ ref('si_meetings') }} m
    INNER JOIN {{ ref('si_participants') }} p ON m.meeting_id = p.meeting_id
    WHERE m.record_status = 'ACTIVE' AND p.record_status = 'ACTIVE'
    GROUP BY m.host_id, DATE(m.start_time)
),

final_usage_facts AS (
    SELECT 
        CONCAT('UF_', dmu.user_id, '_', dmu.usage_date::STRING) AS usage_fact_id,
        dmu.user_id,
        uo.organization_id,
        dmu.usage_date,
        COALESCE(dmu.meeting_count, 0) AS meeting_count,
        COALESCE(dmu.total_meeting_minutes, 0) AS total_meeting_minutes,
        COALESCE(dwu.webinar_count, 0) AS webinar_count,
        COALESCE(dwu.total_webinar_minutes, 0) AS total_webinar_minutes,
        COALESCE(dfu.recording_storage_gb, 0) AS recording_storage_gb,
        COALESCE(dfu.feature_usage_count, 0) AS feature_usage_count,
        COALESCE(uph.unique_participants_hosted, 0) AS unique_participants_hosted,
        dmu.load_date,
        CURRENT_DATE() AS update_date,
        dmu.source_system,
        CURRENT_TIMESTAMP() AS created_at,
        CURRENT_TIMESTAMP() AS updated_at,
        'ACTIVE' AS process_status
    FROM daily_meeting_usage dmu
    LEFT JOIN user_organization uo ON dmu.user_id = uo.user_id
    LEFT JOIN daily_webinar_usage dwu ON dmu.user_id = dwu.user_id AND dmu.usage_date = dwu.usage_date
    LEFT JOIN daily_feature_usage dfu ON dmu.user_id = dfu.user_id AND dmu.usage_date = dfu.usage_date
    LEFT JOIN unique_participants_hosted uph ON dmu.user_id = uph.user_id AND dmu.usage_date = uph.usage_date
)

SELECT * FROM final_usage_facts
