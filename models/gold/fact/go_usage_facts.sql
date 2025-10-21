{{ config(
    materialized='incremental',
    unique_key='usage_fact_id',
    on_schema_change='sync_all_columns',
    pre_hook="INSERT INTO {{ ref('go_process_audit') }} (execution_id, pipeline_name, start_time, status, process_type, source_system, target_system, user_executed) VALUES (UUID_STRING(), 'go_usage_facts_load', CURRENT_TIMESTAMP(), 'STARTED', 'FACT_LOAD', 'SILVER', 'GOLD', CURRENT_USER()) WHERE '{{ this.name }}' != 'go_process_audit'",
    post_hook="INSERT INTO {{ ref('go_process_audit') }} (execution_id, pipeline_name, end_time, status, process_type, source_system, target_system, user_executed, records_processed) VALUES (UUID_STRING(), 'go_usage_facts_load', CURRENT_TIMESTAMP(), 'COMPLETED', 'FACT_LOAD', 'SILVER', 'GOLD', CURRENT_USER(), (SELECT COUNT(*) FROM {{ this }})) WHERE '{{ this.name }}' != 'go_process_audit'"
) }}

WITH usage_base AS (
    SELECT 
        usage_date,
        load_date,
        update_date,
        source_system
    FROM {{ ref('si_feature_usage') }}
    WHERE record_status = 'ACTIVE'
    {% if is_incremental() %}
        AND update_date > (SELECT MAX(update_date) FROM {{ this }})
    {% endif %}
    GROUP BY usage_date, load_date, update_date, source_system
),

user_org_mapping AS (
    SELECT 
        user_id,
        COALESCE(company, 'INDIVIDUAL') AS organization_id
    FROM {{ ref('si_users') }}
    WHERE record_status = 'ACTIVE'
),

meeting_metrics AS (
    SELECT 
        host_id AS user_id,
        DATE(start_time) AS usage_date,
        COUNT(DISTINCT meeting_id) AS meeting_count,
        SUM(duration_minutes) AS total_meeting_minutes
    FROM {{ ref('si_meetings') }}
    WHERE record_status = 'ACTIVE'
    GROUP BY host_id, DATE(start_time)
),

webinar_metrics AS (
    SELECT 
        host_id AS user_id,
        DATE(start_time) AS usage_date,
        COUNT(DISTINCT webinar_id) AS webinar_count,
        SUM(DATEDIFF('minute', start_time, end_time)) AS total_webinar_minutes
    FROM {{ ref('si_webinars') }}
    WHERE record_status = 'ACTIVE'
    GROUP BY host_id, DATE(start_time)
),

feature_metrics AS (
    SELECT 
        usage_date,
        SUM(CASE WHEN feature_name = 'Recording' THEN usage_count * 0.1 ELSE 0 END) AS recording_storage_gb,
        SUM(usage_count) AS feature_usage_count
    FROM {{ ref('si_feature_usage') }}
    WHERE record_status = 'ACTIVE'
    GROUP BY usage_date
),

participant_metrics AS (
    SELECT 
        DATE(join_time) AS usage_date,
        COUNT(DISTINCT user_id) AS unique_participants_hosted
    FROM {{ ref('si_participants') }}
    WHERE record_status = 'ACTIVE'
    GROUP BY DATE(join_time)
),

user_dates AS (
    SELECT DISTINCT 
        u.user_id,
        ub.usage_date,
        u.organization_id
    FROM user_org_mapping u
    CROSS JOIN usage_base ub
),

final_transform AS (
    SELECT 
        CONCAT('UF_', ud.user_id, '_', ud.usage_date::STRING) AS usage_fact_id,
        ud.user_id,
        ud.organization_id,
        ud.usage_date,
        COALESCE(mm.meeting_count, 0) AS meeting_count,
        COALESCE(mm.total_meeting_minutes, 0) AS total_meeting_minutes,
        COALESCE(wm.webinar_count, 0) AS webinar_count,
        COALESCE(wm.total_webinar_minutes, 0) AS total_webinar_minutes,
        COALESCE(fm.recording_storage_gb, 0) AS recording_storage_gb,
        COALESCE(fm.feature_usage_count, 0) AS feature_usage_count,
        COALESCE(pm.unique_participants_hosted, 0) AS unique_participants_hosted,
        ub.load_date,
        CURRENT_DATE() AS update_date,
        ub.source_system
    FROM user_dates ud
    LEFT JOIN usage_base ub ON ud.usage_date = ub.usage_date
    LEFT JOIN meeting_metrics mm ON ud.user_id = mm.user_id AND ud.usage_date = mm.usage_date
    LEFT JOIN webinar_metrics wm ON ud.user_id = wm.user_id AND ud.usage_date = wm.usage_date
    LEFT JOIN feature_metrics fm ON ud.usage_date = fm.usage_date
    LEFT JOIN participant_metrics pm ON ud.usage_date = pm.usage_date
    WHERE (mm.meeting_count > 0 OR wm.webinar_count > 0 OR fm.feature_usage_count > 0)
)

SELECT * FROM final_transform
