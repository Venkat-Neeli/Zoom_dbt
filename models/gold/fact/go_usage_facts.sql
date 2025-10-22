{{ config(
    materialized='incremental',
    unique_key='usage_fact_id',
    on_schema_change='sync_all_columns',
    pre_hook="INSERT INTO {{ ref('go_process_audit') }} (execution_id, pipeline_name, start_time, status, process_type, user_executed, source_system, target_system) VALUES (UUID_STRING(), 'go_usage_facts_load', CURRENT_TIMESTAMP(), 'STARTED', 'FACT_LOAD', 'DBT_USER', 'SILVER', 'GOLD') WHERE '{{ this.name }}' != 'go_process_audit'",
    post_hook="UPDATE {{ ref('go_process_audit') }} SET end_time = CURRENT_TIMESTAMP(), status = 'COMPLETED', records_processed = (SELECT COUNT(*) FROM {{ this }}) WHERE pipeline_name = 'go_usage_facts_load' AND status = 'STARTED' AND '{{ this.name }}' != 'go_process_audit'"
) }}

WITH user_base AS (
    SELECT 
        user_id,
        company AS organization_id,
        load_date,
        update_date,
        source_system,
        record_status
    FROM {{ ref('si_users') }}
    WHERE record_status = 'ACTIVE'
        {% if is_incremental() %}
        AND update_date >= (SELECT MAX(update_date) FROM {{ this }})
        {% endif %}
),

feature_usage_daily AS (
    SELECT 
        usage_date,
        load_date,
        update_date,
        source_system,
        SUM(usage_count) AS feature_usage_count
    FROM {{ ref('si_feature_usage') }}
    WHERE record_status = 'ACTIVE'
    GROUP BY usage_date, load_date, update_date, source_system
),

meeting_stats AS (
    SELECT 
        host_id AS user_id,
        DATE(start_time) AS usage_date,
        COUNT(DISTINCT meeting_id) AS meeting_count,
        SUM(duration_minutes) AS total_meeting_minutes
    FROM {{ ref('si_meetings') }}
    WHERE record_status = 'ACTIVE'
    GROUP BY host_id, DATE(start_time)
),

webinar_stats AS (
    SELECT 
        host_id AS user_id,
        DATE(start_time) AS usage_date,
        COUNT(DISTINCT webinar_id) AS webinar_count,
        SUM(DATEDIFF('minute', start_time, end_time)) AS total_webinar_minutes
    FROM {{ ref('si_webinars') }}
    WHERE record_status = 'ACTIVE'
    GROUP BY host_id, DATE(start_time)
),

recording_storage AS (
    SELECT 
        usage_date,
        SUM(usage_count) * 0.1 AS recording_storage_gb
    FROM {{ ref('si_feature_usage') }}
    WHERE feature_name = 'Recording'
        AND record_status = 'ACTIVE'
    GROUP BY usage_date
),

participant_stats AS (
    SELECT 
        mi.host_id AS user_id,
        DATE(p.join_time) AS usage_date,
        COUNT(DISTINCT p.user_id) AS unique_participants_hosted
    FROM {{ ref('si_participants') }} p
    JOIN {{ ref('si_meetings') }} mi ON p.meeting_id = mi.meeting_id
    WHERE p.record_status = 'ACTIVE'
        AND mi.record_status = 'ACTIVE'
    GROUP BY mi.host_id, DATE(p.join_time)
),

final_facts AS (
    SELECT 
        CONCAT('UF_', ub.user_id, '_', fud.usage_date::STRING) AS usage_fact_id,
        ub.user_id,
        COALESCE(ub.organization_id, 'INDIVIDUAL') AS organization_id,
        fud.usage_date,
        COALESCE(ms.meeting_count, 0) AS meeting_count,
        COALESCE(ms.total_meeting_minutes, 0) AS total_meeting_minutes,
        COALESCE(ws.webinar_count, 0) AS webinar_count,
        COALESCE(ws.total_webinar_minutes, 0) AS total_webinar_minutes,
        COALESCE(rs.recording_storage_gb, 0) AS recording_storage_gb,
        COALESCE(fud.feature_usage_count, 0) AS feature_usage_count,
        COALESCE(ps.unique_participants_hosted, 0) AS unique_participants_hosted,
        COALESCE(fud.load_date, ub.load_date) AS load_date,
        CURRENT_DATE() AS update_date,
        COALESCE(fud.source_system, ub.source_system) AS source_system
    FROM user_base ub
    CROSS JOIN feature_usage_daily fud
    LEFT JOIN meeting_stats ms ON ub.user_id = ms.user_id AND fud.usage_date = ms.usage_date
    LEFT JOIN webinar_stats ws ON ub.user_id = ws.user_id AND fud.usage_date = ws.usage_date
    LEFT JOIN recording_storage rs ON fud.usage_date = rs.usage_date
    LEFT JOIN participant_stats ps ON ub.user_id = ps.user_id AND fud.usage_date = ps.usage_date
)

SELECT * FROM final_facts
