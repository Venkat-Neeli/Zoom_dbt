{{ config(
    materialized='incremental',
    unique_key='usage_fact_key',
    on_schema_change='sync_all_columns',
    pre_hook="INSERT INTO {{ ref('go_process_audit') }} (process_name, start_time, status) VALUES ('go_usage_facts', CURRENT_TIMESTAMP(), 'STARTED')",
    post_hook="INSERT INTO {{ ref('go_process_audit') }} (process_name, end_time, status) VALUES ('go_usage_facts', CURRENT_TIMESTAMP(), 'COMPLETED')"
) }}

WITH usage_base AS (
    SELECT 
        usage_id,
        meeting_id,
        feature_name,
        usage_count,
        CONVERT_TIMEZONE('UTC', usage_date) AS usage_date_utc,
        load_date,
        update_date,
        source_system
    FROM {{ ref('si_feature_usage') }}
    WHERE record_status = 'ACTIVE'
        AND data_quality_score >= 0.7
        {% if is_incremental() %}
        AND update_date > (SELECT MAX(update_date) FROM {{ this }})
        {% endif %}
),

meeting_info AS (
    SELECT 
        meeting_id,
        host_id,
        meeting_topic,
        duration_minutes
    FROM {{ ref('si_meetings') }}
    WHERE record_status = 'ACTIVE'
        AND data_quality_score >= 0.7
),

user_info AS (
    SELECT 
        user_id,
        user_name,
        company,
        plan_type
    FROM {{ ref('si_users') }}
    WHERE record_status = 'ACTIVE'
        AND data_quality_score >= 0.7
)

SELECT 
    UUID_STRING() AS usage_fact_key,
    ub.usage_id,
    ub.meeting_id,
    mi.host_id,
    ui.user_name AS host_name,
    ui.company AS host_company,
    ui.plan_type AS host_plan_type,
    mi.meeting_topic,
    ub.feature_name,
    COALESCE(ub.usage_count, 0) AS usage_count,
    ub.usage_date_utc,
    COALESCE(mi.duration_minutes, 0) AS meeting_duration_minutes,
    CASE 
        WHEN ub.feature_name IN ('screen_share', 'recording', 'breakout_rooms') THEN 'Core'
        WHEN ub.feature_name IN ('whiteboard', 'annotation', 'polling') THEN 'Interactive'
        WHEN ub.feature_name IN ('chat', 'reactions', 'hand_raise') THEN 'Communication'
        ELSE 'Other'
    END AS feature_category,
    CASE 
        WHEN ub.usage_count > 10 THEN 'High Usage'
        WHEN ub.usage_count > 3 THEN 'Medium Usage'
        ELSE 'Low Usage'
    END AS usage_intensity,
    EXTRACT(YEAR FROM ub.usage_date_utc) AS usage_year,
    EXTRACT(MONTH FROM ub.usage_date_utc) AS usage_month,
    EXTRACT(QUARTER FROM ub.usage_date_utc) AS usage_quarter,
    DAYNAME(ub.usage_date_utc) AS usage_day_of_week,
    ub.load_date,
    ub.update_date,
    ub.source_system,
    CURRENT_TIMESTAMP() AS created_at
FROM usage_base ub
LEFT JOIN meeting_info mi ON ub.meeting_id = mi.meeting_id
LEFT JOIN user_info ui ON mi.host_id = ui.user_id
