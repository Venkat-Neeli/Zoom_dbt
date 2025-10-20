{{ config(
    materialized='table',
    schema='gold',
    cluster_by=['load_date'],
    pre_hook="ALTER SESSION SET TIMEZONE = 'UTC'",
    post_hook=[
        "ALTER TABLE {{ this }} SET CHANGE_TRACKING = TRUE",
        "GRANT SELECT ON {{ this }} TO ROLE ANALYTICS_READER"
    ]
) }}

WITH usage_base AS (
    SELECT 
        usage_id,
        meeting_id,
        feature_name,
        usage_count,
        usage_date,
        load_timestamp,
        update_timestamp,
        source_system,
        load_date,
        update_date,
        data_quality_score,
        record_status
    FROM {{ ref('si_feature_usage') }}
    WHERE record_status = 'ACTIVE'
        AND data_quality_score >= {{ var('min_quality_score', 3.0) }}
        AND usage_count > 0
),

meeting_info AS (
    SELECT 
        meeting_id,
        host_id,
        meeting_topic,
        start_time,
        duration_minutes
    FROM {{ ref('si_meetings') }}
    WHERE record_status = 'ACTIVE'
),

user_info AS (
    SELECT 
        user_id,
        user_name,
        company,
        plan_type
    FROM {{ ref('si_users') }}
    WHERE record_status = 'ACTIVE'
),

usage_metrics AS (
    SELECT 
        ub.*,
        CASE 
            WHEN ub.feature_name IN ('screen_share', 'recording', 'chat') THEN 'Core'
            WHEN ub.feature_name IN ('whiteboard', 'breakout_rooms', 'polls') THEN 'Collaboration'
            WHEN ub.feature_name IN ('virtual_background', 'filters', 'reactions') THEN 'Enhancement'
            ELSE 'Other'
        END as feature_category,
        CASE 
            WHEN ub.usage_count >= 10 THEN 'Heavy'
            WHEN ub.usage_count >= 5 THEN 'Moderate'
            WHEN ub.usage_count >= 1 THEN 'Light'
            ELSE 'None'
        END as usage_intensity,
        EXTRACT(HOUR FROM ub.usage_date) as usage_hour,
        DAYNAME(ub.usage_date) as usage_day_of_week
    FROM usage_base ub
)

SELECT 
    UUID_STRING() as usage_fact_key,
    um.usage_id,
    um.meeting_id,
    mi.host_id,
    ui.user_name as host_name,
    ui.company as host_company,
    ui.plan_type as host_plan_type,
    mi.meeting_topic,
    um.feature_name,
    um.feature_category,
    um.usage_count,
    um.usage_intensity,
    um.usage_date,
    um.usage_hour,
    um.usage_day_of_week,
    mi.duration_minutes as meeting_duration_minutes,
    CASE 
        WHEN mi.duration_minutes > 0 THEN (um.usage_count::FLOAT / mi.duration_minutes) * 60
        ELSE 0
    END as usage_per_hour_rate,
    um.data_quality_score,
    um.source_system,
    um.load_date,
    um.update_date,
    CURRENT_TIMESTAMP() as created_at,
    CURRENT_TIMESTAMP() as updated_at,
    'SUCCESS' as process_status
FROM usage_metrics um
LEFT JOIN meeting_info mi ON um.meeting_id = mi.meeting_id
LEFT JOIN user_info ui ON mi.host_id = ui.user_id
