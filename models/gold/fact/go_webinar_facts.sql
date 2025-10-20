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

WITH webinar_base AS (
    SELECT 
        webinar_id,
        host_id,
        webinar_topic,
        start_time,
        end_time,
        registrants,
        load_timestamp,
        update_timestamp,
        source_system,
        load_date,
        update_date,
        data_quality_score,
        record_status
    FROM {{ ref('si_webinars') }}
    WHERE record_status = 'ACTIVE'
        AND data_quality_score >= {{ var('min_quality_score', 3.0) }}
),

host_info AS (
    SELECT 
        user_id,
        user_name,
        company,
        plan_type
    FROM {{ ref('si_users') }}
    WHERE record_status = 'ACTIVE'
),

webinar_metrics AS (
    SELECT 
        wb.*,
        DATEDIFF('minute', wb.start_time, wb.end_time) as duration_minutes,
        CASE 
            WHEN wb.registrants >= 100 THEN 'Large'
            WHEN wb.registrants >= 50 THEN 'Medium'
            WHEN wb.registrants >= 10 THEN 'Small'
            ELSE 'Micro'
        END as webinar_size_category
    FROM webinar_base wb
)

SELECT 
    UUID_STRING() as webinar_fact_key,
    wm.webinar_id,
    wm.host_id,
    hi.user_name as host_name,
    hi.company as host_company,
    hi.plan_type as host_plan_type,
    wm.webinar_topic,
    wm.start_time,
    wm.end_time,
    wm.duration_minutes,
    wm.registrants,
    wm.webinar_size_category,
    CASE 
        WHEN wm.duration_minutes >= 120 THEN 'Long'
        WHEN wm.duration_minutes >= 60 THEN 'Medium'
        ELSE 'Short'
    END as duration_category,
    EXTRACT(HOUR FROM wm.start_time) as start_hour,
    DAYNAME(wm.start_time) as start_day_of_week,
    wm.data_quality_score,
    wm.source_system,
    wm.load_date,
    wm.update_date,
    CURRENT_TIMESTAMP() as created_at,
    CURRENT_TIMESTAMP() as updated_at,
    'SUCCESS' as process_status
FROM webinar_metrics wm
LEFT JOIN host_info hi ON wm.host_id = hi.user_id
