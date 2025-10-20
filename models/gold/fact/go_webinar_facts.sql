{{ config(
    materialized='incremental',
    unique_key='webinar_fact_key',
    on_schema_change='sync_all_columns',
    pre_hook="INSERT INTO {{ ref('go_process_audit') }} (process_name, start_time, status) VALUES ('go_webinar_facts', CURRENT_TIMESTAMP(), 'STARTED')",
    post_hook="INSERT INTO {{ ref('go_process_audit') }} (process_name, end_time, status) VALUES ('go_webinar_facts', CURRENT_TIMESTAMP(), 'COMPLETED')"
) }}

WITH webinar_base AS (
    SELECT 
        webinar_id,
        host_id,
        webinar_topic,
        CONVERT_TIMEZONE('UTC', start_time) AS start_time_utc,
        CONVERT_TIMEZONE('UTC', end_time) AS end_time_utc,
        registrants,
        load_date,
        update_date,
        source_system
    FROM {{ ref('si_webinars') }}
    WHERE record_status = 'ACTIVE'
        AND data_quality_score >= 0.7
        {% if is_incremental() %}
        AND update_date > (SELECT MAX(update_date) FROM {{ this }})
        {% endif %}
),

host_info AS (
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
    UUID_STRING() AS webinar_fact_key,
    wb.webinar_id,
    wb.host_id,
    hi.user_name AS host_name,
    hi.company AS host_company,
    hi.plan_type AS host_plan_type,
    wb.webinar_topic,
    wb.start_time_utc,
    wb.end_time_utc,
    COALESCE(DATEDIFF('minute', wb.start_time_utc, wb.end_time_utc), 0) AS duration_minutes,
    COALESCE(wb.registrants, 0) AS total_registrants,
    CASE 
        WHEN DATEDIFF('minute', wb.start_time_utc, wb.end_time_utc) > 120 THEN 'Long'
        WHEN DATEDIFF('minute', wb.start_time_utc, wb.end_time_utc) > 60 THEN 'Medium'
        ELSE 'Short'
    END AS webinar_duration_category,
    CASE 
        WHEN wb.registrants > 1000 THEN 'Large'
        WHEN wb.registrants > 100 THEN 'Medium'
        ELSE 'Small'
    END AS webinar_size_category,
    EXTRACT(HOUR FROM wb.start_time_utc) AS start_hour,
    DAYNAME(wb.start_time_utc) AS start_day_of_week,
    wb.load_date,
    wb.update_date,
    wb.source_system,
    CURRENT_TIMESTAMP() AS created_at
FROM webinar_base wb
LEFT JOIN host_info hi ON wb.host_id = hi.user_id
