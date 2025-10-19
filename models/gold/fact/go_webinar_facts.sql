{{ config(
    materialized='table',
    cluster_by=['load_date'],
    pre_hook="INSERT INTO {{ ref('audit_log') }} (table_name, operation, start_time, status) VALUES ('go_webinar_facts', 'transform_start', CURRENT_TIMESTAMP(), 'RUNNING')",
    post_hook="INSERT INTO {{ ref('audit_log') }} (table_name, operation, end_time, status) VALUES ('go_webinar_facts', 'transform_end', CURRENT_TIMESTAMP(), 'SUCCESS')"
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
        AND data_quality_score >= 0.8
        AND webinar_id IS NOT NULL
        AND host_id IS NOT NULL
),

host_context AS (
    SELECT 
        user_id,
        user_name,
        email,
        company,
        plan_type
    FROM {{ ref('si_users') }}
    WHERE record_status = 'ACTIVE'
)

SELECT 
    -- Primary Keys
    wb.webinar_id,
    wb.host_id,
    
    -- Webinar Details
    wb.webinar_topic,
    wb.start_time,
    wb.end_time,
    wb.registrants,
    
    -- Host Context
    hc.user_name as host_name,
    hc.email as host_email,
    hc.company as host_company,
    hc.plan_type as host_plan_type,
    
    -- Calculated Metrics
    DATEDIFF('minute', wb.start_time, wb.end_time) as webinar_duration_minutes,
    DATEDIFF('hour', wb.start_time, wb.end_time) as webinar_duration_hours,
    
    -- Webinar Classification
    CASE 
        WHEN DATEDIFF('minute', wb.start_time, wb.end_time) <= 30 THEN 'SHORT'
        WHEN DATEDIFF('minute', wb.start_time, wb.end_time) <= 60 THEN 'MEDIUM'
        WHEN DATEDIFF('minute', wb.start_time, wb.end_time) <= 120 THEN 'LONG'
        ELSE 'EXTENDED'
    END as webinar_duration_category,
    
    CASE 
        WHEN wb.registrants <= 50 THEN 'SMALL'
        WHEN wb.registrants <= 200 THEN 'MEDIUM'
        WHEN wb.registrants <= 500 THEN 'LARGE'
        ELSE 'ENTERPRISE'
    END as webinar_size_category,
    
    -- Time Dimensions
    DATE(wb.start_time) as webinar_date,
    EXTRACT(HOUR FROM wb.start_time) as webinar_hour,
    DAYOFWEEK(wb.start_time) as webinar_day_of_week,
    EXTRACT(MONTH FROM wb.start_time) as webinar_month,
    EXTRACT(YEAR FROM wb.start_time) as webinar_year,
    
    -- Quality and Audit Fields
    wb.data_quality_score,
    wb.source_system,
    wb.load_date,
    wb.update_date,
    CURRENT_TIMESTAMP() as created_at,
    CURRENT_TIMESTAMP() as updated_at
    
FROM webinar_base wb
LEFT JOIN host_context hc ON wb.host_id = hc.user_id
