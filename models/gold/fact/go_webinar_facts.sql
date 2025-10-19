{{ config(
    materialized='table',
    cluster_by=['webinar_date', 'host_id'],
    pre_hook="INSERT INTO {{ ref('audit_log') }} (table_name, operation, timestamp) VALUES ('go_webinar_facts', 'transform_start', CURRENT_TIMESTAMP())",
    post_hook="INSERT INTO {{ ref('audit_log') }} (table_name, operation, timestamp) VALUES ('go_webinar_facts', 'transform_complete', CURRENT_TIMESTAMP())"
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
),

attendee_aggregates AS (
    SELECT 
        meeting_id as webinar_id,
        COUNT(DISTINCT participant_id) as actual_attendees,
        COUNT(DISTINCT participant_id) as max_concurrent_attendees
    FROM {{ ref('si_participants') }}
    WHERE record_status = 'ACTIVE'
        AND data_quality_score >= 0.8
    GROUP BY meeting_id
),

host_context AS (
    SELECT 
        user_id,
        user_name,
        email,
        company
    FROM {{ ref('si_users') }}
    WHERE record_status = 'ACTIVE'
        AND data_quality_score >= 0.8
),

feature_usage AS (
    SELECT 
        meeting_id,
        SUM(CASE WHEN feature_name = 'Q&A' THEN usage_count ELSE 0 END) as qa_questions_count,
        SUM(CASE WHEN feature_name = 'Polling' THEN usage_count ELSE 0 END) as poll_responses_count
    FROM {{ ref('si_feature_usage') }}
    WHERE record_status = 'ACTIVE'
        AND data_quality_score >= 0.8
    GROUP BY meeting_id
),

final AS (
    SELECT 
        -- Primary Keys
        CONCAT('WF_', wb.webinar_id, '_', CURRENT_TIMESTAMP()::STRING) as webinar_fact_id,
        wb.webinar_id,
        wb.host_id,
        
        -- Webinar Details
        TRIM(COALESCE(wb.webinar_topic, 'No Topic Specified')) as webinar_topic,
        CONVERT_TIMEZONE('UTC', wb.start_time) as start_time,
        CONVERT_TIMEZONE('UTC', wb.end_time) as end_time,
        
        -- Time Dimensions
        DATE(wb.start_time) as webinar_date,
        EXTRACT(HOUR FROM wb.start_time) as webinar_hour,
        EXTRACT(DOW FROM wb.start_time) as day_of_week,
        
        -- Duration Metrics
        DATEDIFF('minute', wb.start_time, wb.end_time) as duration_minutes,
        
        -- Registration and Attendance
        COALESCE(wb.registrants, 0) as registrants_count,
        COALESCE(aa.actual_attendees, 0) as actual_attendees,
        COALESCE(aa.max_concurrent_attendees, 0) as max_concurrent_attendees,
        
        -- Calculated Metrics
        CASE 
            WHEN wb.registrants > 0 THEN 
                ROUND((COALESCE(aa.actual_attendees, 0)::FLOAT / wb.registrants) * 100, 2)
            ELSE 0 
        END as attendance_rate,
        
        -- Engagement Metrics
        COALESCE(fu.qa_questions_count, 0) as qa_questions_count,
        COALESCE(fu.poll_responses_count, 0) as poll_responses_count,
        
        ROUND(
            (COALESCE(fu.qa_questions_count, 0) * 0.4 + 
             COALESCE(fu.poll_responses_count, 0) * 0.3 + 
             CASE WHEN wb.registrants > 0 THEN (COALESCE(aa.actual_attendees, 0)::FLOAT / wb.registrants) * 100 ELSE 0 END * 0.3) / 10, 2
        ) as engagement_score,
        
        -- Categorization
        CASE 
            WHEN DATEDIFF('minute', wb.start_time, wb.end_time) > 120 THEN 'Long Form'
            WHEN DATEDIFF('minute', wb.start_time, wb.end_time) > 60 THEN 'Standard'
            ELSE 'Short Form'
        END as event_category,
        
        CASE 
            WHEN COALESCE(wb.registrants, 0) > 1000 THEN 'Large'
            WHEN COALESCE(wb.registrants, 0) > 100 THEN 'Medium'
            ELSE 'Small'
        END as webinar_size_category,
        
        -- Host Context
        hc.user_name as host_name,
        hc.email as host_email,
        COALESCE(hc.company, 'Unknown') as host_organization,
        
        -- Audit Fields
        wb.load_date,
        CURRENT_DATE() as update_date,
        wb.source_system,
        CURRENT_TIMESTAMP() as created_at,
        CURRENT_TIMESTAMP() as updated_at
        
    FROM webinar_base wb
    LEFT JOIN attendee_aggregates aa ON wb.webinar_id = aa.webinar_id
    LEFT JOIN host_context hc ON wb.host_id = hc.user_id
    LEFT JOIN feature_usage fu ON wb.webinar_id = fu.meeting_id
)

SELECT * FROM final
