{{ config(
    materialized='table'
) }}

WITH webinar_base AS (
    SELECT 
        webinar_id,
        host_id,
        webinar_topic,
        start_time,
        end_time,
        registrants,
        load_date,
        source_system,
        ROW_NUMBER() OVER (PARTITION BY webinar_id ORDER BY update_timestamp DESC) as rn
    FROM {{ source('silver', 'si_webinars') }}
    WHERE record_status = 'ACTIVE'
),

webinar_attendees AS (
    SELECT 
        meeting_id as webinar_id,
        COUNT(DISTINCT participant_id) as actual_attendees
    FROM {{ source('silver', 'si_participants') }}
    WHERE record_status = 'ACTIVE'
    GROUP BY meeting_id
),

webinar_engagement AS (
    SELECT 
        meeting_id as webinar_id,
        SUM(CASE WHEN feature_name = 'Q&A' THEN usage_count ELSE 0 END) as qa_questions_count,
        SUM(CASE WHEN feature_name = 'Polling' THEN usage_count ELSE 0 END) as poll_responses_count
    FROM {{ source('silver', 'si_feature_usage') }}
    WHERE record_status = 'ACTIVE'
    GROUP BY meeting_id
),

final_webinar_facts AS (
    SELECT 
        CONCAT('WF_', wb.webinar_id, '_', CURRENT_TIMESTAMP()::STRING) as webinar_fact_id,
        wb.webinar_id,
        wb.host_id,
        TRIM(COALESCE(wb.webinar_topic, 'No Topic Specified')) as webinar_topic,
        CONVERT_TIMEZONE('UTC', wb.start_time) as start_time,
        CONVERT_TIMEZONE('UTC', wb.end_time) as end_time,
        DATEDIFF('minute', wb.start_time, wb.end_time) as duration_minutes,
        COALESCE(wb.registrants, 0) as registrants_count,
        COALESCE(wa.actual_attendees, 0) as actual_attendees,
        CASE 
            WHEN wb.registrants > 0 THEN (wa.actual_attendees::FLOAT / wb.registrants) * 100 
            ELSE 0 
        END as attendance_rate,
        COALESCE(wa.actual_attendees, 0) as max_concurrent_attendees,
        COALESCE(we.qa_questions_count, 0) as qa_questions_count,
        COALESCE(we.poll_responses_count, 0) as poll_responses_count,
        ROUND(
            (COALESCE(we.qa_questions_count, 0) * 0.4 + 
             COALESCE(we.poll_responses_count, 0) * 0.3 + 
             CASE WHEN wb.registrants > 0 THEN (wa.actual_attendees::FLOAT / wb.registrants) * 100 ELSE 0 END * 0.3) / 10, 2
        ) as engagement_score,
        CASE 
            WHEN DATEDIFF('minute', wb.start_time, wb.end_time) > 120 THEN 'Long Form'
            WHEN DATEDIFF('minute', wb.start_time, wb.end_time) > 60 THEN 'Standard'
            ELSE 'Short Form'
        END as event_category,
        wb.load_date,
        CURRENT_DATE() as update_date,
        wb.source_system
    FROM webinar_base wb
    LEFT JOIN webinar_attendees wa ON wb.webinar_id = wa.webinar_id
    LEFT JOIN webinar_engagement we ON wb.webinar_id = we.webinar_id
    WHERE wb.rn = 1
)

SELECT * FROM final_webinar_facts
