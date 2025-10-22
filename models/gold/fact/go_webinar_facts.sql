{{ config(
    materialized='table',
    cluster_by=['start_time', 'host_id']
) }}

-- Webinar Facts Transformation
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

attendee_metrics AS (
    SELECT 
        meeting_id AS webinar_id,
        COUNT(DISTINCT participant_id) AS actual_attendees
    FROM {{ source('silver', 'si_participants') }}
    WHERE record_status = 'ACTIVE'
      AND join_time IS NOT NULL
    GROUP BY meeting_id
),

engagement_metrics AS (
    SELECT 
        meeting_id AS webinar_id,
        SUM(CASE WHEN feature_name = 'Q&A' THEN usage_count ELSE 0 END) AS qa_questions_count,
        SUM(CASE WHEN feature_name = 'Polling' THEN usage_count ELSE 0 END) AS poll_responses_count
    FROM {{ source('silver', 'si_feature_usage') }}
    WHERE record_status = 'ACTIVE'
    GROUP BY meeting_id
)

SELECT 
    CONCAT('WF_', wb.webinar_id, '_', CURRENT_TIMESTAMP()::STRING)::VARCHAR(50) AS webinar_fact_id,
    wb.webinar_id::VARCHAR(50) AS webinar_id,
    wb.host_id::VARCHAR(50) AS host_id,
    TRIM(COALESCE(wb.webinar_topic, 'No Topic Specified'))::VARCHAR(500) AS webinar_topic,
    CONVERT_TIMEZONE('UTC', wb.start_time) AS start_time,
    CONVERT_TIMEZONE('UTC', wb.end_time) AS end_time,
    DATEDIFF('minute', wb.start_time, wb.end_time) AS duration_minutes,
    COALESCE(wb.registrants, 0) AS registrants_count,
    COALESCE(am.actual_attendees, 0) AS actual_attendees,
    CASE 
        WHEN wb.registrants > 0 THEN (am.actual_attendees::FLOAT / wb.registrants) * 100 
        ELSE 0 
    END AS attendance_rate,
    COALESCE(am.actual_attendees, 0) AS max_concurrent_attendees,
    COALESCE(em.qa_questions_count, 0) AS qa_questions_count,
    COALESCE(em.poll_responses_count, 0) AS poll_responses_count,
    ROUND(
        (COALESCE(em.qa_questions_count, 0) * 0.4 + 
         COALESCE(em.poll_responses_count, 0) * 0.3 + 
         CASE WHEN wb.registrants > 0 THEN (am.actual_attendees::FLOAT / wb.registrants) * 100 ELSE 0 END * 0.3) / 10, 2
    ) AS engagement_score,
    CASE 
        WHEN DATEDIFF('minute', wb.start_time, wb.end_time) > 120 THEN 'Long Form'
        WHEN DATEDIFF('minute', wb.start_time, wb.end_time) > 60 THEN 'Standard'
        ELSE 'Short Form'
    END::VARCHAR(100) AS event_category,
    wb.load_date,
    CURRENT_DATE() AS update_date,
    wb.source_system
FROM webinar_base wb
LEFT JOIN attendee_metrics am ON wb.webinar_id = am.webinar_id
LEFT JOIN engagement_metrics em ON wb.webinar_id = em.webinar_id
WHERE wb.rn = 1
