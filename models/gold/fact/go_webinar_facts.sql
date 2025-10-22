{{ config(
    materialized='table'
) }}

SELECT 
    CONCAT('WF_', webinar_id) AS webinar_fact_id,
    webinar_id,
    host_id,
    COALESCE(webinar_topic, 'No Topic') AS webinar_topic,
    start_time,
    end_time,
    DATEDIFF('minute', start_time, end_time) AS duration_minutes,
    COALESCE(registrants, 0) AS registrants_count,
    0 AS actual_attendees,
    0.0 AS attendance_rate,
    0 AS max_concurrent_attendees,
    0 AS qa_questions_count,
    0 AS poll_responses_count,
    0.0 AS engagement_score,
    'Standard' AS event_category,
    load_date,
    CURRENT_DATE() AS update_date,
    source_system
FROM {{ ref('si_webinars') }}
WHERE record_status = 'ACTIVE'
