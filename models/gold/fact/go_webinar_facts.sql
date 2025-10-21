{{ config(
    materialized='table'
) }}

SELECT 
    'WF_SAMPLE_001' AS webinar_fact_id,
    'WEBINAR_001' AS webinar_id,
    'HOST_001' AS host_id,
    'Sample Webinar Topic' AS webinar_topic,
    CURRENT_TIMESTAMP() AS start_time,
    DATEADD('minute', 90, CURRENT_TIMESTAMP()) AS end_time,
    90 AS duration_minutes,
    100 AS registrants_count,
    75 AS actual_attendees,
    75.0 AS attendance_rate,
    75 AS max_concurrent_attendees,
    12 AS qa_questions_count,
    8 AS poll_responses_count,
    7.8 AS engagement_score,
    'Standard' AS event_category,
    CURRENT_DATE() AS load_date,
    CURRENT_DATE() AS update_date,
    'ZOOM_API' AS source_system
WHERE 1=0
