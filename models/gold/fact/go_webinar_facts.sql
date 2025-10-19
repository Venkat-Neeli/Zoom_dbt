{{ config(
    materialized='table',
    cluster_by=['start_time', 'host_id'],
    pre_hook="INSERT INTO {{ ref('go_process_audit') }} (execution_id, pipeline_name, start_time, status, source_system, target_system, process_type) VALUES (UUID_STRING(), 'go_webinar_facts', CURRENT_TIMESTAMP(), 'STARTED', 'SILVER', 'GOLD', 'FACT_LOAD')",
    post_hook="INSERT INTO {{ ref('go_process_audit') }} (execution_id, pipeline_name, end_time, status, records_processed, source_system, target_system, process_type) VALUES (UUID_STRING(), 'go_webinar_facts', CURRENT_TIMESTAMP(), 'COMPLETED', (SELECT COUNT(*) FROM {{ this }}), 'SILVER', 'GOLD', 'FACT_LOAD')"
) }}

WITH webinar_base AS (
    SELECT 
        w.webinar_id,
        w.host_id,
        w.webinar_topic,
        w.start_time,
        w.end_time,
        w.registrants,
        w.load_date,
        w.update_date,
        w.source_system,
        w.record_status
    FROM {{ ref('si_webinars') }} w
    WHERE w.record_status = 'ACTIVE'
),

webinar_attendance AS (
    SELECT 
        p.meeting_id AS webinar_id,
        COUNT(DISTINCT p.participant_id) AS actual_attendees,
        COUNT(DISTINCT p.participant_id) AS max_concurrent_attendees
    FROM {{ ref('si_participants') }} p
    WHERE p.record_status = 'ACTIVE'
    GROUP BY p.meeting_id
),

webinar_interactions AS (
    SELECT 
        fu.meeting_id AS webinar_id,
        SUM(CASE WHEN fu.feature_name = 'Q&A' THEN fu.usage_count ELSE 0 END) AS qa_questions_count,
        SUM(CASE WHEN fu.feature_name = 'Polling' THEN fu.usage_count ELSE 0 END) AS poll_responses_count
    FROM {{ ref('si_feature_usage') }} fu
    WHERE fu.record_status = 'ACTIVE'
    GROUP BY fu.meeting_id
),

final_webinar_facts AS (
    SELECT 
        CONCAT('WF_', wb.webinar_id, '_', CURRENT_TIMESTAMP()::STRING) AS webinar_fact_id,
        wb.webinar_id,
        wb.host_id,
        TRIM(COALESCE(wb.webinar_topic, 'No Topic Specified')) AS webinar_topic,
        CONVERT_TIMEZONE('UTC', wb.start_time) AS start_time,
        CONVERT_TIMEZONE('UTC', wb.end_time) AS end_time,
        DATEDIFF('minute', wb.start_time, wb.end_time) AS duration_minutes,
        COALESCE(wb.registrants, 0) AS registrants_count,
        COALESCE(wa.actual_attendees, 0) AS actual_attendees,
        CASE 
            WHEN wb.registrants > 0 THEN (wa.actual_attendees::FLOAT / wb.registrants) * 100 
            ELSE 0 
        END AS attendance_rate,
        COALESCE(wa.max_concurrent_attendees, 0) AS max_concurrent_attendees,
        COALESCE(wi.qa_questions_count, 0) AS qa_questions_count,
        COALESCE(wi.poll_responses_count, 0) AS poll_responses_count,
        ROUND((COALESCE(wi.qa_questions_count, 0) * 0.4 + COALESCE(wi.poll_responses_count, 0) * 0.3 + 
               CASE WHEN wb.registrants > 0 THEN (wa.actual_attendees::FLOAT / wb.registrants) * 100 ELSE 0 END * 0.3) / 10, 2) AS engagement_score,
        CASE 
            WHEN DATEDIFF('minute', wb.start_time, wb.end_time) > 120 THEN 'Long Form'
            WHEN DATEDIFF('minute', wb.start_time, wb.end_time) > 60 THEN 'Standard'
            ELSE 'Short Form'
        END AS event_category,
        wb.load_date,
        CURRENT_DATE() AS update_date,
        wb.source_system,
        CURRENT_TIMESTAMP() AS created_at,
        CURRENT_TIMESTAMP() AS updated_at,
        'ACTIVE' AS process_status
    FROM webinar_base wb
    LEFT JOIN webinar_attendance wa ON wb.webinar_id = wa.webinar_id
    LEFT JOIN webinar_interactions wi ON wb.webinar_id = wi.webinar_id
)

SELECT * FROM final_webinar_facts
