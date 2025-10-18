{{ config(
    materialized='table',
    cluster_by=['start_time', 'host_id'],
    pre_hook="INSERT INTO {{ ref('go_process_audit') }} (execution_id, pipeline_name, start_time, status, source_system, target_system, process_type, user_executed) VALUES (UUID_STRING(), 'go_webinar_facts', CURRENT_TIMESTAMP(), 'STARTED', 'SILVER', 'GOLD', 'FACT_LOAD', CURRENT_USER()) WHERE '{{ this.name }}' != 'go_process_audit'",
    post_hook="UPDATE {{ ref('go_process_audit') }} SET end_time = CURRENT_TIMESTAMP(), status = 'COMPLETED', records_processed = (SELECT COUNT(*) FROM {{ this }}) WHERE pipeline_name = 'go_webinar_facts' AND status = 'STARTED' AND '{{ this.name }}' != 'go_process_audit'"
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
        w.source_system
    FROM {{ ref('si_webinars') }} w
    WHERE w.record_status = 'ACTIVE'
),

webinar_attendance AS (
    SELECT 
        p.meeting_id AS webinar_id,
        COUNT(DISTINCT p.participant_id) AS actual_attendees
    FROM {{ ref('si_participants') }} p
    WHERE p.record_status = 'ACTIVE'
    GROUP BY p.meeting_id
),

webinar_features AS (
    SELECT 
        f.meeting_id AS webinar_id,
        SUM(CASE WHEN f.feature_name = 'Q&A' THEN f.usage_count ELSE 0 END) AS qa_questions_count,
        SUM(CASE WHEN f.feature_name = 'Polling' THEN f.usage_count ELSE 0 END) AS poll_responses_count
    FROM {{ ref('si_feature_usage') }} f
    WHERE f.record_status = 'ACTIVE'
    GROUP BY f.meeting_id
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
        COALESCE(wa.actual_attendees, 0) AS max_concurrent_attendees,
        COALESCE(wf.qa_questions_count, 0) AS qa_questions_count,
        COALESCE(wf.poll_responses_count, 0) AS poll_responses_count,
        ROUND((COALESCE(wf.qa_questions_count, 0) * 0.4 + COALESCE(wf.poll_responses_count, 0) * 0.3 + COALESCE(wa.actual_attendees, 0) * 0.3) / 10, 2) AS engagement_score,
        CASE 
            WHEN DATEDIFF('minute', wb.start_time, wb.end_time) > 120 THEN 'Long Form'
            WHEN DATEDIFF('minute', wb.start_time, wb.end_time) > 60 THEN 'Standard'
            ELSE 'Short Form'
        END AS event_category,
        wb.load_date,
        CURRENT_DATE() AS update_date,
        wb.source_system
    FROM webinar_base wb
    LEFT JOIN webinar_attendance wa ON wb.webinar_id = wa.webinar_id
    LEFT JOIN webinar_features wf ON wb.webinar_id = wf.webinar_id
)

SELECT * FROM final_webinar_facts
