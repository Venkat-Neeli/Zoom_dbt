{{ config(
    materialized='table',
    cluster_by=['start_time', 'host_id']
) }}

-- Gold Webinar Facts
WITH webinar_base AS (
    SELECT 
        webinar_id,
        host_id,
        webinar_topic,
        start_time,
        end_time,
        registrants,
        load_date,
        update_date,
        source_system
    FROM {{ source('silver', 'si_webinars') }}
    WHERE record_status = 'ACTIVE'
    AND webinar_id IS NOT NULL
),

attendee_metrics AS (
    SELECT 
        meeting_id AS webinar_id,
        COUNT(DISTINCT participant_id) AS actual_attendees
    FROM {{ source('silver', 'si_participants') }}
    WHERE record_status = 'ACTIVE'
    GROUP BY meeting_id
),

feature_metrics AS (
    SELECT 
        meeting_id AS webinar_id,
        SUM(CASE WHEN feature_name = 'Q&A' THEN usage_count ELSE 0 END) AS qa_questions_count,
        SUM(CASE WHEN feature_name = 'Polling' THEN usage_count ELSE 0 END) AS poll_responses_count
    FROM {{ source('silver', 'si_feature_usage') }}
    WHERE record_status = 'ACTIVE'
    GROUP BY meeting_id
),

webinar_facts AS (
    SELECT 
        CONCAT('WF_', w.webinar_id, '_', CURRENT_TIMESTAMP()::STRING) AS webinar_fact_id,
        w.webinar_id,
        w.host_id,
        TRIM(COALESCE(w.webinar_topic, 'No Topic Specified')) AS webinar_topic,
        CONVERT_TIMEZONE('UTC', w.start_time) AS start_time,
        CONVERT_TIMEZONE('UTC', w.end_time) AS end_time,
        DATEDIFF('minute', w.start_time, w.end_time) AS duration_minutes,
        COALESCE(w.registrants, 0) AS registrants_count,
        COALESCE(a.actual_attendees, 0) AS actual_attendees,
        CASE 
            WHEN w.registrants > 0 THEN (a.actual_attendees::FLOAT / w.registrants) * 100 
            ELSE 0 
        END AS attendance_rate,
        COALESCE(a.actual_attendees, 0) AS max_concurrent_attendees,
        COALESCE(f.qa_questions_count, 0) AS qa_questions_count,
        COALESCE(f.poll_responses_count, 0) AS poll_responses_count,
        ROUND((COALESCE(f.qa_questions_count, 0) * 0.4 + COALESCE(f.poll_responses_count, 0) * 0.3 + 
               CASE WHEN w.registrants > 0 THEN (a.actual_attendees::FLOAT / w.registrants) * 100 ELSE 0 END * 0.3) / 10, 2) AS engagement_score,
        CASE 
            WHEN DATEDIFF('minute', w.start_time, w.end_time) > 120 THEN 'Long Form'
            WHEN DATEDIFF('minute', w.start_time, w.end_time) > 60 THEN 'Standard'
            ELSE 'Short Form'
        END AS event_category,
        w.load_date,
        CURRENT_DATE() AS update_date,
        w.source_system
    FROM webinar_base w
    LEFT JOIN attendee_metrics a ON w.webinar_id = a.webinar_id
    LEFT JOIN feature_metrics f ON w.webinar_id = f.webinar_id
)

SELECT * FROM webinar_facts
