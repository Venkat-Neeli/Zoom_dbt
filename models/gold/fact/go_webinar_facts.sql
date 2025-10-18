{{ config(
    materialized='table',
    cluster_by=['start_time', 'host_id'],
    pre_hook="INSERT INTO {{ ref('go_process_audit') }} (process_name, table_name, start_time, status) VALUES ('go_webinar_facts_transform', 'go_webinar_facts', CURRENT_TIMESTAMP(), 'STARTED') WHERE '{{ this.name }}' != 'go_process_audit'",
    post_hook="INSERT INTO {{ ref('go_process_audit') }} (process_name, table_name, end_time, status, records_processed) VALUES ('go_webinar_facts_transform', 'go_webinar_facts', CURRENT_TIMESTAMP(), 'COMPLETED', (SELECT COUNT(*) FROM {{ this }})) WHERE '{{ this.name }}' != 'go_process_audit'"
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
    WHERE webinar_id IS NOT NULL
        AND record_status = 'active'
),

host_info AS (
    SELECT 
        user_id,
        user_name,
        company,
        plan_type
    FROM {{ ref('si_users') }}
    WHERE user_id IS NOT NULL
        AND record_status = 'active'
),

webinar_engagement AS (
    SELECT 
        w.webinar_id,
        COUNT(DISTINCT p.participant_id) AS actual_attendees,
        COUNT(DISTINCT p.participant_id) AS max_concurrent_attendees,
        AVG(DATEDIFF('minute', p.join_time, p.leave_time)) AS avg_attendance_duration
    FROM webinar_base w
    LEFT JOIN {{ ref('si_participants') }} p ON w.webinar_id::STRING = p.meeting_id::STRING
    WHERE p.record_status = 'active' OR p.record_status IS NULL
    GROUP BY w.webinar_id
),

webinar_features AS (
    SELECT 
        meeting_id as webinar_id,
        SUM(CASE WHEN feature_name = 'Q&A' THEN usage_count ELSE 0 END) AS qa_questions_count,
        SUM(CASE WHEN feature_name = 'Polling' THEN usage_count ELSE 0 END) AS poll_responses_count
    FROM {{ ref('si_feature_usage') }}
    WHERE meeting_id IS NOT NULL
        AND record_status = 'active'
    GROUP BY meeting_id
)

SELECT 
    CONCAT('WF_', wb.webinar_id, '_', CURRENT_TIMESTAMP()::STRING) AS webinar_fact_id,
    wb.webinar_id,
    wb.host_id,
    TRIM(COALESCE(wb.webinar_topic, 'No Topic Specified')) AS webinar_topic,
    CONVERT_TIMEZONE('UTC', wb.start_time) AS start_time,
    CONVERT_TIMEZONE('UTC', wb.end_time) AS end_time,
    DATEDIFF('minute', wb.start_time, wb.end_time) AS duration_minutes,
    COALESCE(wb.registrants, 0) AS registrants_count,
    COALESCE(we.actual_attendees, 0) AS actual_attendees,
    CASE 
        WHEN wb.registrants > 0 
        THEN ROUND((COALESCE(we.actual_attendees, 0)::FLOAT / wb.registrants) * 100, 2)
        ELSE 0 
    END AS attendance_rate,
    COALESCE(we.max_concurrent_attendees, 0) AS max_concurrent_attendees,
    COALESCE(wf.qa_questions_count, 0) AS qa_questions_count,
    COALESCE(wf.poll_responses_count, 0) AS poll_responses_count,
    ROUND(
        (COALESCE(wf.qa_questions_count, 0) * 0.4 + 
         COALESCE(wf.poll_responses_count, 0) * 0.3 + 
         CASE WHEN wb.registrants > 0 THEN (COALESCE(we.actual_attendees, 0)::FLOAT / wb.registrants) * 100 ELSE 0 END * 0.3) / 10, 2
    ) AS engagement_score,
    CASE 
        WHEN DATEDIFF('minute', wb.start_time, wb.end_time) > 120 THEN 'Long Form'
        WHEN DATEDIFF('minute', wb.start_time, wb.end_time) > 60 THEN 'Standard'
        ELSE 'Short Form'
    END AS event_category,
    wb.load_date,
    CURRENT_DATE() AS update_date,
    wb.source_system
FROM webinar_base wb
LEFT JOIN host_info hi ON wb.host_id = hi.user_id
LEFT JOIN webinar_engagement we ON wb.webinar_id = we.webinar_id
LEFT JOIN webinar_features wf ON wb.webinar_id = wf.webinar_id
