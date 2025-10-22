{{ config(
    materialized='table',
    cluster_by=['start_time', 'host_id'],
    tags=['fact', 'gold'],
    pre_hook="INSERT INTO {{ ref('go_process_audit') }} (execution_id, pipeline_name, process_type, start_time, status, source_system, target_system, user_executed, server_name, load_date, update_date) SELECT '{{ dbt_utils.generate_surrogate_key(['invocation_id', 'this.name']) }}', 'GO_WEBINAR_FACTS', 'FACT_BUILD', CURRENT_TIMESTAMP(), 'STARTED', 'SILVER', 'GOLD', CURRENT_USER(), 'DBT_CLOUD', CURRENT_DATE(), CURRENT_DATE() WHERE '{{ this.name }}' != 'go_process_audit'",
    post_hook="UPDATE {{ ref('go_process_audit') }} SET end_time = CURRENT_TIMESTAMP(), status = 'COMPLETED', records_processed = (SELECT COUNT(*) FROM {{ this }}), records_successful = (SELECT COUNT(*) FROM {{ this }}), processing_duration_seconds = DATEDIFF('second', start_time, CURRENT_TIMESTAMP()) WHERE execution_id = '{{ dbt_utils.generate_surrogate_key(['invocation_id', 'this.name']) }}' AND '{{ this.name }}' != 'go_process_audit'"
) }}

-- Gold Webinar Facts Table
-- Comprehensive webinar analytics and engagement metrics

WITH webinar_base AS (
    SELECT
        webinar_id,
        host_id,
        webinar_topic,
        start_time,
        end_time,
        registrants,
        source_system,
        load_date,
        update_date
    FROM {{ source('silver', 'si_webinars') }}
    WHERE record_status = 'ACTIVE'
),

webinar_attendees AS (
    SELECT
        p.meeting_id AS webinar_id,
        COUNT(DISTINCT p.participant_id) AS actual_attendees
    FROM {{ source('silver', 'si_participants') }} p
    INNER JOIN webinar_base wb ON p.meeting_id = wb.webinar_id
    WHERE p.record_status = 'ACTIVE'
    GROUP BY p.meeting_id
),

webinar_features AS (
    SELECT
        fu.meeting_id AS webinar_id,
        SUM(CASE WHEN UPPER(fu.feature_name) LIKE '%Q&A%' OR UPPER(fu.feature_name) LIKE '%QA%' THEN fu.usage_count ELSE 0 END) AS qa_questions_count,
        SUM(CASE WHEN UPPER(fu.feature_name) LIKE '%POLL%' THEN fu.usage_count ELSE 0 END) AS poll_responses_count
    FROM {{ source('silver', 'si_feature_usage') }} fu
    INNER JOIN webinar_base wb ON fu.meeting_id = wb.webinar_id
    WHERE fu.record_status = 'ACTIVE'
    GROUP BY fu.meeting_id
),

webinar_facts AS (
    SELECT
        CONCAT('WF_', wb.webinar_id, '_', CURRENT_TIMESTAMP()::STRING) AS webinar_fact_id,
        wb.webinar_id,
        wb.host_id,
        TRIM(COALESCE(wb.webinar_topic, 'No Topic Specified')) AS webinar_topic,
        CONVERT_TIMEZONE('UTC', wb.start_time) AS start_time,
        CONVERT_TIMEZONE('UTC', wb.end_time) AS end_time,
        DATEDIFF('minute', wb.start_time, COALESCE(wb.end_time, CURRENT_TIMESTAMP())) AS duration_minutes,
        COALESCE(wb.registrants, 0) AS registrants_count,
        COALESCE(wa.actual_attendees, 0) AS actual_attendees,
        CASE 
            WHEN wb.registrants > 0 THEN (wa.actual_attendees::FLOAT / wb.registrants) * 100 
            ELSE 0 
        END AS attendance_rate,
        COALESCE(wa.actual_attendees, 0) AS max_concurrent_attendees,
        COALESCE(wf.qa_questions_count, 0) AS qa_questions_count,
        COALESCE(wf.poll_responses_count, 0) AS poll_responses_count,
        ROUND(
            (COALESCE(wf.qa_questions_count, 0) * 0.4 + 
             COALESCE(wf.poll_responses_count, 0) * 0.3 + 
             CASE WHEN wb.registrants > 0 THEN (wa.actual_attendees::FLOAT / wb.registrants) * 100 ELSE 0 END * 0.3) / 10, 2
        ) AS engagement_score,
        CASE 
            WHEN DATEDIFF('minute', wb.start_time, COALESCE(wb.end_time, CURRENT_TIMESTAMP())) > 120 THEN 'Long Form'
            WHEN DATEDIFF('minute', wb.start_time, COALESCE(wb.end_time, CURRENT_TIMESTAMP())) > 60 THEN 'Standard'
            ELSE 'Short Form'
        END AS event_category,
        wb.load_date,
        CURRENT_DATE() AS update_date,
        wb.source_system
    FROM webinar_base wb
    LEFT JOIN webinar_attendees wa ON wb.webinar_id = wa.webinar_id
    LEFT JOIN webinar_features wf ON wb.webinar_id = wf.webinar_id
)

SELECT
    webinar_fact_id::VARCHAR(50) AS webinar_fact_id,
    webinar_id::VARCHAR(50) AS webinar_id,
    host_id::VARCHAR(50) AS host_id,
    webinar_topic::VARCHAR(500) AS webinar_topic,
    start_time,
    end_time,
    duration_minutes,
    registrants_count,
    actual_attendees,
    attendance_rate,
    max_concurrent_attendees,
    qa_questions_count,
    poll_responses_count,
    engagement_score,
    event_category::VARCHAR(100) AS event_category,
    load_date,
    update_date,
    source_system::VARCHAR(100) AS source_system
FROM webinar_facts
