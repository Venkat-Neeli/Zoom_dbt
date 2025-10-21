{{ config(
    materialized='incremental',
    unique_key='webinar_fact_id',
    on_schema_change='sync_all_columns',
    pre_hook="INSERT INTO {{ ref('go_process_audit') }} (execution_id, pipeline_name, start_time, status, process_type, source_system, target_system, user_executed) VALUES (UUID_STRING(), 'go_webinar_facts_load', CURRENT_TIMESTAMP(), 'STARTED', 'FACT_LOAD', 'SILVER', 'GOLD', CURRENT_USER()) WHERE '{{ this.name }}' != 'go_process_audit'",
    post_hook="INSERT INTO {{ ref('go_process_audit') }} (execution_id, pipeline_name, end_time, status, process_type, source_system, target_system, user_executed, records_processed) VALUES (UUID_STRING(), 'go_webinar_facts_load', CURRENT_TIMESTAMP(), 'COMPLETED', 'FACT_LOAD', 'SILVER', 'GOLD', CURRENT_USER(), (SELECT COUNT(*) FROM {{ this }})) WHERE '{{ this.name }}' != 'go_process_audit'"
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
        update_date,
        source_system
    FROM {{ ref('si_webinars') }}
    WHERE record_status = 'ACTIVE'
    {% if is_incremental() %}
        AND update_date > (SELECT MAX(update_date) FROM {{ this }})
    {% endif %}
),

attendee_metrics AS (
    SELECT 
        meeting_id AS webinar_id,
        COUNT(DISTINCT participant_id) AS actual_attendees
    FROM {{ ref('si_participants') }}
    WHERE record_status = 'ACTIVE'
    GROUP BY meeting_id
),

webinar_features AS (
    SELECT 
        meeting_id AS webinar_id,
        SUM(CASE WHEN feature_name = 'Q&A' THEN usage_count ELSE 0 END) AS qa_questions_count,
        SUM(CASE WHEN feature_name = 'Polling' THEN usage_count ELSE 0 END) AS poll_responses_count
    FROM {{ ref('si_feature_usage') }}
    WHERE record_status = 'ACTIVE'
    GROUP BY meeting_id
),

final_transform AS (
    SELECT 
        CONCAT('WF_', wb.webinar_id, '_', CURRENT_TIMESTAMP()::STRING) AS webinar_fact_id,
        wb.webinar_id,
        wb.host_id,
        TRIM(COALESCE(wb.webinar_topic, 'No Topic Specified')) AS webinar_topic,
        CONVERT_TIMEZONE('UTC', wb.start_time) AS start_time,
        CONVERT_TIMEZONE('UTC', wb.end_time) AS end_time,
        DATEDIFF('minute', wb.start_time, wb.end_time) AS duration_minutes,
        COALESCE(wb.registrants, 0) AS registrants_count,
        COALESCE(am.actual_attendees, 0) AS actual_attendees,
        CASE 
            WHEN wb.registrants > 0 THEN (am.actual_attendees::FLOAT / wb.registrants) * 100 
            ELSE 0 
        END AS attendance_rate,
        0 AS max_concurrent_attendees, -- Complex calculation placeholder
        COALESCE(wf.qa_questions_count, 0) AS qa_questions_count,
        COALESCE(wf.poll_responses_count, 0) AS poll_responses_count,
        ROUND((COALESCE(wf.qa_questions_count, 0) * 0.4 + COALESCE(wf.poll_responses_count, 0) * 0.3 + 
               CASE WHEN wb.registrants > 0 THEN (am.actual_attendees::FLOAT / wb.registrants) * 100 ELSE 0 END * 0.3) / 10, 2) AS engagement_score,
        CASE 
            WHEN DATEDIFF('minute', wb.start_time, wb.end_time) > 120 THEN 'Long Form'
            WHEN DATEDIFF('minute', wb.start_time, wb.end_time) > 60 THEN 'Standard'
            ELSE 'Short Form'
        END AS event_category,
        wb.load_date,
        CURRENT_DATE() AS update_date,
        wb.source_system
    FROM webinar_base wb
    LEFT JOIN attendee_metrics am ON wb.webinar_id = am.webinar_id
    LEFT JOIN webinar_features wf ON wb.webinar_id = wf.webinar_id
)

SELECT * FROM final_transform
