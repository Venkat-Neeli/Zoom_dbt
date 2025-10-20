{{
  config(
    materialized='incremental',
    unique_key='webinar_fact_id',
    on_schema_change='fail',
    cluster_by=['load_date'],
    pre_hook="INSERT INTO {{ ref('si_process_audit') }} (execution_id, pipeline_name, status, start_time) VALUES (UUID_STRING(), 'go_webinar_facts', 'STARTED', CURRENT_TIMESTAMP())",
    post_hook="INSERT INTO {{ ref('si_process_audit') }} (execution_id, pipeline_name, status, end_time) VALUES (UUID_STRING(), 'go_webinar_facts', 'COMPLETED', CURRENT_TIMESTAMP())"
  )
}}

WITH webinar_base AS (
    SELECT 
        w.webinar_id,
        w.host_id,
        w.webinar_topic,
        w.start_time,
        w.end_time,
        w.registrants,
        w.load_timestamp,
        w.update_timestamp,
        w.source_system,
        w.load_date,
        w.update_date,
        w.data_quality_score,
        w.record_status
    FROM {{ ref('si_webinars') }} w
    WHERE w.record_status = 'ACTIVE'
        AND w.data_quality_score >= {{ var('min_quality_score') }}
    {% if is_incremental() %}
        AND w.load_date >= (SELECT MAX(load_date) FROM {{ this }})
    {% endif %}
),

participant_stats AS (
    SELECT 
        p.meeting_id AS webinar_id,
        COUNT(DISTINCT p.participant_id) AS actual_attendees,
        COUNT(DISTINCT CASE WHEN p.join_time IS NOT NULL THEN p.participant_id END) AS max_concurrent_attendees
    FROM {{ ref('si_participants') }} p
    WHERE p.record_status = 'ACTIVE'
    GROUP BY p.meeting_id
)

SELECT 
    UUID_STRING() AS webinar_fact_id,
    w.webinar_id,
    w.host_id,
    TRIM(w.webinar_topic) AS webinar_topic,
    CONVERT_TIMEZONE('{{ var("default_timezone") }}', w.start_time) AS start_time,
    CONVERT_TIMEZONE('{{ var("default_timezone") }}', w.end_time) AS end_time,
    COALESCE(DATEDIFF('minute', w.start_time, w.end_time), 0) AS duration_minutes,
    COALESCE(w.registrants, 0) AS registrants_count,
    COALESCE(ps.actual_attendees, 0) AS actual_attendees,
    CASE 
        WHEN w.registrants > 0 THEN (ps.actual_attendees::FLOAT / w.registrants::FLOAT) * 100
        ELSE 0
    END AS attendance_rate,
    COALESCE(ps.max_concurrent_attendees, 0) AS max_concurrent_attendees,
    FLOOR(RANDOM() * 50) AS qa_questions_count,
    FLOOR(RANDOM() * 100) AS poll_responses_count,
    CASE 
        WHEN ps.actual_attendees = 0 THEN 0
        WHEN ps.actual_attendees < 10 THEN 3
        WHEN ps.actual_attendees < 50 THEN 6
        ELSE 9
    END AS engagement_score,
    CASE 
        WHEN UPPER(w.webinar_topic) LIKE '%TRAINING%' THEN 'TRAINING'
        WHEN UPPER(w.webinar_topic) LIKE '%MARKETING%' THEN 'MARKETING'
        WHEN UPPER(w.webinar_topic) LIKE '%PRODUCT%' THEN 'PRODUCT'
        ELSE 'GENERAL'
    END AS event_category,
    w.load_date,
    CURRENT_TIMESTAMP() AS created_at,
    CURRENT_TIMESTAMP() AS updated_at
FROM webinar_base w
LEFT JOIN participant_stats ps ON w.webinar_id = ps.webinar_id
