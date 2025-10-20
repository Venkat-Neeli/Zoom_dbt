{{
  config(
    materialized='incremental',
    unique_key='webinar_fact_id',
    on_schema_change='sync_all_columns',
    pre_hook="INSERT INTO {{ target.schema }}.audit_log (table_name, operation, start_time, user_name) VALUES ('go_webinar_facts', 'transform_start', CURRENT_TIMESTAMP(), CURRENT_USER())",
    post_hook="INSERT INTO {{ target.schema }}.audit_log (table_name, operation, end_time, user_name, records_processed) VALUES ('go_webinar_facts', 'transform_end', CURRENT_TIMESTAMP(), CURRENT_USER(), (SELECT COUNT(*) FROM {{ this }}))"
  )
}}

WITH webinar_base AS (
  SELECT 
    webinar_id,
    host_id,
    TRIM(webinar_topic) AS webinar_topic,
    CONVERT_TIMEZONE('UTC', start_time) AS start_time,
    CONVERT_TIMEZONE('UTC', end_time) AS end_time,
    COALESCE(registrants, 0) AS registrants,
    load_timestamp,
    update_timestamp,
    source_system,
    load_date,
    update_date,
    data_quality_score
  FROM {{ source('silver', 'si_webinars') }}
  WHERE record_status = 'ACTIVE'
    AND data_quality_score >= {{ var('min_quality_score') }}
    AND webinar_id IS NOT NULL
    {% if is_incremental() %}
      AND update_timestamp > (SELECT MAX(update_date) FROM {{ this }})
    {% endif %}
),

webinar_participants AS (
  SELECT 
    w.webinar_id,
    COUNT(DISTINCT p.participant_id) AS actual_attendees,
    COUNT(DISTINCT p.participant_id) AS max_concurrent_attendees
  FROM webinar_base w
  LEFT JOIN {{ source('silver', 'si_participants') }} p 
    ON CAST(w.webinar_id AS STRING) = CAST(p.meeting_id AS STRING)
  WHERE p.record_status = 'ACTIVE'
    AND p.data_quality_score >= {{ var('min_quality_score') }}
  GROUP BY w.webinar_id
)

SELECT 
  {{ dbt_utils.generate_surrogate_key(['wb.webinar_id', 'wb.host_id']) }} AS webinar_fact_id,
  wb.webinar_id,
  wb.host_id,
  wb.webinar_topic,
  wb.start_time,
  wb.end_time,
  COALESCE(DATEDIFF('minute', wb.start_time, wb.end_time), 0) AS duration_minutes,
  wb.registrants AS registrants_count,
  COALESCE(wp.actual_attendees, 0) AS actual_attendees,
  CASE 
    WHEN wb.registrants > 0 THEN ROUND((COALESCE(wp.actual_attendees, 0) * 100.0) / wb.registrants, 2)
    ELSE 0
  END AS attendance_rate,
  COALESCE(wp.max_concurrent_attendees, 0) AS max_concurrent_attendees,
  0 AS qa_questions_count,
  0 AS poll_responses_count,
  CASE 
    WHEN COALESCE(wp.actual_attendees, 0) > 100 THEN 'High'
    WHEN COALESCE(wp.actual_attendees, 0) > 50 THEN 'Medium'
    ELSE 'Low'
  END AS engagement_score,
  CASE 
    WHEN UPPER(wb.webinar_topic) LIKE '%TRAINING%' THEN 'Training'
    WHEN UPPER(wb.webinar_topic) LIKE '%PRODUCT%' THEN 'Product Demo'
    ELSE 'General'
  END AS event_category,
  wb.load_date,
  wb.update_date,
  wb.source_system,
  {{ dbt_utils.generate_surrogate_key(['wb.webinar_id', 'wb.host_id']) }} AS surrogate_key
FROM webinar_base wb
LEFT JOIN webinar_participants wp ON wb.webinar_id = wp.webinar_id
