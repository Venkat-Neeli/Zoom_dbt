{{
  config(
    materialized='incremental',
    unique_key='usage_fact_id',
    on_schema_change='sync_all_columns',
    pre_hook="INSERT INTO {{ target.schema }}.audit_log (table_name, operation, start_time, user_name) VALUES ('go_usage_facts', 'transform_start', CURRENT_TIMESTAMP(), CURRENT_USER())",
    post_hook="INSERT INTO {{ target.schema }}.audit_log (table_name, operation, end_time, user_name, records_processed) VALUES ('go_usage_facts', 'transform_end', CURRENT_TIMESTAMP(), CURRENT_USER(), (SELECT COUNT(*) FROM {{ this }}))"
  )
}}

WITH daily_usage AS (
  SELECT 
    u.user_id,
    DATE(fu.usage_date) AS usage_date,
    u.company AS organization_name,
    COUNT(DISTINCT m.meeting_id) AS meeting_count,
    SUM(COALESCE(m.duration_minutes, 0)) AS total_meeting_minutes,
    COUNT(DISTINCT w.webinar_id) AS webinar_count,
    SUM(COALESCE(DATEDIFF('minute', w.start_time, w.end_time), 0)) AS total_webinar_minutes,
    SUM(COALESCE(fu.usage_count, 0)) AS feature_usage_count,
    COUNT(DISTINCT p.participant_id) AS unique_participants_hosted
  FROM {{ source('silver', 'si_users') }} u
  LEFT JOIN {{ source('silver', 'si_meetings') }} m ON u.user_id = m.host_id
    AND m.record_status = 'ACTIVE' AND m.data_quality_score >= {{ var('min_quality_score') }}
  LEFT JOIN {{ source('silver', 'si_webinars') }} w ON u.user_id = w.host_id
    AND w.record_status = 'ACTIVE' AND w.data_quality_score >= {{ var('min_quality_score') }}
  LEFT JOIN {{ source('silver', 'si_feature_usage') }} fu ON m.meeting_id = fu.meeting_id
    AND fu.record_status = 'ACTIVE' AND fu.data_quality_score >= {{ var('min_quality_score') }}
  LEFT JOIN {{ source('silver', 'si_participants') }} p ON m.meeting_id = p.meeting_id
    AND p.record_status = 'ACTIVE' AND p.data_quality_score >= {{ var('min_quality_score') }}
  WHERE u.record_status = 'ACTIVE'
    AND u.data_quality_score >= {{ var('min_quality_score') }}
    AND u.user_id IS NOT NULL
    {% if is_incremental() %}
      AND u.update_timestamp > (SELECT MAX(update_date) FROM {{ this }})
    {% endif %}
  GROUP BY u.user_id, DATE(fu.usage_date), u.company
)

SELECT 
  {{ dbt_utils.generate_surrogate_key(['user_id', 'usage_date']) }} AS usage_fact_id,
  user_id,
  {{ dbt_utils.generate_surrogate_key(['organization_name']) }} AS organization_id,
  usage_date,
  COALESCE(meeting_count, 0) AS meeting_count,
  COALESCE(total_meeting_minutes, 0) AS total_meeting_minutes,
  COALESCE(webinar_count, 0) AS webinar_count,
  COALESCE(total_webinar_minutes, 0) AS total_webinar_minutes,
  ROUND(COALESCE(total_meeting_minutes, 0) / 1024.0, 2) AS recording_storage_gb,
  COALESCE(feature_usage_count, 0) AS feature_usage_count,
  COALESCE(unique_participants_hosted, 0) AS unique_participants_hosted,
  CURRENT_DATE() AS load_date,
  CURRENT_DATE() AS update_date,
  'ZOOM_API' AS source_system,
  {{ dbt_utils.generate_surrogate_key(['user_id', 'usage_date']) }} AS surrogate_key
FROM daily_usage
WHERE usage_date IS NOT NULL
