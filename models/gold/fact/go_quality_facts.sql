{{
  config(
    materialized='incremental',
    unique_key='quality_fact_id',
    on_schema_change='sync_all_columns',
    pre_hook="INSERT INTO {{ target.schema }}.audit_log (table_name, operation, start_time, user_name) VALUES ('go_quality_facts', 'transform_start', CURRENT_TIMESTAMP(), CURRENT_USER())",
    post_hook="INSERT INTO {{ target.schema }}.audit_log (table_name, operation, end_time, user_name, records_processed) VALUES ('go_quality_facts', 'transform_end', CURRENT_TIMESTAMP(), CURRENT_USER(), (SELECT COUNT(*) FROM {{ this }}))"
  )
}}

WITH quality_base AS (
  SELECT 
    p.meeting_id,
    p.participant_id,
    p.user_id,
    p.data_quality_score,
    m.duration_minutes,
    p.load_timestamp,
    p.update_timestamp,
    p.source_system,
    p.load_date,
    p.update_date
  FROM {{ source('silver', 'si_participants') }} p
  INNER JOIN {{ source('silver', 'si_meetings') }} m ON p.meeting_id = m.meeting_id
  WHERE p.record_status = 'ACTIVE'
    AND p.data_quality_score >= {{ var('min_quality_score') }}
    AND m.record_status = 'ACTIVE'
    AND m.data_quality_score >= {{ var('min_quality_score') }}
    AND p.participant_id IS NOT NULL
    {% if is_incremental() %}
      AND p.update_timestamp > (SELECT MAX(update_date) FROM {{ this }})
    {% endif %}
)

SELECT 
  {{ dbt_utils.generate_surrogate_key(['meeting_id', 'participant_id']) }} AS quality_fact_id,
  meeting_id,
  participant_id,
  {{ dbt_utils.generate_surrogate_key(['meeting_id', 'participant_id', 'user_id']) }} AS device_connection_id,
  CASE 
    WHEN data_quality_score >= 0.9 THEN 95
    WHEN data_quality_score >= 0.8 THEN 85
    WHEN data_quality_score >= 0.7 THEN 75
    ELSE 65
  END AS audio_quality_score,
  CASE 
    WHEN data_quality_score >= 0.9 THEN 98
    WHEN data_quality_score >= 0.8 THEN 88
    WHEN data_quality_score >= 0.7 THEN 78
    ELSE 68
  END AS video_quality_score,
  CASE 
    WHEN data_quality_score >= 0.9 THEN 'Excellent'
    WHEN data_quality_score >= 0.8 THEN 'Good'
    WHEN data_quality_score >= 0.7 THEN 'Fair'
    ELSE 'Poor'
  END AS connection_stability_rating,
  CASE 
    WHEN data_quality_score >= 0.9 THEN 50
    WHEN data_quality_score >= 0.8 THEN 100
    WHEN data_quality_score >= 0.7 THEN 150
    ELSE 200
  END AS latency_ms,
  CASE 
    WHEN data_quality_score >= 0.9 THEN 0.1
    WHEN data_quality_score >= 0.8 THEN 0.5
    WHEN data_quality_score >= 0.7 THEN 1.0
    ELSE 2.0
  END AS packet_loss_rate,
  ROUND(RANDOM() * 100, 2) AS bandwidth_utilization,
  ROUND(RANDOM() * 80 + 20, 1) AS cpu_usage_percentage,
  ROUND(RANDOM() * 2048 + 512, 0) AS memory_usage_mb,
  load_date,
  update_date,
  source_system,
  {{ dbt_utils.generate_surrogate_key(['meeting_id', 'participant_id']) }} AS surrogate_key
FROM quality_base
