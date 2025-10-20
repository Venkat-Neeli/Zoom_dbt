{{ config(
    materialized='table',
    tags=['gold', 'fact']
) }}

SELECT
    meeting_id,
    host_id,
    meeting_topic,
    start_time,
    end_time,
    duration_minutes,
    load_date,
    update_date,
    data_quality_score,
    record_status,
    load_timestamp,
    update_timestamp,
    source_system
FROM {{ ref('si_meetings') }}
WHERE record_status = 'ACTIVE'
  AND data_quality_score >= 0.8
