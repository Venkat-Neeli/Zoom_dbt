{{ config(
    materialized='table',
    tags=['gold', 'fact']
) }}

SELECT
    webinar_id,
    host_id,
    webinar_topic,
    start_time,
    end_time,
    registrants,
    load_date,
    update_date,
    data_quality_score,
    record_status,
    load_timestamp,
    update_timestamp,
    source_system
FROM {{ ref('si_webinars') }}
WHERE record_status = 'ACTIVE'
  AND data_quality_score >= 0.8
