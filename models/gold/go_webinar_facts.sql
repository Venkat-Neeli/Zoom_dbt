{{ config(
    materialized='table',
    schema='gold'
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
    source_system,
    data_quality_score,
    HASH(webinar_id || host_id) as webinar_key,
    CURRENT_TIMESTAMP() as created_at
FROM {{ ref('si_webinars') }}
WHERE record_status = 'ACTIVE'
    AND data_quality_score >= 0.7
