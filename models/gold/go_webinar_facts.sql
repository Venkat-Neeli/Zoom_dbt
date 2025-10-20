{{ config(materialized='table') }}

SELECT 
    webinar_id,
    host_id,
    webinar_topic,
    start_time,
    end_time,
    registrants,
    CURRENT_TIMESTAMP() as created_at
FROM {{ ref('si_webinars') }}
WHERE record_status = 'ACTIVE'
