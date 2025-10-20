{{ config(materialized='table') }}

SELECT 
    meeting_id,
    host_id,
    meeting_topic,
    start_time,
    end_time,
    duration_minutes,
    CURRENT_TIMESTAMP() as created_at
FROM {{ ref('si_meetings') }}
WHERE record_status = 'ACTIVE'
