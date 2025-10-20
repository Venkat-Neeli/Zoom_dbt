{{ config(
    materialized='table',
    schema='gold'
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
    source_system,
    data_quality_score,
    HASH(meeting_id || host_id) as meeting_key,
    CURRENT_TIMESTAMP() as created_at
FROM {{ ref('si_meetings') }}
WHERE record_status = 'ACTIVE'
    AND data_quality_score >= 0.7
