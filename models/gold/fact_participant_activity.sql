{{ config(
    materialized='table',
    tags=['gold', 'fact']
) }}

SELECT
    participant_id,
    meeting_id,
    user_id,
    join_time,
    leave_time,
    load_date,
    update_date,
    data_quality_score,
    record_status,
    load_timestamp,
    update_timestamp,
    source_system
FROM {{ ref('si_participants') }}
WHERE record_status = 'ACTIVE'
  AND data_quality_score >= 0.8
