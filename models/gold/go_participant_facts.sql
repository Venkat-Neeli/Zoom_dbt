{{ config(
    materialized='table',
    schema='gold'
) }}

SELECT 
    participant_id,
    meeting_id,
    user_id,
    join_time,
    leave_time,
    load_date,
    update_date,
    source_system,
    data_quality_score,
    HASH(participant_id || meeting_id || user_id) as participant_key,
    CURRENT_TIMESTAMP() as created_at
FROM {{ ref('si_participants') }}
WHERE record_status = 'ACTIVE'
    AND data_quality_score >= 0.7
