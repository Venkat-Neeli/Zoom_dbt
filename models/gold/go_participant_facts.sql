{{ config(materialized='table') }}

SELECT 
    participant_id,
    meeting_id,
    user_id,
    join_time,
    leave_time,
    CURRENT_TIMESTAMP() as created_at
FROM {{ ref('si_participants') }}
WHERE record_status = 'ACTIVE'
