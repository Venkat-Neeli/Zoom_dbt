{{ config(
    materialized='table'
) }}

SELECT 
    'participant_001' as participant_id,
    'meeting_001' as meeting_id,
    'user_001' as user_id,
    CURRENT_TIMESTAMP() as join_time,
    CURRENT_TIMESTAMP() as leave_time,
    CURRENT_TIMESTAMP() as load_timestamp,
    CURRENT_TIMESTAMP() as update_timestamp,
    'ZOOM_API' as source_system,
    CURRENT_DATE() as load_date,
    CURRENT_DATE() as update_date,
    0.95 as data_quality_score,
    'ACTIVE' as record_status
WHERE FALSE  -- This creates an empty table with the right structure
