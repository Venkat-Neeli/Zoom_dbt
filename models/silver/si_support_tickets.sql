{{ config(
    materialized='table'
) }}

SELECT 
    'ticket_001' as ticket_id,
    'user_001' as user_id,
    'AUDIO_ISSUE' as ticket_type,
    'RESOLVED' as resolution_status,
    CURRENT_DATE() as open_date,
    CURRENT_TIMESTAMP() as load_timestamp,
    CURRENT_TIMESTAMP() as update_timestamp,
    'SUPPORT_SYSTEM' as source_system,
    CURRENT_DATE() as load_date,
    CURRENT_DATE() as update_date,
    0.95 as data_quality_score,
    'ACTIVE' as record_status
WHERE FALSE  -- This creates an empty table with the right structure
