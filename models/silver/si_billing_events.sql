{{ config(
    materialized='table'
) }}

SELECT 
    'event_001' as event_id,
    'user_001' as user_id,
    'SUBSCRIPTION' as event_type,
    99.99 as amount,
    CURRENT_DATE() as event_date,
    CURRENT_TIMESTAMP() as load_timestamp,
    CURRENT_TIMESTAMP() as update_timestamp,
    'BILLING_SYSTEM' as source_system,
    CURRENT_DATE() as load_date,
    CURRENT_DATE() as update_date,
    0.95 as data_quality_score,
    'ACTIVE' as record_status
WHERE FALSE  -- This creates an empty table with the right structure
