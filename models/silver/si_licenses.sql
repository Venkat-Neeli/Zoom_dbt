{{ config(
    materialized='table'
) }}

SELECT 
    'license_001' as license_id,
    'Pro' as license_type,
    'user_001' as assigned_to_user_id,
    CURRENT_DATE() as start_date,
    CURRENT_DATE() + 365 as end_date,
    CURRENT_TIMESTAMP() as load_timestamp,
    CURRENT_TIMESTAMP() as update_timestamp,
    'LICENSE_SYSTEM' as source_system,
    CURRENT_DATE() as load_date,
    CURRENT_DATE() as update_date,
    0.95 as data_quality_score,
    'ACTIVE' as record_status
WHERE FALSE  -- This creates an empty table with the right structure
