{{ config(
    materialized='table'
) }}

SELECT 
    'user_001' as user_id,
    'John Doe' as user_name,
    'john.doe@example.com' as email,
    'Example Corp' as company,
    'Pro' as plan_type,
    CURRENT_TIMESTAMP() as load_timestamp,
    CURRENT_TIMESTAMP() as update_timestamp,
    'ZOOM_API' as source_system,
    CURRENT_DATE() as load_date,
    CURRENT_DATE() as update_date,
    0.95 as data_quality_score,
    'ACTIVE' as record_status
WHERE FALSE  -- This creates an empty table with the right structure
