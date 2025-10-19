{{ config(
    materialized='table'
) }}

SELECT 
    'usage_001' as usage_id,
    'meeting_001' as meeting_id,
    'SCREEN_SHARE' as feature_name,
    5 as usage_count,
    CURRENT_TIMESTAMP() as usage_date,
    CURRENT_TIMESTAMP() as load_timestamp,
    CURRENT_TIMESTAMP() as update_timestamp,
    'ZOOM_API' as source_system,
    CURRENT_DATE() as load_date,
    CURRENT_DATE() as update_date,
    0.95 as data_quality_score,
    'ACTIVE' as record_status
WHERE FALSE  -- This creates an empty table with the right structure
