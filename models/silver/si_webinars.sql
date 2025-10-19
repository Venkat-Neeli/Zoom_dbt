{{ config(
    materialized='table'
) }}

SELECT 
    'webinar_001' as webinar_id,
    'user_001' as host_id,
    'Sample Webinar' as webinar_topic,
    CURRENT_TIMESTAMP() as start_time,
    CURRENT_TIMESTAMP() as end_time,
    100 as registrants,
    CURRENT_TIMESTAMP() as load_timestamp,
    CURRENT_TIMESTAMP() as update_timestamp,
    'ZOOM_API' as source_system,
    CURRENT_DATE() as load_date,
    CURRENT_DATE() as update_date,
    0.95 as data_quality_score,
    'ACTIVE' as record_status
WHERE FALSE  -- This creates an empty table with the right structure
