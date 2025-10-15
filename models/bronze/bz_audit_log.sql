{{ config(
    materialized='table',
    pre_hook="",
    post_hook=""
) }}

-- Audit log table for tracking bronze layer processing
SELECT 
    ROW_NUMBER() OVER (ORDER BY CURRENT_TIMESTAMP()) as record_id,
    'AUDIT_LOG_INIT' as source_table,
    CURRENT_TIMESTAMP() as load_timestamp,
    'DBT_SYSTEM' as processed_by,
    0 as processing_time,
    'INITIALIZED' as status
WHERE FALSE  -- This ensures no data is inserted during initial creation
