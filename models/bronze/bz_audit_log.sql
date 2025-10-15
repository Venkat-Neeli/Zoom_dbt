{{ config(materialized='table') }}

-- Audit log table to track processing information
-- This table must be created first to support audit logging in other models
SELECT 
    1 as record_id,
    'INITIAL_SETUP' as source_table,
    CURRENT_TIMESTAMP() as load_timestamp,
    'DBT_SYSTEM' as processed_by,
    0 as processing_time,
    'INITIALIZED' as status
WHERE FALSE -- This ensures no actual data is inserted during initial creation
