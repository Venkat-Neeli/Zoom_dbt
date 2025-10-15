{{ config(materialized='table') }}

-- Audit log table for tracking bronze layer processing
SELECT
    1 as record_id,
    CAST('INITIAL' AS VARCHAR(255)) as source_table,
    CURRENT_TIMESTAMP() as load_timestamp,
    'DBT' as processed_by,
    0 as processing_time,
    'INITIALIZED' as status
WHERE FALSE  -- This ensures no initial records are created
