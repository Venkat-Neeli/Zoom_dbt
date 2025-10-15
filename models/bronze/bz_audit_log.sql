{{ config(materialized='table') }}

SELECT 
    1 as record_id,
    'INITIAL' as source_table,
    CURRENT_TIMESTAMP() as load_timestamp,
    'DBT_SYSTEM' as processed_by,
    0 as processing_time,
    'INITIALIZED' as status
WHERE FALSE -- This ensures no actual records are inserted during initial creation
