{{ config(
    materialized='table',
    pre_hook="",
    post_hook=""
) }}

-- Audit log table for tracking bronze layer processing
WITH audit_base AS (
    SELECT 
        NULL::NUMBER as record_id,
        'AUDIT_LOG'::VARCHAR(255) as source_table,
        CURRENT_TIMESTAMP()::TIMESTAMP_NTZ as load_timestamp,
        'DBT_SYSTEM'::STRING as processed_by,
        0::NUMBER as processing_time,
        'INITIALIZED'::STRING as status
    WHERE FALSE  -- This ensures no actual records are inserted during model creation
)

SELECT 
    record_id,
    source_table,
    load_timestamp,
    processed_by,
    processing_time,
    status
FROM audit_base
