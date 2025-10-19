{{ config(
    materialized='table'
) }}

SELECT 
    'audit_log' as table_name,
    'initial_setup' as operation,
    CURRENT_TIMESTAMP() as start_time,
    CURRENT_TIMESTAMP() as end_time,
    'SUCCESS' as status
WHERE FALSE  -- This creates an empty table with the right structure
