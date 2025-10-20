{{ config(materialized='table') }}

SELECT 
    usage_id,
    meeting_id,
    feature_name,
    usage_count,
    usage_date,
    CURRENT_TIMESTAMP() as created_at
FROM {{ ref('si_feature_usage') }}
WHERE record_status = 'ACTIVE'
