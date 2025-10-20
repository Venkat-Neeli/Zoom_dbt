{{ config(
    materialized='table',
    schema='gold'
) }}

SELECT 
    usage_id,
    meeting_id,
    feature_name,
    usage_count,
    usage_date,
    load_date,
    update_date,
    source_system,
    data_quality_score,
    HASH(usage_id || meeting_id) as usage_key,
    CURRENT_TIMESTAMP() as created_at
FROM {{ ref('si_feature_usage') }}
WHERE record_status = 'ACTIVE'
    AND data_quality_score >= 0.7
