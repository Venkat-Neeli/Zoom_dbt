{{ config(
    materialized='table',
    tags=['gold', 'fact']
) }}

SELECT
    usage_id,
    meeting_id,
    feature_name,
    usage_count,
    usage_date,
    load_date,
    update_date,
    data_quality_score,
    record_status,
    load_timestamp,
    update_timestamp,
    source_system
FROM {{ ref('si_feature_usage') }}
WHERE record_status = 'ACTIVE'
  AND data_quality_score >= 0.8
