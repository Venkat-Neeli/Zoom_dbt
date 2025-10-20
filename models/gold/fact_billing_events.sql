{{ config(
    materialized='table',
    tags=['gold', 'fact']
) }}

SELECT
    event_id,
    user_id,
    event_type,
    amount,
    event_date,
    load_date,
    update_date,
    data_quality_score,
    record_status,
    load_timestamp,
    update_timestamp,
    source_system
FROM {{ ref('si_billing_events') }}
WHERE record_status = 'ACTIVE'
  AND data_quality_score >= 0.8
