{{ config(
    materialized='table',
    schema='gold'
) }}

SELECT 
    event_id,
    user_id,
    event_type,
    amount,
    event_date,
    load_date,
    update_date,
    source_system,
    data_quality_score,
    HASH(event_id || user_id) as billing_key,
    CURRENT_TIMESTAMP() as created_at
FROM {{ ref('si_billing_events') }}
WHERE record_status = 'ACTIVE'
    AND data_quality_score >= 0.7
