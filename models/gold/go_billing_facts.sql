{{ config(materialized='table') }}

SELECT 
    event_id,
    user_id,
    event_type,
    amount,
    event_date,
    CURRENT_TIMESTAMP() as created_at
FROM {{ ref('si_billing_events') }}
WHERE record_status = 'ACTIVE'
