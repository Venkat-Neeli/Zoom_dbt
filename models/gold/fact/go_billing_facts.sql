{{ config(
    materialized='table',
    cluster_by=['event_date', 'user_id']
) }}

-- Billing Facts Transformation
WITH billing_base AS (
    SELECT 
        be.event_id,
        be.user_id,
        be.event_type,
        be.amount,
        be.event_date,
        be.load_date,
        be.source_system,
        u.company,
        ROW_NUMBER() OVER (PARTITION BY be.event_id ORDER BY be.update_timestamp DESC) as rn
    FROM {{ source('silver', 'si_billing_events') }} be
    LEFT JOIN {{ source('silver', 'si_users') }} u ON be.user_id = u.user_id
    WHERE be.record_status = 'ACTIVE'
)

SELECT 
    CONCAT('BF_', event_id, '_', user_id)::VARCHAR(50) AS billing_fact_id,
    event_id::VARCHAR(50) AS event_id,
    user_id::VARCHAR(50) AS user_id,
    COALESCE(UPPER(TRIM(company)), 'INDIVIDUAL')::VARCHAR(50) AS organization_id,
    UPPER(TRIM(event_type))::VARCHAR(100) AS event_type,
    ROUND(amount, 2) AS amount,
    event_date,
    DATE_TRUNC('month', event_date) AS billing_period_start,
    LAST_DAY(event_date) AS billing_period_end,
    'Credit Card'::VARCHAR(50) AS payment_method,
    CASE WHEN amount > 0 THEN 'Completed' ELSE 'Refunded' END::VARCHAR(50) AS transaction_status,
    'USD'::VARCHAR(10) AS currency_code,
    ROUND(amount * 0.08, 2) AS tax_amount,
    0.00 AS discount_amount,
    load_date,
    CURRENT_DATE() AS update_date,
    source_system
FROM billing_base
WHERE rn = 1
