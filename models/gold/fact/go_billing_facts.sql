{{ config(
    materialized='table',
    cluster_by=['event_date', 'user_id']
) }}

-- Gold Billing Facts
WITH billing_base AS (
    SELECT 
        b.event_id,
        b.user_id,
        b.event_type,
        b.amount,
        b.event_date,
        b.load_date,
        b.source_system,
        u.company
    FROM {{ source('silver', 'si_billing_events') }} b
    LEFT JOIN {{ source('silver', 'si_users') }} u ON b.user_id = u.user_id
    WHERE b.record_status = 'ACTIVE'
    AND b.event_id IS NOT NULL
),

billing_facts AS (
    SELECT 
        CONCAT('BF_', event_id, '_', user_id) AS billing_fact_id,
        event_id,
        user_id,
        COALESCE(company, 'INDIVIDUAL') AS organization_id,
        UPPER(TRIM(event_type)) AS event_type,
        ROUND(amount, 2) AS amount,
        event_date,
        DATE_TRUNC('month', event_date) AS billing_period_start,
        LAST_DAY(event_date) AS billing_period_end,
        'Credit Card' AS payment_method,
        CASE WHEN amount > 0 THEN 'Completed' ELSE 'Refunded' END AS transaction_status,
        'USD' AS currency_code,
        ROUND(amount * 0.08, 2) AS tax_amount,
        0.00 AS discount_amount,
        load_date,
        CURRENT_DATE() AS update_date,
        source_system
    FROM billing_base
)

SELECT * FROM billing_facts
