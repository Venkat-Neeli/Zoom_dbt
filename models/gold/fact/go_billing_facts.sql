{{ config(
    materialized='table'
) }}

WITH billing_base AS (
    SELECT 
        event_id,
        user_id,
        event_type,
        amount,
        event_date,
        source_system,
        load_date,
        update_date,
        data_quality_score
    FROM {{ ref('si_billing_events') }}
    WHERE record_status = 'ACTIVE'
)

SELECT 
    CONCAT('BF_', event_id, '_', user_id) AS billing_fact_id,
    event_id,
    user_id,
    'INDIVIDUAL' AS organization_id,
    UPPER(TRIM(event_type)) AS event_type,
    ROUND(amount, 2) AS amount,
    event_date,
    DATE_TRUNC('month', event_date) AS billing_period_start,
    LAST_DAY(event_date) AS billing_period_end,
    'Credit Card' AS payment_method,
    CASE 
        WHEN amount > 0 THEN 'Completed' 
        ELSE 'Refunded' 
    END AS transaction_status,
    'USD' AS currency_code,
    amount * 0.08 AS tax_amount,
    0.00 AS discount_amount,
    load_date,
    CURRENT_DATE() AS update_date,
    source_system
FROM billing_base
