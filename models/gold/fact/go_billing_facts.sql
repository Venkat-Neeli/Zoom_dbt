{{ config(
    materialized='table'
) }}

SELECT 
    'BF_' || 'SAMPLE_001' AS billing_fact_id,
    'EVENT_001' AS event_id,
    'USER_001' AS user_id,
    'ORG_001' AS organization_id,
    'SUBSCRIPTION' AS event_type,
    29.99 AS amount,
    CURRENT_DATE() AS event_date,
    DATE_TRUNC('month', CURRENT_DATE()) AS billing_period_start,
    LAST_DAY(CURRENT_DATE()) AS billing_period_end,
    'Credit Card' AS payment_method,
    'Completed' AS transaction_status,
    'USD' AS currency_code,
    2.40 AS tax_amount,
    0.00 AS discount_amount,
    CURRENT_DATE() AS load_date,
    CURRENT_DATE() AS update_date,
    'ZOOM_API' AS source_system
WHERE FALSE  -- This creates the table structure without data
