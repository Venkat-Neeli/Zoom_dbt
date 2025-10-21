SELECT 
    'BF_001' AS billing_fact_id,
    'EVENT_001' AS event_id,
    'USER_001' AS user_id,
    'ORG_001' AS organization_id,
    'SUBSCRIPTION' AS event_type,
    29.99 AS amount,
    '2024-01-01'::DATE AS event_date,
    '2024-01-01'::DATE AS billing_period_start,
    '2024-01-31'::DATE AS billing_period_end,
    'Credit Card' AS payment_method,
    'Completed' AS transaction_status,
    'USD' AS currency_code,
    2.40 AS tax_amount,
    0.00 AS discount_amount,
    '2024-01-01'::DATE AS load_date,
    '2024-01-01'::DATE AS update_date,
    'ZOOM_API' AS source_system
WHERE 1=0
