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
        load_date,
        source_system,
        ROW_NUMBER() OVER (PARTITION BY event_id ORDER BY update_timestamp DESC) as rn
    FROM {{ source('silver', 'si_billing_events') }}
    WHERE record_status = 'ACTIVE'
),

user_organizations AS (
    SELECT 
        user_id,
        COALESCE(company, 'INDIVIDUAL') as organization_id
    FROM {{ source('silver', 'si_users') }}
    WHERE record_status = 'ACTIVE'
),

final_billing_facts AS (
    SELECT 
        CONCAT('BF_', bb.event_id, '_', bb.user_id) as billing_fact_id,
        bb.event_id,
        bb.user_id,
        COALESCE(uo.organization_id, 'INDIVIDUAL') as organization_id,
        UPPER(TRIM(bb.event_type)) as event_type,
        ROUND(bb.amount, 2) as amount,
        bb.event_date,
        DATE_TRUNC('month', bb.event_date) as billing_period_start,
        LAST_DAY(bb.event_date) as billing_period_end,
        'Credit Card' as payment_method,
        CASE WHEN bb.amount > 0 THEN 'Completed' ELSE 'Refunded' END as transaction_status,
        'USD' as currency_code,
        ROUND(bb.amount * 0.08, 2) as tax_amount,
        0.00 as discount_amount,
        bb.load_date,
        CURRENT_DATE() as update_date,
        bb.source_system
    FROM billing_base bb
    LEFT JOIN user_organizations uo ON bb.user_id = uo.user_id
    WHERE bb.rn = 1
)

SELECT * FROM final_billing_facts
