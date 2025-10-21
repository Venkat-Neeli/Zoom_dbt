{{ config(
    materialized='incremental',
    on_schema_change='sync_all_columns',
    unique_key='billing_fact_id'
) }}

WITH billing_base AS (
    SELECT 
        be.event_id,
        be.user_id,
        be.event_type,
        be.amount,
        be.event_date,
        be.load_date,
        be.update_date,
        be.source_system,
        u.company AS organization_id
    FROM {{ ref('si_billing_events') }} be
    LEFT JOIN {{ ref('si_users') }} u ON be.user_id = u.user_id
    WHERE be.record_status = 'ACTIVE'
    {% if is_incremental() %}
        AND be.update_date > (SELECT COALESCE(MAX(update_date), '1900-01-01') FROM {{ this }})
    {% endif %}
),

final_billing_facts AS (
    SELECT 
        CONCAT('BF_', bb.event_id, '_', bb.user_id) AS billing_fact_id,
        bb.event_id,
        bb.user_id,
        COALESCE(bb.organization_id, 'INDIVIDUAL') AS organization_id,
        UPPER(TRIM(bb.event_type)) AS event_type,
        ROUND(bb.amount, 2) AS amount,
        bb.event_date,
        DATE_TRUNC('month', bb.event_date) AS billing_period_start,
        LAST_DAY(bb.event_date) AS billing_period_end,
        'Credit Card' AS payment_method,
        CASE WHEN bb.amount > 0 THEN 'Completed' ELSE 'Refunded' END AS transaction_status,
        'USD' AS currency_code,
        bb.amount * 0.08 AS tax_amount,
        0.00 AS discount_amount,
        bb.load_date,
        CURRENT_DATE() AS update_date,
        bb.source_system
    FROM billing_base bb
)

SELECT * FROM final_billing_facts
