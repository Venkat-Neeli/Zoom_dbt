{{ config(
    materialized='table',
    cluster_by=['billing_date', 'user_id'],
    pre_hook="INSERT INTO {{ ref('go_process_audit') }} (process_name, table_name, start_time, status) VALUES ('go_billing_facts_transform', 'go_billing_facts', CURRENT_TIMESTAMP(), 'STARTED') WHERE '{{ this.name }}' != 'go_process_audit'",
    post_hook="INSERT INTO {{ ref('go_process_audit') }} (process_name, table_name, end_time, status, records_processed) VALUES ('go_billing_facts_transform', 'go_billing_facts', CURRENT_TIMESTAMP(), 'COMPLETED', (SELECT COUNT(*) FROM {{ this }})) WHERE '{{ this.name }}' != 'go_process_audit'"
) }}

WITH billing_base AS (
    SELECT 
        event_id,
        user_id,
        event_type,
        amount,
        currency,
        event_date,
        billing_period_start,
        billing_period_end,
        payment_method,
        transaction_status,
        created_at AS billing_created_at,
        updated_at AS billing_updated_at
    FROM {{ ref('si_billing_events') }}
    WHERE event_id IS NOT NULL
),

user_context AS (
    SELECT 
        user_id,
        user_name,
        email,
        company,
        plan_type
    FROM {{ ref('si_users') }}
    WHERE user_id IS NOT NULL
),

license_context AS (
    SELECT 
        assigned_to_user_id,
        license_type,
        start_date AS license_start_date,
        end_date AS license_end_date,
        license_status
    FROM {{ ref('si_licenses') }}
    WHERE assigned_to_user_id IS NOT NULL
),

user_billing_summary AS (
    SELECT 
        user_id,
        COUNT(*) AS total_billing_events,
        SUM(CASE WHEN event_type = 'PAYMENT' THEN amount ELSE 0 END) AS total_payments,
        SUM(CASE WHEN event_type = 'REFUND' THEN amount ELSE 0 END) AS total_refunds,
        MAX(event_date) AS last_billing_date
    FROM billing_base
    GROUP BY user_id
)

SELECT 
    {{ dbt_utils.generate_surrogate_key(['bb.event_id']) }} AS billing_fact_key,
    bb.event_id,
    bb.user_id,
    uc.user_name,
    uc.email,
    uc.company,
    uc.plan_type,
    lc.license_type,
    bb.event_type,
    bb.amount,
    bb.currency,
    DATE(bb.event_date) AS billing_date,
    bb.event_date,
    bb.billing_period_start,
    bb.billing_period_end,
    bb.payment_method,
    bb.transaction_status,
    DATEDIFF('day', bb.billing_period_start, bb.billing_period_end) AS billing_period_days,
    CASE 
        WHEN bb.billing_period_start IS NOT NULL AND bb.billing_period_end IS NOT NULL 
        THEN ROUND(bb.amount / NULLIF(DATEDIFF('day', bb.billing_period_start, bb.billing_period_end), 0), 2)
        ELSE bb.amount
    END AS daily_rate,
    ubs.total_billing_events,
    ubs.total_payments,
    ubs.total_refunds,
    ubs.total_payments - ubs.total_refunds AS net_revenue,
    CASE 
        WHEN bb.event_type = 'PAYMENT' AND bb.transaction_status = 'SUCCESS' THEN bb.amount
        ELSE 0
    END AS successful_payment_amount,
    CASE 
        WHEN bb.event_type = 'REFUND' AND bb.transaction_status = 'SUCCESS' THEN bb.amount
        ELSE 0
    END AS successful_refund_amount,
    CASE 
        WHEN bb.amount >= 1000 THEN 'High Value'
        WHEN bb.amount >= 500 THEN 'Medium Value'
        ELSE 'Low Value'
    END AS transaction_value_category,
    CASE 
        WHEN bb.transaction_status = 'SUCCESS' THEN 'Successful'
        WHEN bb.transaction_status = 'FAILED' THEN 'Failed'
        WHEN bb.transaction_status = 'PENDING' THEN 'Pending'
        ELSE 'Unknown'
    END AS transaction_status_category,
    CURRENT_TIMESTAMP() AS created_at,
    CURRENT_TIMESTAMP() AS updated_at,
    'SUCCESS' AS process_status
FROM billing_base bb
LEFT JOIN user_context uc ON bb.user_id = uc.user_id
LEFT JOIN license_context lc ON bb.user_id = lc.assigned_to_user_id
LEFT JOIN user_billing_summary ubs ON bb.user_id = ubs.user_id
