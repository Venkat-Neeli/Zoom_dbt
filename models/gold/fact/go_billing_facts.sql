{{ config(
    materialized='table',
    cluster_by=['event_date', 'user_id'],
    pre_hook="INSERT INTO {{ ref('go_process_audit') }} (process_name, table_name, start_time, status) VALUES ('go_billing_facts_transform', 'go_billing_facts', CURRENT_TIMESTAMP(), 'STARTED') WHERE '{{ this.name }}' != 'go_process_audit'",
    post_hook="INSERT INTO {{ ref('go_process_audit') }} (process_name, table_name, end_time, status, records_processed) VALUES ('go_billing_facts_transform', 'go_billing_facts', CURRENT_TIMESTAMP(), 'COMPLETED', (SELECT COUNT(*) FROM {{ this }})) WHERE '{{ this.name }}' != 'go_process_audit'"
) }}

WITH billing_base AS (
    SELECT 
        event_id,
        user_id,
        event_type,
        amount,
        event_date,
        load_timestamp,
        update_timestamp,
        source_system,
        load_date,
        update_date,
        data_quality_score,
        record_status
    FROM {{ ref('si_billing_events') }}
    WHERE event_id IS NOT NULL
        AND record_status = 'active'
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
        AND record_status = 'active'
),

license_context AS (
    SELECT 
        assigned_to_user_id,
        license_type,
        start_date AS license_start_date,
        end_date AS license_end_date
    FROM {{ ref('si_licenses') }}
    WHERE assigned_to_user_id IS NOT NULL
        AND record_status = 'active'
)

SELECT 
    CONCAT('BF_', bb.event_id, '_', bb.user_id) AS billing_fact_id,
    bb.event_id,
    bb.user_id,
    COALESCE(uc.company, 'INDIVIDUAL') AS organization_id,
    UPPER(TRIM(bb.event_type)) AS event_type,
    ROUND(bb.amount, 2) AS amount,
    bb.event_date,
    DATE_TRUNC('month', bb.event_date) AS billing_period_start,
    LAST_DAY(bb.event_date) AS billing_period_end,
    'Credit Card' AS payment_method,
    CASE 
        WHEN bb.amount > 0 THEN 'Completed'
        ELSE 'Refunded'
    END AS transaction_status,
    'USD' AS currency_code,
    ROUND(bb.amount * 0.08, 2) AS tax_amount,
    0.00 AS discount_amount,
    bb.load_date,
    CURRENT_DATE() AS update_date,
    bb.source_system
FROM billing_base bb
LEFT JOIN user_context uc ON bb.user_id = uc.user_id
LEFT JOIN license_context lc ON bb.user_id = lc.assigned_to_user_id
