{{ config(
    materialized='table',
    cluster_by=['event_date', 'user_id'],
    pre_hook="INSERT INTO {{ ref('go_process_audit') }} (execution_id, pipeline_name, start_time, status, source_system, target_system, process_type, user_executed) VALUES (UUID_STRING(), 'go_billing_facts', CURRENT_TIMESTAMP(), 'STARTED', 'SILVER', 'GOLD', 'FACT_LOAD', CURRENT_USER()) WHERE '{{ this.name }}' != 'go_process_audit'",
    post_hook="UPDATE {{ ref('go_process_audit') }} SET end_time = CURRENT_TIMESTAMP(), status = 'COMPLETED', records_processed = (SELECT COUNT(*) FROM {{ this }}) WHERE pipeline_name = 'go_billing_facts' AND status = 'STARTED' AND '{{ this.name }}' != 'go_process_audit'"
) }}

WITH billing_base AS (
    SELECT 
        b.event_id,
        b.user_id,
        b.event_type,
        b.amount,
        b.event_date,
        b.load_date,
        b.update_date,
        b.source_system
    FROM {{ ref('si_billing_events') }} b
    WHERE b.record_status = 'ACTIVE'
),

user_organization AS (
    SELECT 
        u.user_id,
        COALESCE(u.company, 'INDIVIDUAL') AS organization_id
    FROM {{ ref('si_users') }} u
    WHERE u.record_status = 'ACTIVE'
),

final_billing_facts AS (
    SELECT 
        CONCAT('BF_', bb.event_id, '_', bb.user_id) AS billing_fact_id,
        bb.event_id,
        bb.user_id,
        uo.organization_id,
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
        bb.amount * 0.08 AS tax_amount,
        0.00 AS discount_amount,
        bb.load_date,
        CURRENT_DATE() AS update_date,
        bb.source_system
    FROM billing_base bb
    LEFT JOIN user_organization uo ON bb.user_id = uo.user_id
)

SELECT * FROM final_billing_facts
