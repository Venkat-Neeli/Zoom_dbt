{{ config(
    materialized='table',
    cluster_by=['event_date', 'user_id'],
    tags=['fact', 'gold'],
    pre_hook="INSERT INTO {{ ref('go_process_audit') }} (execution_id, pipeline_name, process_type, start_time, status, source_system, target_system, user_executed, server_name, load_date, update_date) SELECT '{{ dbt_utils.generate_surrogate_key(['invocation_id', 'this.name']) }}', 'GO_BILLING_FACTS', 'FACT_BUILD', CURRENT_TIMESTAMP(), 'STARTED', 'SILVER', 'GOLD', CURRENT_USER(), 'DBT_CLOUD', CURRENT_DATE(), CURRENT_DATE() WHERE '{{ this.name }}' != 'go_process_audit'",
    post_hook="UPDATE {{ ref('go_process_audit') }} SET end_time = CURRENT_TIMESTAMP(), status = 'COMPLETED', records_processed = (SELECT COUNT(*) FROM {{ this }}), records_successful = (SELECT COUNT(*) FROM {{ this }}), processing_duration_seconds = DATEDIFF('second', start_time, CURRENT_TIMESTAMP()) WHERE execution_id = '{{ dbt_utils.generate_surrogate_key(['invocation_id', 'this.name']) }}' AND '{{ this.name }}' != 'go_process_audit'"
) }}

-- Gold Billing Facts Table
-- Comprehensive billing and financial transaction analytics

WITH billing_base AS (
    SELECT
        event_id,
        user_id,
        event_type,
        amount,
        event_date,
        source_system,
        load_date,
        update_date
    FROM {{ source('silver', 'si_billing_events') }}
    WHERE record_status = 'ACTIVE'
),

user_organizations AS (
    SELECT
        user_id,
        COALESCE(company, 'INDIVIDUAL') AS organization_id
    FROM {{ source('silver', 'si_users') }}
    WHERE record_status = 'ACTIVE'
),

billing_facts AS (
    SELECT
        CONCAT('BF_', bb.event_id, '_', bb.user_id) AS billing_fact_id,
        bb.event_id,
        bb.user_id,
        COALESCE(uo.organization_id, 'INDIVIDUAL') AS organization_id,
        UPPER(TRIM(bb.event_type)) AS event_type,
        ROUND(bb.amount, 2) AS amount,
        bb.event_date,
        DATE_TRUNC('month', bb.event_date) AS billing_period_start,
        LAST_DAY(bb.event_date) AS billing_period_end,
        'Credit Card' AS payment_method,
        CASE WHEN bb.amount > 0 THEN 'Completed' ELSE 'Refunded' END AS transaction_status,
        'USD' AS currency_code,
        ROUND(bb.amount * 0.08, 2) AS tax_amount,
        0.00 AS discount_amount,
        bb.load_date,
        CURRENT_DATE() AS update_date,
        bb.source_system
    FROM billing_base bb
    LEFT JOIN user_organizations uo ON bb.user_id = uo.user_id
)

SELECT
    billing_fact_id::VARCHAR(50) AS billing_fact_id,
    event_id::VARCHAR(50) AS event_id,
    user_id::VARCHAR(50) AS user_id,
    organization_id::VARCHAR(50) AS organization_id,
    event_type::VARCHAR(100) AS event_type,
    amount,
    event_date,
    billing_period_start,
    billing_period_end,
    payment_method::VARCHAR(50) AS payment_method,
    transaction_status::VARCHAR(50) AS transaction_status,
    currency_code::VARCHAR(10) AS currency_code,
    tax_amount,
    discount_amount,
    load_date,
    update_date,
    source_system::VARCHAR(100) AS source_system
FROM billing_facts
