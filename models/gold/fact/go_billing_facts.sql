{{ config(
    materialized='incremental',
    unique_key='billing_fact_id',
    on_schema_change='sync_all_columns',
    pre_hook="INSERT INTO {{ ref('go_process_audit') }} (execution_id, pipeline_name, start_time, status, process_type, source_system, target_system, user_executed) VALUES (UUID_STRING(), 'go_billing_facts_load', CURRENT_TIMESTAMP(), 'STARTED', 'FACT_LOAD', 'SILVER', 'GOLD', CURRENT_USER()) WHERE '{{ this.name }}' != 'go_process_audit'",
    post_hook="INSERT INTO {{ ref('go_process_audit') }} (execution_id, pipeline_name, end_time, status, process_type, source_system, target_system, user_executed, records_processed) VALUES (UUID_STRING(), 'go_billing_facts_load', CURRENT_TIMESTAMP(), 'COMPLETED', 'FACT_LOAD', 'SILVER', 'GOLD', CURRENT_USER(), (SELECT COUNT(*) FROM {{ this }})) WHERE '{{ this.name }}' != 'go_process_audit'"
) }}

WITH billing_base AS (
    SELECT 
        event_id,
        user_id,
        event_type,
        amount,
        event_date,
        load_date,
        update_date,
        source_system
    FROM {{ ref('si_billing_events') }}
    WHERE record_status = 'ACTIVE'
    {% if is_incremental() %}
        AND update_date > (SELECT MAX(update_date) FROM {{ this }})
    {% endif %}
),

user_org_mapping AS (
    SELECT 
        user_id,
        COALESCE(company, 'INDIVIDUAL') AS organization_id
    FROM {{ ref('si_users') }}
    WHERE record_status = 'ACTIVE'
),

final_transform AS (
    SELECT 
        CONCAT('BF_', bb.event_id, '_', bb.user_id) AS billing_fact_id,
        bb.event_id,
        bb.user_id,
        uom.organization_id,
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
    LEFT JOIN user_org_mapping uom ON bb.user_id = uom.user_id
)

SELECT * FROM final_transform
