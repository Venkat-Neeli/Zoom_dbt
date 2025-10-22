{{ config(
    materialized='incremental',
    unique_key='billing_fact_id',
    on_schema_change='sync_all_columns',
    pre_hook="INSERT INTO {{ ref('go_process_audit') }} (execution_id, pipeline_name, start_time, status, process_type, user_executed, source_system, target_system) VALUES (UUID_STRING(), 'go_billing_facts_load', CURRENT_TIMESTAMP(), 'STARTED', 'FACT_LOAD', 'DBT_USER', 'SILVER', 'GOLD') WHERE '{{ this.name }}' != 'go_process_audit'",
    post_hook="UPDATE {{ ref('go_process_audit') }} SET end_time = CURRENT_TIMESTAMP(), status = 'COMPLETED', records_processed = (SELECT COUNT(*) FROM {{ this }}) WHERE pipeline_name = 'go_billing_facts_load' AND status = 'STARTED' AND '{{ this.name }}' != 'go_process_audit'"
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
    WHERE record_status = 'ACTIVE'
        AND data_quality_score >= 0.7
        {% if is_incremental() %}
        AND update_date >= (SELECT MAX(update_date) FROM {{ this }})
        {% endif %}
),

user_organization AS (
    SELECT 
        user_id,
        company AS organization_id
    FROM {{ ref('si_users') }}
    WHERE record_status = 'ACTIVE'
),

final_facts AS (
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

SELECT * FROM final_facts
