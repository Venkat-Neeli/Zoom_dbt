{{ config(
    materialized='incremental',
    on_schema_change='sync_all_columns',
    unique_key='billing_fact_id',
    pre_hook="INSERT INTO {{ ref('go_process_audit') }} (execution_id, pipeline_name, start_time, status, process_type, user_executed, load_date) SELECT UUID_STRING(), 'go_billing_facts', CURRENT_TIMESTAMP(), 'STARTED', 'FACT_LOAD', 'DBT_USER', CURRENT_DATE() WHERE '{{ this.name }}' != 'go_process_audit'",
    post_hook="INSERT INTO {{ ref('go_process_audit') }} (execution_id, pipeline_name, end_time, status, records_processed, process_type, user_executed, load_date) SELECT UUID_STRING(), 'go_billing_facts', CURRENT_TIMESTAMP(), 'COMPLETED', (SELECT COUNT(*) FROM {{ this }}), 'FACT_LOAD', 'DBT_USER', CURRENT_DATE() WHERE '{{ this.name }}' != 'go_process_audit'"
) }}

WITH billing_base AS (
    SELECT 
        be.event_id,
        be.user_id,
        be.event_type,
        be.amount,
        be.event_date,
        be.load_timestamp,
        be.update_timestamp,
        be.source_system,
        be.load_date,
        be.update_date,
        be.data_quality_score,
        be.record_status
    FROM {{ ref('si_billing_events') }} be
    WHERE be.record_status = 'ACTIVE'
        AND be.data_quality_score >= 0.7
        {% if is_incremental() %}
        AND be.update_date > (SELECT MAX(update_date) FROM {{ this }})
        {% endif %}
),

user_organization AS (
    SELECT 
        user_id,
        company AS organization_id
    FROM {{ ref('si_users') }}
    WHERE record_status = 'ACTIVE'
),

final_billing_facts AS (
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
        ROUND(bb.amount * 0.08, 2) AS tax_amount,
        0.00 AS discount_amount,
        bb.load_date,
        CURRENT_DATE() AS update_date,
        bb.source_system
    FROM billing_base bb
    LEFT JOIN user_organization uo ON bb.user_id = uo.user_id
)

SELECT * FROM final_billing_facts
