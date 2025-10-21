{{ config(
    materialized='incremental',
    on_schema_change='sync_all_columns',
    unique_key='billing_fact_id',
    pre_hook="INSERT INTO {{ ref('go_process_audit') }} (execution_id, pipeline_name, start_time, status, source_system, target_system, process_type, user_executed) SELECT UUID_STRING() AS execution_id, 'go_billing_facts' AS pipeline_name, CURRENT_TIMESTAMP() AS start_time, 'STARTED' AS status, 'SILVER' AS source_system, 'GOLD' AS target_system, 'FACT_LOAD' AS process_type, 'DBT_USER' AS user_executed WHERE '{{ this.name }}' != 'go_process_audit'",
    post_hook="INSERT INTO {{ ref('go_process_audit') }} (execution_id, pipeline_name, end_time, status, records_processed, source_system, target_system, process_type, user_executed) SELECT UUID_STRING() AS execution_id, 'go_billing_facts' AS pipeline_name, CURRENT_TIMESTAMP() AS end_time, 'COMPLETED' AS status, (SELECT COUNT(*) FROM {{ this }}) AS records_processed, 'SILVER' AS source_system, 'GOLD' AS target_system, 'FACT_LOAD' AS process_type, 'DBT_USER' AS user_executed WHERE '{{ this.name }}' != 'go_process_audit'"
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
        AND be.update_date > (SELECT MAX(update_date) FROM {{ this }})
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
