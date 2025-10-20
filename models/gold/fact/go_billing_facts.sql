{{ config(
    materialized='incremental',
    unique_key='billing_fact_key',
    on_schema_change='sync_all_columns',
    pre_hook="INSERT INTO {{ ref('go_process_audit') }} (process_name, start_time, status) VALUES ('go_billing_facts', CURRENT_TIMESTAMP(), 'STARTED')",
    post_hook="INSERT INTO {{ ref('go_process_audit') }} (process_name, end_time, status) VALUES ('go_billing_facts', CURRENT_TIMESTAMP(), 'COMPLETED')"
) }}

WITH billing_base AS (
    SELECT 
        event_id,
        user_id,
        event_type,
        amount,
        CONVERT_TIMEZONE('UTC', event_date) AS event_date_utc,
        load_date,
        update_date,
        source_system
    FROM {{ ref('si_billing_events') }}
    WHERE record_status = 'ACTIVE'
        AND data_quality_score >= 0.7
        {% if is_incremental() %}
        AND update_date > (SELECT MAX(update_date) FROM {{ this }})
        {% endif %}
),

user_info AS (
    SELECT 
        user_id,
        user_name,
        email,
        company,
        plan_type
    FROM {{ ref('si_users') }}
    WHERE record_status = 'ACTIVE'
        AND data_quality_score >= 0.7
),

license_info AS (
    SELECT 
        assigned_to_user_id,
        license_type,
        start_date,
        end_date
    FROM {{ ref('si_licenses') }}
    WHERE record_status = 'ACTIVE'
        AND data_quality_score >= 0.7
)

SELECT 
    UUID_STRING() AS billing_fact_key,
    bb.event_id,
    bb.user_id,
    ui.user_name,
    ui.email,
    ui.company,
    ui.plan_type,
    li.license_type,
    bb.event_type,
    bb.event_date_utc,
    COALESCE(bb.amount, 0) AS amount,
    CASE 
        WHEN bb.event_type IN ('PAYMENT', 'SUBSCRIPTION') THEN 'Revenue'
        WHEN bb.event_type IN ('REFUND', 'CHARGEBACK') THEN 'Loss'
        ELSE 'Other'
    END AS revenue_category,
    CASE 
        WHEN bb.amount > 1000 THEN 'High Value'
        WHEN bb.amount > 100 THEN 'Medium Value'
        ELSE 'Low Value'
    END AS transaction_value_category,
    EXTRACT(YEAR FROM bb.event_date_utc) AS event_year,
    EXTRACT(MONTH FROM bb.event_date_utc) AS event_month,
    EXTRACT(QUARTER FROM bb.event_date_utc) AS event_quarter,
    bb.load_date,
    bb.update_date,
    bb.source_system,
    CURRENT_TIMESTAMP() AS created_at
FROM billing_base bb
LEFT JOIN user_info ui ON bb.user_id = ui.user_id
LEFT JOIN license_info li ON bb.user_id = li.assigned_to_user_id
