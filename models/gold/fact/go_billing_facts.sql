{{
  config(
    materialized='incremental',
    unique_key='billing_fact_id',
    on_schema_change='fail',
    cluster_by=['load_date'],
    pre_hook="INSERT INTO {{ ref('si_process_audit') }} (execution_id, pipeline_name, status, start_time) VALUES (UUID_STRING(), 'go_billing_facts', 'STARTED', CURRENT_TIMESTAMP())",
    post_hook="INSERT INTO {{ ref('si_process_audit') }} (execution_id, pipeline_name, status, end_time) VALUES (UUID_STRING(), 'go_billing_facts', 'COMPLETED', CURRENT_TIMESTAMP())"
  )
}}

WITH billing_base AS (
    SELECT 
        b.event_id,
        b.user_id,
        b.event_type,
        b.amount,
        b.event_date,
        b.load_timestamp,
        b.update_timestamp,
        b.source_system,
        b.load_date,
        b.update_date,
        b.data_quality_score,
        b.record_status
    FROM {{ ref('si_billing_events') }} b
    WHERE b.record_status = 'ACTIVE'
        AND b.data_quality_score >= {{ var('min_quality_score') }}
        AND b.amount IS NOT NULL
    {% if is_incremental() %}
        AND b.load_date >= (SELECT MAX(load_date) FROM {{ this }})
    {% endif %}
),

user_org_mapping AS (
    SELECT 
        u.user_id,
        COALESCE(u.company, 'UNKNOWN') AS organization_id
    FROM {{ ref('si_users') }} u
    WHERE u.record_status = 'ACTIVE'
)

SELECT 
    UUID_STRING() AS billing_fact_id,
    b.event_id,
    b.user_id,
    COALESCE(u.organization_id, 'UNKNOWN') AS organization_id,
    UPPER(TRIM(b.event_type)) AS event_type,
    b.amount,
    CONVERT_TIMEZONE('{{ var("default_timezone") }}', b.event_date) AS event_date,
    DATE_TRUNC('month', b.event_date) AS billing_period_start,
    LAST_DAY(b.event_date) AS billing_period_end,
    'CREDIT_CARD' AS payment_method,
    CASE 
        WHEN b.amount > 0 THEN 'COMPLETED'
        ELSE 'PENDING'
    END AS transaction_status,
    'USD' AS currency_code,
    ROUND(b.amount * 0.08, 2) AS tax_amount,
    0.00 AS discount_amount,
    b.load_date,
    CURRENT_TIMESTAMP() AS created_at,
    CURRENT_TIMESTAMP() AS updated_at
FROM billing_base b
LEFT JOIN user_org_mapping u ON b.user_id = u.user_id
