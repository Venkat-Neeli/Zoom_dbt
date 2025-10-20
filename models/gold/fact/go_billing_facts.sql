{{
  config(
    materialized='incremental',
    unique_key='billing_fact_id',
    on_schema_change='sync_all_columns',
    pre_hook="INSERT INTO {{ target.schema }}.audit_log (table_name, operation, start_time, user_name) VALUES ('go_billing_facts', 'transform_start', CURRENT_TIMESTAMP(), CURRENT_USER())",
    post_hook="INSERT INTO {{ target.schema }}.audit_log (table_name, operation, end_time, user_name, records_processed) VALUES ('go_billing_facts', 'transform_end', CURRENT_TIMESTAMP(), CURRENT_USER(), (SELECT COUNT(*) FROM {{ this }}))"
  )
}}

WITH billing_base AS (
  SELECT 
    event_id,
    user_id,
    TRIM(event_type) AS event_type,
    COALESCE(amount, 0) AS amount,
    CONVERT_TIMEZONE('UTC', event_date) AS event_date,
    load_timestamp,
    update_timestamp,
    source_system,
    load_date,
    update_date,
    data_quality_score
  FROM {{ source('silver', 'si_billing_events') }}
  WHERE record_status = 'ACTIVE'
    AND data_quality_score >= {{ var('min_quality_score') }}
    AND event_id IS NOT NULL
    {% if is_incremental() %}
      AND update_timestamp > (SELECT MAX(update_date) FROM {{ this }})
    {% endif %}
),

user_org AS (
  SELECT 
    user_id,
    COALESCE(company, 'Unknown') AS organization_name
  FROM {{ source('silver', 'si_users') }}
  WHERE record_status = 'ACTIVE'
    AND data_quality_score >= {{ var('min_quality_score') }}
)

SELECT 
  {{ dbt_utils.generate_surrogate_key(['bb.event_id', 'bb.user_id']) }} AS billing_fact_id,
  bb.event_id,
  bb.user_id,
  {{ dbt_utils.generate_surrogate_key(['uo.organization_name']) }} AS organization_id,
  bb.event_type,
  bb.amount,
  bb.event_date,
  DATE_TRUNC('month', bb.event_date) AS billing_period_start,
  LAST_DAY(bb.event_date) AS billing_period_end,
  'Credit Card' AS payment_method,
  CASE 
    WHEN bb.amount > 0 THEN 'Completed'
    ELSE 'Pending'
  END AS transaction_status,
  'USD' AS currency_code,
  ROUND(bb.amount * 0.08, 2) AS tax_amount,
  0 AS discount_amount,
  bb.load_date,
  bb.update_date,
  bb.source_system,
  {{ dbt_utils.generate_surrogate_key(['bb.event_id', 'bb.user_id']) }} AS surrogate_key
FROM billing_base bb
LEFT JOIN user_org uo ON bb.user_id = uo.user_id
