{{
  config(
    materialized='incremental',
    unique_key='billing_fact_key',
    on_schema_change='fail',
    tags=['gold', 'fact_table']
  )
}}

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
    data_quality_score,
    record_status
  FROM {{ ref('si_billing_events') }}
  WHERE record_status = 'ACTIVE'
    AND data_quality_score >= 0.8
    {% if is_incremental() %}
      AND update_timestamp > (SELECT COALESCE(MAX(last_updated), '1900-01-01') FROM {{ this }})
    {% endif %}
),

user_context AS (
  SELECT 
    user_id,
    user_name,
    company,
    plan_type
  FROM {{ ref('si_users') }}
  WHERE record_status = 'ACTIVE'
),

license_context AS (
  SELECT 
    assigned_to_user_id,
    license_type,
    start_date,
    end_date
  FROM {{ ref('si_licenses') }}
  WHERE record_status = 'ACTIVE'
)

SELECT 
  {{ dbt_utils.generate_surrogate_key(['b.event_id', 'b.user_id', 'b.event_date']) }} as billing_fact_key,
  b.event_id,
  b.user_id,
  b.event_type,
  b.amount,
  b.event_date,
  CASE 
    WHEN b.amount > 100 THEN 'High Value'
    WHEN b.amount > 50 THEN 'Medium Value'
    ELSE 'Low Value'
  END as amount_category,
  CASE 
    WHEN b.event_type IN ('subscription', 'upgrade') THEN 'Revenue'
    WHEN b.event_type IN ('refund', 'chargeback') THEN 'Loss'
    ELSE 'Other'
  END as revenue_impact,
  u.user_name,
  u.company,
  u.plan_type,
  l.license_type,
  b.data_quality_score,
  b.source_system,
  CURRENT_TIMESTAMP() as created_at,
  b.update_timestamp as last_updated,
  EXTRACT(YEAR FROM b.event_date) as billing_year,
  EXTRACT(MONTH FROM b.event_date) as billing_month,
  EXTRACT(QUARTER FROM b.event_date) as billing_quarter
FROM billing_base b
LEFT JOIN user_context u ON b.user_id = u.user_id
LEFT JOIN license_context l ON b.user_id = l.assigned_to_user_id
