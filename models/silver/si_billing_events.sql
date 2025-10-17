{{
  config(
    materialized='incremental',
    unique_key='event_id',
    on_schema_change='sync_all_columns',
    post_hook="
      {% if this.name != 'si_process_audit' %}
        INSERT INTO {{ ref('si_process_audit') }} (
          execution_id, pipeline_name, start_time, end_time, status,
          records_processed, records_successful, records_failed,
          processing_duration_seconds, source_system, target_system,
          process_type, load_date, update_date
        )
        VALUES (
          '{{ invocation_id }}_{{ this.name }}', '{{ this.name }}', CURRENT_TIMESTAMP(),
          CURRENT_TIMESTAMP(), 'SUCCESS', 
          (SELECT COUNT(*) FROM {{ this }}), (SELECT COUNT(*) FROM {{ this }}), 0,
          0, 'BRONZE', 'SILVER', 'ETL_TRANSFORMATION', CURRENT_DATE(), CURRENT_DATE()
        )
      {% endif %}
    "
  )
}}

-- Billing Events Silver Layer Transformation
WITH bronze_billing_events AS (
  SELECT 
    event_id,
    user_id,
    event_type,
    amount,
    event_date,
    load_timestamp,
    update_timestamp,
    source_system,
    ROW_NUMBER() OVER (
      PARTITION BY event_id 
      ORDER BY update_timestamp DESC, load_timestamp DESC
    ) as rn
  FROM {{ source('bronze', 'bz_billing_events') }}
  WHERE event_id IS NOT NULL
    AND user_id IS NOT NULL
    AND event_type IS NOT NULL
    AND amount >= 0
),

data_quality_checks AS (
  SELECT 
    *,
    CASE 
      WHEN event_id IS NULL THEN 0.0
      WHEN user_id IS NULL THEN 0.0
      WHEN event_type IS NULL THEN 0.0
      WHEN amount < 0 THEN 0.5
      WHEN event_type NOT IN ('Subscription Fee', 'Subscription Renewal', 'Add-on Purchase', 'Refund') THEN 0.8
      ELSE 1.0
    END as data_quality_score,
    
    CASE 
      WHEN event_id IS NULL OR user_id IS NULL OR event_type IS NULL THEN 'error'
      WHEN amount < 0 THEN 'error'
      ELSE 'active'
    END as record_status
  FROM bronze_billing_events
  WHERE rn = 1
),

final_transformation AS (
  SELECT 
    event_id,
    user_id,
    CASE 
      WHEN UPPER(TRIM(event_type)) LIKE '%SUBSCRIPTION%FEE%' THEN 'Subscription Fee'
      WHEN UPPER(TRIM(event_type)) LIKE '%SUBSCRIPTION%RENEWAL%' THEN 'Subscription Renewal'
      WHEN UPPER(TRIM(event_type)) LIKE '%ADD%ON%' OR UPPER(TRIM(event_type)) LIKE '%ADDON%' THEN 'Add-on Purchase'
      WHEN UPPER(TRIM(event_type)) = 'REFUND' THEN 'Refund'
      ELSE TRIM(event_type)
    END as event_type,
    amount,
    event_date,
    load_timestamp,
    update_timestamp,
    source_system,
    DATE(load_timestamp) as load_date,
    DATE(update_timestamp) as update_date,
    data_quality_score,
    record_status
  FROM data_quality_checks
  WHERE record_status = 'active'
)

SELECT * FROM final_transformation

{% if is_incremental() %}
  WHERE update_timestamp > (SELECT MAX(update_timestamp) FROM {{ this }})
{% endif %}
