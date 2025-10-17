{{
  config(
    materialized='incremental',
    unique_key='user_id',
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

-- Users Silver Layer Transformation
WITH bronze_users AS (
  SELECT 
    user_id,
    user_name,
    email,
    company,
    plan_type,
    load_timestamp,
    update_timestamp,
    source_system,
    ROW_NUMBER() OVER (
      PARTITION BY user_id 
      ORDER BY update_timestamp DESC, load_timestamp DESC
    ) as rn
  FROM {{ source('bronze', 'bz_users') }}
  WHERE user_id IS NOT NULL
    AND user_name IS NOT NULL
    AND email IS NOT NULL
    AND email LIKE '%@%.%'  -- Basic email validation
),

data_quality_checks AS (
  SELECT 
    *,
    CASE 
      WHEN user_id IS NULL THEN 0.0
      WHEN user_name IS NULL OR TRIM(user_name) = '' THEN 0.2
      WHEN email IS NULL OR NOT (email LIKE '%@%.%') THEN 0.0
      WHEN plan_type NOT IN ('Free', 'Pro', 'Business', 'Enterprise') THEN 0.8
      ELSE 1.0
    END as data_quality_score,
    
    CASE 
      WHEN user_id IS NULL OR email IS NULL OR user_name IS NULL THEN 'error'
      ELSE 'active'
    END as record_status
  FROM bronze_users
  WHERE rn = 1  -- Deduplication: keep latest record
),

final_transformation AS (
  SELECT 
    user_id,
    TRIM(user_name) as user_name,
    LOWER(TRIM(email)) as email,
    COALESCE(TRIM(company), 'Unknown') as company,
    CASE 
      WHEN UPPER(plan_type) IN ('FREE', 'BASIC') THEN 'Free'
      WHEN UPPER(plan_type) = 'PRO' THEN 'Pro'
      WHEN UPPER(plan_type) = 'BUSINESS' THEN 'Business'
      WHEN UPPER(plan_type) = 'ENTERPRISE' THEN 'Enterprise'
      ELSE 'Free'
    END as plan_type,
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
