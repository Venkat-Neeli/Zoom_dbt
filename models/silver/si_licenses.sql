{{
  config(
    materialized='incremental',
    unique_key='license_id',
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

-- Licenses Silver Layer Transformation
WITH bronze_licenses AS (
  SELECT 
    license_id,
    license_type,
    assigned_to_user_id,
    start_date,
    end_date,
    load_timestamp,
    update_timestamp,
    source_system,
    ROW_NUMBER() OVER (
      PARTITION BY license_id 
      ORDER BY update_timestamp DESC, load_timestamp DESC
    ) as rn
  FROM {{ source('bronze', 'bz_licenses') }}
  WHERE license_id IS NOT NULL
    AND license_type IS NOT NULL
    AND start_date IS NOT NULL
    AND end_date IS NOT NULL
    AND start_date < end_date
),

data_quality_checks AS (
  SELECT 
    *,
    CASE 
      WHEN license_id IS NULL THEN 0.0
      WHEN license_type IS NULL THEN 0.0
      WHEN start_date IS NULL OR end_date IS NULL THEN 0.0
      WHEN start_date >= end_date THEN 0.3
      WHEN license_type NOT IN ('Pro', 'Business', 'Enterprise', 'Education') THEN 0.8
      ELSE 1.0
    END as data_quality_score,
    
    CASE 
      WHEN license_id IS NULL OR license_type IS NULL OR start_date IS NULL OR end_date IS NULL THEN 'error'
      WHEN start_date >= end_date THEN 'error'
      ELSE 'active'
    END as record_status
  FROM bronze_licenses
  WHERE rn = 1
),

final_transformation AS (
  SELECT 
    license_id,
    CASE 
      WHEN UPPER(TRIM(license_type)) = 'PRO' THEN 'Pro'
      WHEN UPPER(TRIM(license_type)) = 'BUSINESS' THEN 'Business'
      WHEN UPPER(TRIM(license_type)) = 'ENTERPRISE' THEN 'Enterprise'
      WHEN UPPER(TRIM(license_type)) = 'EDUCATION' THEN 'Education'
      ELSE TRIM(license_type)
    END as license_type,
    assigned_to_user_id,
    start_date,
    end_date,
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
