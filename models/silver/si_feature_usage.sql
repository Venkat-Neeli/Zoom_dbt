{{
  config(
    materialized='incremental',
    unique_key='usage_id',
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

-- Feature Usage Silver Layer Transformation
WITH bronze_feature_usage AS (
  SELECT 
    usage_id,
    meeting_id,
    feature_name,
    usage_count,
    usage_date,
    load_timestamp,
    update_timestamp,
    source_system,
    ROW_NUMBER() OVER (
      PARTITION BY usage_id 
      ORDER BY update_timestamp DESC, load_timestamp DESC
    ) as rn
  FROM {{ source('bronze', 'bz_feature_usage') }}
  WHERE usage_id IS NOT NULL
    AND meeting_id IS NOT NULL
    AND feature_name IS NOT NULL
    AND usage_count >= 0
),

data_quality_checks AS (
  SELECT 
    *,
    CASE 
      WHEN usage_id IS NULL THEN 0.0
      WHEN meeting_id IS NULL THEN 0.0
      WHEN feature_name IS NULL THEN 0.0
      WHEN usage_count < 0 THEN 0.5
      WHEN feature_name NOT IN ('Screen Sharing', 'Chat', 'Recording', 'Whiteboard', 'Virtual Background') THEN 0.8
      ELSE 1.0
    END as data_quality_score,
    
    CASE 
      WHEN usage_id IS NULL OR meeting_id IS NULL OR feature_name IS NULL THEN 'error'
      WHEN usage_count < 0 THEN 'error'
      ELSE 'active'
    END as record_status
  FROM bronze_feature_usage
  WHERE rn = 1
),

final_transformation AS (
  SELECT 
    usage_id,
    meeting_id,
    CASE 
      WHEN UPPER(TRIM(feature_name)) LIKE '%SCREEN%SHAR%' THEN 'Screen Sharing'
      WHEN UPPER(TRIM(feature_name)) = 'CHAT' THEN 'Chat'
      WHEN UPPER(TRIM(feature_name)) LIKE '%RECORD%' THEN 'Recording'
      WHEN UPPER(TRIM(feature_name)) LIKE '%WHITEBOARD%' THEN 'Whiteboard'
      WHEN UPPER(TRIM(feature_name)) LIKE '%VIRTUAL%BACKGROUND%' THEN 'Virtual Background'
      ELSE TRIM(feature_name)
    END as feature_name,
    usage_count,
    usage_date,
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
