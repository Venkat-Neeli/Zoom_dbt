{{
  config(
    materialized='incremental',
    unique_key='webinar_id',
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

-- Webinars Silver Layer Transformation
WITH bronze_webinars AS (
  SELECT 
    webinar_id,
    host_id,
    webinar_topic,
    start_time,
    end_time,
    registrants,
    load_timestamp,
    update_timestamp,
    source_system,
    ROW_NUMBER() OVER (
      PARTITION BY webinar_id 
      ORDER BY update_timestamp DESC, load_timestamp DESC
    ) as rn
  FROM {{ source('bronze', 'bz_webinars') }}
  WHERE webinar_id IS NOT NULL
    AND host_id IS NOT NULL
    AND start_time IS NOT NULL
    AND end_time IS NOT NULL
    AND start_time < end_time
    AND registrants >= 0
),

data_quality_checks AS (
  SELECT 
    *,
    CASE 
      WHEN webinar_id IS NULL THEN 0.0
      WHEN host_id IS NULL THEN 0.0
      WHEN start_time IS NULL OR end_time IS NULL THEN 0.0
      WHEN start_time >= end_time THEN 0.3
      WHEN registrants < 0 THEN 0.7
      ELSE 1.0
    END as data_quality_score,
    
    CASE 
      WHEN webinar_id IS NULL OR host_id IS NULL OR start_time IS NULL OR end_time IS NULL THEN 'error'
      WHEN start_time >= end_time OR registrants < 0 THEN 'error'
      ELSE 'active'
    END as record_status
  FROM bronze_webinars
  WHERE rn = 1
),

final_transformation AS (
  SELECT 
    webinar_id,
    host_id,
    COALESCE(TRIM(webinar_topic), 'No Topic') as webinar_topic,
    start_time,
    end_time,
    registrants,
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
