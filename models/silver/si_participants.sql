{{
  config(
    materialized='incremental',
    unique_key='participant_id',
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

-- Participants Silver Layer Transformation
WITH bronze_participants AS (
  SELECT 
    bp.participant_id,
    bp.meeting_id,
    bp.user_id,
    bp.join_time,
    bp.leave_time,
    bp.load_timestamp,
    bp.update_timestamp,
    bp.source_system,
    ROW_NUMBER() OVER (
      PARTITION BY bp.participant_id 
      ORDER BY bp.update_timestamp DESC, bp.load_timestamp DESC
    ) as rn
  FROM {{ source('bronze', 'bz_participants') }} bp
  WHERE bp.participant_id IS NOT NULL
    AND bp.meeting_id IS NOT NULL
    AND bp.join_time IS NOT NULL
    AND bp.leave_time IS NOT NULL
    AND bp.join_time < bp.leave_time
),

data_quality_checks AS (
  SELECT 
    *,
    CASE 
      WHEN participant_id IS NULL THEN 0.0
      WHEN meeting_id IS NULL THEN 0.0
      WHEN join_time IS NULL OR leave_time IS NULL THEN 0.0
      WHEN join_time >= leave_time THEN 0.3
      ELSE 1.0
    END as data_quality_score,
    
    CASE 
      WHEN participant_id IS NULL OR meeting_id IS NULL OR join_time IS NULL OR leave_time IS NULL THEN 'error'
      WHEN join_time >= leave_time THEN 'error'
      ELSE 'active'
    END as record_status
  FROM bronze_participants
  WHERE rn = 1
),

final_transformation AS (
  SELECT 
    participant_id,
    meeting_id,
    user_id,
    join_time,
    leave_time,
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
