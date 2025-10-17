{{
  config(
    materialized='incremental',
    unique_key='ticket_id',
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

-- Support Tickets Silver Layer Transformation
WITH bronze_support_tickets AS (
  SELECT 
    ticket_id,
    user_id,
    ticket_type,
    resolution_status,
    open_date,
    load_timestamp,
    update_timestamp,
    source_system,
    ROW_NUMBER() OVER (
      PARTITION BY ticket_id 
      ORDER BY update_timestamp DESC, load_timestamp DESC
    ) as rn
  FROM {{ source('bronze', 'bz_support_tickets') }}
  WHERE ticket_id IS NOT NULL
    AND user_id IS NOT NULL
    AND ticket_type IS NOT NULL
    AND resolution_status IS NOT NULL
),

data_quality_checks AS (
  SELECT 
    *,
    CASE 
      WHEN ticket_id IS NULL THEN 0.0
      WHEN user_id IS NULL THEN 0.0
      WHEN ticket_type IS NULL THEN 0.0
      WHEN resolution_status IS NULL THEN 0.0
      WHEN ticket_type NOT IN ('Audio Issue', 'Video Issue', 'Connectivity', 'Billing Inquiry', 'Feature Request', 'Account Access') THEN 0.8
      WHEN resolution_status NOT IN ('Open', 'In Progress', 'Pending Customer', 'Closed', 'Resolved') THEN 0.8
      ELSE 1.0
    END as data_quality_score,
    
    CASE 
      WHEN ticket_id IS NULL OR user_id IS NULL OR ticket_type IS NULL OR resolution_status IS NULL THEN 'error'
      ELSE 'active'
    END as record_status
  FROM bronze_support_tickets
  WHERE rn = 1
),

final_transformation AS (
  SELECT 
    ticket_id,
    user_id,
    CASE 
      WHEN UPPER(TRIM(ticket_type)) LIKE '%AUDIO%' THEN 'Audio Issue'
      WHEN UPPER(TRIM(ticket_type)) LIKE '%VIDEO%' THEN 'Video Issue'
      WHEN UPPER(TRIM(ticket_type)) LIKE '%CONNECT%' THEN 'Connectivity'
      WHEN UPPER(TRIM(ticket_type)) LIKE '%BILLING%' THEN 'Billing Inquiry'
      WHEN UPPER(TRIM(ticket_type)) LIKE '%FEATURE%' THEN 'Feature Request'
      WHEN UPPER(TRIM(ticket_type)) LIKE '%ACCOUNT%' THEN 'Account Access'
      ELSE TRIM(ticket_type)
    END as ticket_type,
    CASE 
      WHEN UPPER(TRIM(resolution_status)) = 'OPEN' THEN 'Open'
      WHEN UPPER(TRIM(resolution_status)) LIKE '%PROGRESS%' THEN 'In Progress'
      WHEN UPPER(TRIM(resolution_status)) LIKE '%PENDING%' THEN 'Pending Customer'
      WHEN UPPER(TRIM(resolution_status)) = 'CLOSED' THEN 'Closed'
      WHEN UPPER(TRIM(resolution_status)) = 'RESOLVED' THEN 'Resolved'
      ELSE TRIM(resolution_status)
    END as resolution_status,
    open_date,
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
