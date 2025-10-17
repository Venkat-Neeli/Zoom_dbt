{{
  config(
    materialized='incremental',
    unique_key='execution_id',
    on_schema_change='sync_all_columns',
    pre_hook="
      {% if not is_incremental() %}
        CREATE TABLE IF NOT EXISTS {{ this }} (
          execution_id VARCHAR(255),
          pipeline_name VARCHAR(255),
          start_time TIMESTAMP_NTZ,
          end_time TIMESTAMP_NTZ,
          status VARCHAR(50),
          error_message VARCHAR(1000),
          records_processed NUMBER,
          records_successful NUMBER,
          records_failed NUMBER,
          processing_duration_seconds NUMBER,
          source_system VARCHAR(255),
          target_system VARCHAR(255),
          process_type VARCHAR(100),
          user_executed VARCHAR(255),
          server_name VARCHAR(255),
          memory_usage_mb NUMBER,
          cpu_usage_percent NUMBER,
          load_date DATE,
          update_date DATE
        )
      {% endif %}
    "
  )
}}

-- Process Audit Table - This must be created first
SELECT 
  {{ dbt_utils.generate_surrogate_key(['invocation_id', 'run_started_at']) }} as execution_id,
  'INITIAL_SETUP' as pipeline_name,
  CURRENT_TIMESTAMP() as start_time,
  CURRENT_TIMESTAMP() as end_time,
  'SUCCESS' as status,
  NULL as error_message,
  0 as records_processed,
  0 as records_successful,
  0 as records_failed,
  0 as processing_duration_seconds,
  'SYSTEM' as source_system,
  'SILVER' as target_system,
  'INITIALIZATION' as process_type,
  'DBT_SYSTEM' as user_executed,
  'DBT_CLOUD' as server_name,
  NULL as memory_usage_mb,
  NULL as cpu_usage_percent,
  CURRENT_DATE() as load_date,
  CURRENT_DATE() as update_date

{% if is_incremental() %}
  WHERE FALSE  -- Only insert during initial creation
{% endif %}
