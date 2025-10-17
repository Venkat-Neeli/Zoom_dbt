{{ config(
    materialized='incremental',
    unique_key='execution_id',
    on_schema_change='sync_all_columns'
) }}

WITH audit_data AS (
    SELECT
        {{ dbt_utils.generate_surrogate_key(['current_timestamp()']) }} AS execution_id,
        'Bronze_to_Silver_Transform' AS pipeline_name,
        current_timestamp() AS start_time,
        current_timestamp() AS end_time,
        'RUNNING' AS status,
        CAST(NULL AS VARCHAR(500)) AS error_message,
        0 AS records_processed,
        0 AS records_successful,
        0 AS records_failed,
        0 AS processing_duration_seconds,
        'Zoom' AS source_system,
        'Silver' AS target_system,
        'ETL' AS process_type,
        'dbt_user' AS user_executed,
        'dbt_cloud' AS server_name,
        CAST(NULL AS NUMBER) AS memory_usage_mb,
        CAST(NULL AS NUMBER) AS cpu_usage_percent,
        current_date() AS load_date,
        current_date() AS update_date
)

SELECT * FROM audit_data

{% if is_incremental() %}
    WHERE start_time > (SELECT COALESCE(MAX(start_time), '1900-01-01'::timestamp) FROM {{ this }})
{% endif %}
