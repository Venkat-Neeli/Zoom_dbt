{{
    config(
        materialized='incremental',
        unique_key='license_id',
        on_schema_change='fail',
        pre_hook="{% if this.name != 'si_process_audit' %}INSERT INTO {{ ref('si_process_audit') }} (execution_id, pipeline_name, start_time, status, source_system, target_system, process_type, load_date, update_date) SELECT '{{ invocation_id }}', 'si_licenses_transform', current_timestamp(), 'STARTED', 'BRONZE', 'SILVER', 'ETL', current_date(), current_date(){% endif %}",
        post_hook="{% if this.name != 'si_process_audit' %}INSERT INTO {{ ref('si_process_audit') }} (execution_id, pipeline_name, end_time, status, records_processed, source_system, target_system, process_type, load_date, update_date) SELECT '{{ invocation_id }}', 'si_licenses_transform', current_timestamp(), 'COMPLETED', (SELECT count(*) FROM {{ this }}), 'BRONZE', 'SILVER', 'ETL', current_date(), current_date(){% endif %}"
    )
}}

-- Transform bronze licenses to silver licenses with data quality checks
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
        ROW_NUMBER() OVER (PARTITION BY license_id ORDER BY update_timestamp DESC, load_timestamp DESC) as rn
    FROM {{ ref('bz_licenses') }}
    WHERE license_id IS NOT NULL
        AND license_type IS NOT NULL
        AND start_date IS NOT NULL
        AND end_date IS NOT NULL
),

-- Data quality validation and cleansing
cleaned_licenses AS (
    SELECT
        license_id,
        CASE 
            WHEN license_type IN ('Pro', 'Business', 'Enterprise', 'Education') THEN license_type
            ELSE 'Pro'
        END AS license_type,
        assigned_to_user_id,
        start_date,
        end_date,
        load_timestamp,
        update_timestamp,
        source_system,
        -- Calculate data quality score
        CASE 
            WHEN start_date < end_date 
                AND license_type IN ('Pro', 'Business', 'Enterprise', 'Education')
            THEN 1.00
            ELSE 0.50
        END AS data_quality_score,
        CASE 
            WHEN start_date < end_date
            THEN 'active'
            ELSE 'error'
        END AS record_status
    FROM bronze_licenses
    WHERE rn = 1  -- Deduplication: keep latest record
        AND start_date < end_date
)

SELECT
    license_id,
    license_type,
    assigned_to_user_id,
    start_date,
    end_date,
    load_timestamp,
    update_timestamp,
    source_system,
    DATE(load_timestamp) AS load_date,
    DATE(update_timestamp) AS update_date,
    data_quality_score,
    record_status
FROM cleaned_licenses

{% if is_incremental() %}
  WHERE update_timestamp > (SELECT COALESCE(MAX(update_timestamp), '1900-01-01') FROM {{ this }})
{% endif %}
