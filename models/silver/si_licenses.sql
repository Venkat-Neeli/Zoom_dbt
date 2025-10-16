{{ config(
    materialized='incremental',
    unique_key='license_id',
    on_schema_change='fail',
    pre_hook="INSERT INTO {{ ref('si_process_audit') }} (execution_id, pipeline_name, start_time, status, source_system, target_system, process_type, user_executed, load_date, update_date) SELECT '{{ dbt_utils.generate_surrogate_key(['invocation_id', 'si_licenses']) }}', 'si_licenses', CURRENT_TIMESTAMP(), 'RUNNING', 'BRONZE', 'SILVER', 'ETL', CURRENT_USER(), CURRENT_DATE(), CURRENT_DATE() WHERE '{{ this.name }}' != 'si_process_audit'",
    post_hook="UPDATE {{ ref('si_process_audit') }} SET end_time = CURRENT_TIMESTAMP(), status = 'SUCCESS', records_processed = (SELECT COUNT(*) FROM {{ this }}), records_successful = (SELECT COUNT(*) FROM {{ this }} WHERE record_status = 'active'), processing_duration_seconds = DATEDIFF(second, start_time, CURRENT_TIMESTAMP()) WHERE execution_id = '{{ dbt_utils.generate_surrogate_key(['invocation_id', 'si_licenses']) }}' AND '{{ this.name }}' != 'si_process_audit'"
) }}

WITH bronze_licenses AS (
    SELECT *
    FROM {{ source('bronze', 'bz_licenses') }}
    {% if is_incremental() %}
        WHERE update_timestamp > (SELECT MAX(update_timestamp) FROM {{ this }})
    {% endif %}
),

-- Data Quality and Deduplication
cleaned_licenses AS (
    SELECT *,
        ROW_NUMBER() OVER (
            PARTITION BY license_id 
            ORDER BY update_timestamp DESC, 
                     load_timestamp DESC,
                     CASE WHEN license_type IS NOT NULL THEN 1 ELSE 0 END +
                     CASE WHEN assigned_to_user_id IS NOT NULL THEN 1 ELSE 0 END +
                     CASE WHEN start_date IS NOT NULL THEN 1 ELSE 0 END +
                     CASE WHEN end_date IS NOT NULL THEN 1 ELSE 0 END DESC,
                     license_id DESC
        ) AS row_num
    FROM bronze_licenses
    WHERE license_id IS NOT NULL
      AND license_type IS NOT NULL
      AND license_type IN ('Pro', 'Business', 'Enterprise', 'Education')
      AND start_date IS NOT NULL
      AND end_date IS NOT NULL
      AND end_date > start_date
),

-- Calculate Data Quality Score
final_licenses AS (
    SELECT 
        license_id,
        CASE 
            WHEN UPPER(TRIM(license_type)) = 'PRO' THEN 'Pro'
            WHEN UPPER(TRIM(license_type)) = 'BUSINESS' THEN 'Business'
            WHEN UPPER(TRIM(license_type)) = 'ENTERPRISE' THEN 'Enterprise'
            WHEN UPPER(TRIM(license_type)) = 'EDUCATION' THEN 'Education'
            ELSE license_type
        END AS license_type,
        assigned_to_user_id,
        start_date,
        end_date,
        load_timestamp,
        update_timestamp,
        source_system,
        DATE(load_timestamp) AS load_date,
        DATE(update_timestamp) AS update_date,
        -- Data Quality Score Calculation
        ROUND(
            (CASE WHEN license_id IS NOT NULL THEN 0.25 ELSE 0 END +
             CASE WHEN license_type IN ('Pro', 'Business', 'Enterprise', 'Education') THEN 0.25 ELSE 0 END +
             CASE WHEN start_date IS NOT NULL THEN 0.25 ELSE 0 END +
             CASE WHEN end_date IS NOT NULL AND end_date > start_date THEN 0.25 ELSE 0 END), 2
        ) AS data_quality_score,
        'active' AS record_status
    FROM cleaned_licenses
    WHERE row_num = 1
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
    load_date,
    update_date,
    data_quality_score,
    record_status
FROM final_licenses
