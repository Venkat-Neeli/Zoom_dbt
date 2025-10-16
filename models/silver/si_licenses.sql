{{ config(
    materialized='incremental',
    unique_key='license_id',
    on_schema_change='fail',
    tags=['silver', 'licenses'],
    pre_hook="INSERT INTO {{ ref('si_process_audit') }} (execution_id, pipeline_name, start_time, status, source_system, target_system, process_type, load_date, update_date) SELECT '{{ dbt_utils.generate_surrogate_key(['CURRENT_TIMESTAMP()', 'RANDOM()']) }}', 'si_licenses_transformation', CURRENT_TIMESTAMP(), 'STARTED', 'BRONZE', 'SILVER', 'ETL', CURRENT_DATE(), CURRENT_DATE() WHERE '{{ this.name }}' != 'si_process_audit'",
    post_hook="INSERT INTO {{ ref('si_process_audit') }} (execution_id, pipeline_name, end_time, status, records_processed, source_system, target_system, process_type, load_date, update_date) SELECT '{{ dbt_utils.generate_surrogate_key(['CURRENT_TIMESTAMP()', 'RANDOM()']) }}', 'si_licenses_transformation', CURRENT_TIMESTAMP(), 'COMPLETED', (SELECT COUNT(*) FROM {{ this }}), 'BRONZE', 'SILVER', 'ETL', CURRENT_DATE(), CURRENT_DATE() WHERE '{{ this.name }}' != 'si_process_audit'"
) }}

WITH bronze_licenses AS (
    SELECT 
        bl.license_id,
        bl.license_type,
        bl.assigned_to_user_id,
        bl.start_date,
        bl.end_date,
        bl.load_timestamp,
        bl.update_timestamp,
        bl.source_system,
        ROW_NUMBER() OVER (
            PARTITION BY bl.license_id 
            ORDER BY bl.update_timestamp DESC, 
                     bl.load_timestamp DESC
        ) AS row_num
    FROM {{ source('bronze', 'bz_licenses') }} bl
    WHERE bl.license_id IS NOT NULL
      AND bl.license_type IS NOT NULL
      AND bl.start_date IS NOT NULL
      AND bl.end_date IS NOT NULL
      AND bl.end_date > bl.start_date
      AND bl.license_type IN ('Pro', 'Business', 'Enterprise', 'Education')
),

cleaned_licenses AS (
    SELECT 
        license_id,
        CASE 
            WHEN license_type = 'Basic' THEN 'Pro'
            WHEN license_type = 'Standard' THEN 'Pro'
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
        -- Data Quality Score
        CASE 
            WHEN license_id IS NOT NULL 
                 AND license_type IS NOT NULL 
                 AND start_date IS NOT NULL 
                 AND end_date IS NOT NULL 
                 AND end_date > start_date
            THEN 1.0
            ELSE 0.8
        END AS data_quality_score,
        'active' AS record_status
    FROM bronze_licenses
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
FROM cleaned_licenses

{% if is_incremental() %}
  WHERE update_timestamp > (SELECT MAX(update_timestamp) FROM {{ this }})
{% endif %}
