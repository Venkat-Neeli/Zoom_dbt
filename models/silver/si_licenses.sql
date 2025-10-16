{{ config(
    materialized='incremental',
    unique_key='license_id',
    on_schema_change='fail'
) }}

-- Silver Licenses Table Transformation
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
            ORDER BY update_timestamp DESC, 
                     load_timestamp DESC
        ) AS row_num
    FROM {{ source('bronze', 'bz_licenses') }}
    WHERE license_id IS NOT NULL
),

data_quality_checks AS (
    SELECT 
        *,
        -- Calculate data quality score
        CASE 
            WHEN license_id IS NULL THEN 0.0
            WHEN license_type IS NULL OR TRIM(license_type) = '' THEN 0.2
            WHEN license_type NOT IN ('Pro', 'Business', 'Enterprise', 'Education') THEN 0.3
            WHEN start_date IS NULL OR end_date IS NULL THEN 0.4
            WHEN end_date <= start_date THEN 0.5
            ELSE 1.0
        END AS data_quality_score,
        
        -- Set record status
        CASE 
            WHEN license_id IS NULL 
                 OR license_type IS NULL OR TRIM(license_type) = ''
                 OR start_date IS NULL OR end_date IS NULL 
                 OR end_date <= start_date THEN 'error'
            ELSE 'active'
        END AS record_status
    FROM bronze_licenses
    WHERE row_num = 1
),

final_transform AS (
    SELECT 
        license_id,
        CASE 
            WHEN UPPER(TRIM(license_type)) = 'PRO' THEN 'Pro'
            WHEN UPPER(TRIM(license_type)) = 'BUSINESS' THEN 'Business'
            WHEN UPPER(TRIM(license_type)) = 'ENTERPRISE' THEN 'Enterprise'
            WHEN UPPER(TRIM(license_type)) = 'EDUCATION' THEN 'Education'
            ELSE TRIM(license_type)
        END AS license_type,
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
    FROM data_quality_checks
    WHERE record_status = 'active'  -- Only pass clean records to Silver
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
FROM final_transform

{% if is_incremental() %}
    WHERE update_timestamp > (SELECT COALESCE(MAX(update_timestamp), '1900-01-01') FROM {{ this }})
{% endif %}
