{{ config(
    materialized='incremental',
    unique_key='license_id',
    on_schema_change='fail'
) }}

-- Transform bronze licenses to silver with data quality checks
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
    FROM {{ source('bronze', 'bz_licenses') }}
    WHERE license_id IS NOT NULL
),

-- Data quality validation and transformation
validated_licenses AS (
    SELECT 
        license_id,
        CASE 
            WHEN license_type IN ('Pro', 'Business', 'Enterprise', 'Education') THEN license_type
            ELSE 'Pro' -- Default standardization
        END AS license_type,
        assigned_to_user_id,
        start_date,
        end_date,
        load_timestamp,
        update_timestamp,
        source_system,
        -- Data quality score calculation
        CASE 
            WHEN license_type IN ('Pro', 'Business', 'Enterprise', 'Education')
                AND start_date IS NOT NULL 
                AND end_date IS NOT NULL
                AND end_date > start_date
            THEN 1.00
            ELSE 0.75
        END AS data_quality_score,
        -- Record status
        CASE 
            WHEN start_date IS NULL OR end_date IS NULL THEN 'error'
            WHEN end_date <= start_date THEN 'error'
            ELSE 'active'
        END AS record_status
    FROM bronze_licenses
    WHERE rn = 1  -- Deduplication: keep latest record
        AND start_date IS NOT NULL
        AND end_date IS NOT NULL
        AND end_date > start_date
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
FROM validated_licenses

{% if is_incremental() %}
    WHERE update_timestamp > (SELECT MAX(update_timestamp) FROM {{ this }})
{% endif %}
