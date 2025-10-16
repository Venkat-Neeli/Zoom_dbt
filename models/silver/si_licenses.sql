{{ config(
    materialized='incremental',
    unique_key='license_id',
    on_schema_change='fail'
) }}

WITH bronze_licenses AS (
    SELECT *
    FROM {{ source('bronze', 'bz_licenses') }}
    {% if is_incremental() %}
        WHERE update_timestamp > (SELECT MAX(update_timestamp) FROM {{ this }})
    {% endif %}
),

-- Data Quality and Deduplication
deduped_licenses AS (
    SELECT *,
        ROW_NUMBER() OVER (
            PARTITION BY license_id 
            ORDER BY update_timestamp DESC, 
                     load_timestamp DESC,
                     CASE WHEN license_type IS NOT NULL THEN 1 ELSE 0 END +
                     CASE WHEN assigned_to_user_id IS NOT NULL THEN 1 ELSE 0 END +
                     CASE WHEN start_date IS NOT NULL THEN 1 ELSE 0 END +
                     CASE WHEN end_date IS NOT NULL THEN 1 ELSE 0 END DESC
        ) AS row_rank
    FROM bronze_licenses
),

-- Data Quality Checks and Transformations
transformed_licenses AS (
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
        DATE(load_timestamp) AS load_date,
        DATE(update_timestamp) AS update_date,
        
        -- Data Quality Score Calculation
        CASE 
            WHEN license_id IS NULL OR license_type IS NULL OR start_date IS NULL OR end_date IS NULL THEN 0.0
            WHEN end_date <= start_date THEN 0.3
            WHEN license_type NOT IN ('Pro', 'Business', 'Enterprise', 'Education') THEN 0.7
            ELSE 1.0
        END AS data_quality_score,
        
        -- Record Status
        CASE 
            WHEN license_id IS NULL OR license_type IS NULL OR start_date IS NULL OR end_date IS NULL THEN 'error'
            WHEN end_date <= start_date THEN 'error'
            ELSE 'active'
        END AS record_status
        
    FROM deduped_licenses
    WHERE row_rank = 1
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
FROM transformed_licenses
WHERE record_status = 'active'
  AND license_id IS NOT NULL
  AND license_type IS NOT NULL
  AND start_date IS NOT NULL
  AND end_date IS NOT NULL
  AND end_date > start_date
