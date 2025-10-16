{{ config(
    materialized='incremental',
    unique_key='user_id',
    on_schema_change='fail'
) }}

WITH bronze_users AS (
    SELECT *
    FROM {{ source('bronze', 'bz_users') }}
    {% if is_incremental() %}
        WHERE update_timestamp > (SELECT MAX(update_timestamp) FROM {{ this }})
    {% endif %}
),

-- Data Quality and Deduplication
deduped_users AS (
    SELECT *,
        ROW_NUMBER() OVER (
            PARTITION BY user_id 
            ORDER BY update_timestamp DESC, 
                     load_timestamp DESC,
                     CASE WHEN user_name IS NOT NULL THEN 1 ELSE 0 END +
                     CASE WHEN email IS NOT NULL THEN 1 ELSE 0 END +
                     CASE WHEN company IS NOT NULL THEN 1 ELSE 0 END +
                     CASE WHEN plan_type IS NOT NULL THEN 1 ELSE 0 END DESC
        ) AS row_rank
    FROM bronze_users
),

-- Data Quality Checks and Transformations
transformed_users AS (
    SELECT
        user_id,
        TRIM(user_name) AS user_name,
        LOWER(TRIM(email)) AS email,
        CASE 
            WHEN TRIM(company) = '' THEN '000'
            ELSE TRIM(company)
        END AS company,
        CASE 
            WHEN plan_type IN ('Free', 'Pro', 'Business', 'Enterprise') THEN plan_type
            ELSE 'Free'
        END AS plan_type,
        load_timestamp,
        update_timestamp,
        source_system,
        DATE(load_timestamp) AS load_date,
        DATE(update_timestamp) AS update_date,
        
        -- Data Quality Score Calculation
        CASE 
            WHEN user_id IS NULL OR user_name IS NULL OR email IS NULL THEN 0.0
            WHEN NOT REGEXP_LIKE(email, '^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\\.[A-Za-z]{2,}$') THEN 0.5
            WHEN plan_type NOT IN ('Free', 'Pro', 'Business', 'Enterprise') THEN 0.7
            ELSE 1.0
        END AS data_quality_score,
        
        -- Record Status
        CASE 
            WHEN user_id IS NULL OR user_name IS NULL OR email IS NULL THEN 'error'
            WHEN NOT REGEXP_LIKE(email, '^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\\.[A-Za-z]{2,}$') THEN 'error'
            ELSE 'active'
        END AS record_status
        
    FROM deduped_users
    WHERE row_rank = 1
)

SELECT 
    user_id,
    user_name,
    email,
    company,
    plan_type,
    load_timestamp,
    update_timestamp,
    source_system,
    load_date,
    update_date,
    data_quality_score,
    record_status
FROM transformed_users
WHERE record_status = 'active'
  AND user_id IS NOT NULL
  AND user_name IS NOT NULL
  AND email IS NOT NULL
