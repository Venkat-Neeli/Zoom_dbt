{{ config(
    materialized='incremental',
    unique_key='user_id'
) }}

-- Data Quality and Transformation Logic for Users
WITH bronze_users AS (
    SELECT *
    FROM {{ source('bronze', 'bz_users') }}
    {% if is_incremental() %}
        WHERE update_timestamp > (SELECT MAX(update_timestamp) FROM {{ this }})
    {% endif %}
),

-- Data Quality Checks and Cleansing
cleansed_users AS (
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
            ELSE 'Free' -- Default standardization
        END AS plan_type,
        load_timestamp,
        update_timestamp,
        source_system,
        -- Derived columns
        DATE(load_timestamp) AS load_date,
        DATE(update_timestamp) AS update_date,
        -- Data Quality Score Calculation
        CASE 
            WHEN user_id IS NOT NULL 
                AND user_name IS NOT NULL 
                AND email IS NOT NULL 
                AND email RLIKE '^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\\.[A-Za-z]{2,}$'
                AND plan_type IN ('Free', 'Pro', 'Business', 'Enterprise')
            THEN 1.00
            ELSE 0.50
        END AS data_quality_score,
        -- Record Status
        CASE 
            WHEN user_id IS NULL OR user_name IS NULL OR email IS NULL THEN 'error'
            ELSE 'active'
        END AS record_status,
        -- Deduplication ranking
        ROW_NUMBER() OVER (
            PARTITION BY user_id 
            ORDER BY update_timestamp DESC, 
                     (CASE WHEN user_name IS NOT NULL THEN 1 ELSE 0 END +
                      CASE WHEN email IS NOT NULL THEN 1 ELSE 0 END +
                      CASE WHEN company IS NOT NULL THEN 1 ELSE 0 END) DESC,
                     user_id DESC
        ) AS row_rank
    FROM bronze_users
    WHERE user_id IS NOT NULL -- Filter out completely invalid records
),

-- Final deduplicated and validated data
final_users AS (
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
    FROM cleansed_users
    WHERE row_rank = 1 -- Keep only the best record per user_id
        AND record_status = 'active' -- Only propagate clean records
)

SELECT * FROM final_users
