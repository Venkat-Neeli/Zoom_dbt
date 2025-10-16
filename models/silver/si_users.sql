{{ config(
    materialized='incremental',
    unique_key='user_id',
    on_schema_change='fail',
    tags=['silver', 'users'],
    pre_hook="INSERT INTO {{ ref('si_process_audit') }} (execution_id, pipeline_name, start_time, status, source_system, target_system, process_type, load_date, update_date) SELECT '{{ dbt_utils.generate_surrogate_key(['CURRENT_TIMESTAMP()', 'RANDOM()']) }}', 'si_users_transformation', CURRENT_TIMESTAMP(), 'STARTED', 'BRONZE', 'SILVER', 'ETL', CURRENT_DATE(), CURRENT_DATE() WHERE '{{ this.name }}' != 'si_process_audit'",
    post_hook="INSERT INTO {{ ref('si_process_audit') }} (execution_id, pipeline_name, end_time, status, records_processed, source_system, target_system, process_type, load_date, update_date) SELECT '{{ dbt_utils.generate_surrogate_key(['CURRENT_TIMESTAMP()', 'RANDOM()']) }}', 'si_users_transformation', CURRENT_TIMESTAMP(), 'COMPLETED', (SELECT COUNT(*) FROM {{ this }}), 'BRONZE', 'SILVER', 'ETL', CURRENT_DATE(), CURRENT_DATE() WHERE '{{ this.name }}' != 'si_process_audit'"
) }}

-- Data Quality and Transformation Logic for Users
WITH bronze_users AS (
    SELECT 
        user_id,
        user_name,
        email,
        company,
        plan_type,
        load_timestamp,
        update_timestamp,
        source_system,
        ROW_NUMBER() OVER (
            PARTITION BY user_id 
            ORDER BY update_timestamp DESC, 
                     load_timestamp DESC
        ) AS row_num
    FROM {{ source('bronze', 'bz_users') }}
    WHERE user_id IS NOT NULL
      AND user_name IS NOT NULL
      AND email IS NOT NULL
      AND REGEXP_LIKE(email, '^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\\.[A-Za-z]{2,}$')
      AND plan_type IN ('Free', 'Pro', 'Business', 'Enterprise')
),

cleaned_users AS (
    SELECT 
        user_id,
        TRIM(user_name) AS user_name,
        LOWER(TRIM(email)) AS email,
        CASE 
            WHEN TRIM(company) = '' THEN '000'
            ELSE TRIM(company)
        END AS company,
        CASE 
            WHEN plan_type = 'Basic' THEN 'Free'
            ELSE plan_type
        END AS plan_type,
        load_timestamp,
        update_timestamp,
        source_system,
        DATE(load_timestamp) AS load_date,
        DATE(update_timestamp) AS update_date,
        -- Data Quality Score Calculation
        CASE 
            WHEN user_id IS NOT NULL 
                 AND user_name IS NOT NULL 
                 AND email IS NOT NULL 
                 AND REGEXP_LIKE(email, '^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\\.[A-Za-z]{2,}$')
                 AND plan_type IN ('Free', 'Pro', 'Business', 'Enterprise')
            THEN 1.0
            ELSE 0.8
        END AS data_quality_score,
        'active' AS record_status
    FROM bronze_users
    WHERE row_num = 1
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
FROM cleaned_users

{% if is_incremental() %}
  WHERE update_timestamp > (SELECT MAX(update_timestamp) FROM {{ this }})
{% endif %}
