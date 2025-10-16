{{ config(
    materialized='incremental',
    unique_key='user_id',
    on_schema_change='fail',
    pre_hook="INSERT INTO {{ ref('si_process_audit') }} (execution_id, pipeline_name, start_time, status, source_system, target_system, process_type, load_date, update_date) SELECT '{{ invocation_id }}' || '_users', 'si_users_transform', CURRENT_TIMESTAMP(), 'STARTED', 'BRONZE', 'SILVER', 'ETL', CURRENT_DATE(), CURRENT_DATE() WHERE '{{ this.name }}' != 'si_process_audit'",
    post_hook="UPDATE {{ ref('si_process_audit') }} SET end_time = CURRENT_TIMESTAMP(), status = 'SUCCESS', records_processed = (SELECT COUNT(*) FROM {{ this }}), records_successful = (SELECT COUNT(*) FROM {{ this }} WHERE record_status = 'active'), processing_duration_seconds = DATEDIFF('second', start_time, CURRENT_TIMESTAMP()) WHERE execution_id = '{{ invocation_id }}' || '_users' AND '{{ this.name }}' != 'si_process_audit'"
) }}

-- Silver Users Table Transformation
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
),

data_quality_checks AS (
    SELECT 
        *,
        -- Calculate data quality score
        CASE 
            WHEN user_id IS NULL THEN 0.0
            WHEN user_name IS NULL OR TRIM(user_name) = '' THEN 0.2
            WHEN email IS NULL OR TRIM(email) = '' OR NOT REGEXP_LIKE(email, '^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\\.[A-Za-z]{2,}$') THEN 0.3
            WHEN plan_type NOT IN ('Free', 'Pro', 'Business', 'Enterprise') THEN 0.7
            ELSE 1.0
        END AS data_quality_score,
        
        -- Set record status
        CASE 
            WHEN user_id IS NULL OR user_name IS NULL OR TRIM(user_name) = '' 
                 OR email IS NULL OR TRIM(email) = '' 
                 OR NOT REGEXP_LIKE(email, '^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\\.[A-Za-z]{2,}$') THEN 'error'
            ELSE 'active'
        END AS record_status
    FROM bronze_users
    WHERE row_num = 1
),

final_transform AS (
    SELECT 
        user_id,
        TRIM(user_name) AS user_name,
        LOWER(TRIM(email)) AS email,
        TRIM(company) AS company,
        CASE 
            WHEN UPPER(plan_type) IN ('FREE', 'BASIC') THEN 'Free'
            WHEN UPPER(plan_type) = 'PRO' THEN 'Pro'
            WHEN UPPER(plan_type) = 'BUSINESS' THEN 'Business'
            WHEN UPPER(plan_type) = 'ENTERPRISE' THEN 'Enterprise'
            ELSE plan_type
        END AS plan_type,
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
FROM final_transform

{% if is_incremental() %}
    WHERE update_timestamp > (SELECT MAX(update_timestamp) FROM {{ this }})
{% endif %}
