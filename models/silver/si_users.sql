{{ config(
    materialized='incremental',
    unique_key='user_id',
    on_schema_change='fail',
    pre_hook="INSERT INTO {{ ref('si_process_audit') }} (execution_id, pipeline_name, start_time, status, source_system, target_system, process_type, user_executed, load_date, update_date) SELECT '{{ dbt_utils.generate_surrogate_key(['invocation_id', 'si_users']) }}', 'si_users', CURRENT_TIMESTAMP(), 'RUNNING', 'BRONZE', 'SILVER', 'ETL', CURRENT_USER(), CURRENT_DATE(), CURRENT_DATE() WHERE '{{ this.name }}' != 'si_process_audit'",
    post_hook="UPDATE {{ ref('si_process_audit') }} SET end_time = CURRENT_TIMESTAMP(), status = 'SUCCESS', records_processed = (SELECT COUNT(*) FROM {{ this }}), records_successful = (SELECT COUNT(*) FROM {{ this }} WHERE record_status = 'active'), processing_duration_seconds = DATEDIFF(second, start_time, CURRENT_TIMESTAMP()) WHERE execution_id = '{{ dbt_utils.generate_surrogate_key(['invocation_id', 'si_users']) }}' AND '{{ this.name }}' != 'si_process_audit'"
) }}

WITH bronze_users AS (
    SELECT *
    FROM {{ source('bronze', 'bz_users') }}
    {% if is_incremental() %}
        WHERE update_timestamp > (SELECT MAX(update_timestamp) FROM {{ this }})
    {% endif %}
),

-- Data Quality and Deduplication
cleaned_users AS (
    SELECT *,
        ROW_NUMBER() OVER (
            PARTITION BY user_id 
            ORDER BY update_timestamp DESC, 
                     load_timestamp DESC,
                     CASE WHEN user_name IS NOT NULL THEN 1 ELSE 0 END +
                     CASE WHEN email IS NOT NULL THEN 1 ELSE 0 END +
                     CASE WHEN company IS NOT NULL THEN 1 ELSE 0 END +
                     CASE WHEN plan_type IS NOT NULL THEN 1 ELSE 0 END DESC,
                     user_id DESC
        ) AS row_num
    FROM bronze_users
    WHERE user_id IS NOT NULL
      AND user_name IS NOT NULL
      AND email IS NOT NULL
      AND REGEXP_LIKE(email, '^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}$')
      AND plan_type IN ('Free', 'Pro', 'Business', 'Enterprise')
),

-- Calculate Data Quality Score
final_users AS (
    SELECT 
        user_id,
        TRIM(user_name) AS user_name,
        LOWER(TRIM(email)) AS email,
        TRIM(COALESCE(company, '000')) AS company,
        CASE 
            WHEN UPPER(TRIM(plan_type)) = 'FREE' THEN 'Free'
            WHEN UPPER(TRIM(plan_type)) = 'PRO' THEN 'Pro'
            WHEN UPPER(TRIM(plan_type)) = 'BUSINESS' THEN 'Business'
            WHEN UPPER(TRIM(plan_type)) = 'ENTERPRISE' THEN 'Enterprise'
            ELSE plan_type
        END AS plan_type,
        load_timestamp,
        update_timestamp,
        source_system,
        DATE(load_timestamp) AS load_date,
        DATE(update_timestamp) AS update_date,
        -- Data Quality Score Calculation
        ROUND(
            (CASE WHEN user_id IS NOT NULL THEN 0.25 ELSE 0 END +
             CASE WHEN user_name IS NOT NULL AND LENGTH(TRIM(user_name)) > 0 THEN 0.25 ELSE 0 END +
             CASE WHEN email IS NOT NULL AND REGEXP_LIKE(email, '^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}$') THEN 0.25 ELSE 0 END +
             CASE WHEN plan_type IN ('Free', 'Pro', 'Business', 'Enterprise') THEN 0.25 ELSE 0 END), 2
        ) AS data_quality_score,
        'active' AS record_status
    FROM cleaned_users
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
FROM final_users
