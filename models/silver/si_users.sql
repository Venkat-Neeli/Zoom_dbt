{{ config(
    materialized='incremental',
    unique_key='user_id',
    on_schema_change='sync_all_columns',
    pre_hook="INSERT INTO {{ ref('si_process_audit') }} (execution_id, pipeline_name, start_time, status, source_system, target_system, process_type, load_date, update_date) SELECT '{{ invocation_id }}', 'si_users_transform', current_timestamp(), 'STARTED', 'Bronze', 'Silver', 'ETL', current_date(), current_date() WHERE '{{ this.name }}' != 'si_process_audit'",
    post_hook="UPDATE {{ ref('si_process_audit') }} SET end_time = current_timestamp(), status = 'SUCCESS', records_processed = (SELECT COUNT(*) FROM {{ this }}), records_successful = (SELECT COUNT(*) FROM {{ this }}), processing_duration_seconds = DATEDIFF('second', start_time, current_timestamp()) WHERE execution_id = '{{ invocation_id }}' AND pipeline_name = 'si_users_transform' AND '{{ this.name }}' != 'si_process_audit'"
) }}

-- Data Quality and Transformation Logic
WITH bronze_users AS (
    SELECT *,
           ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY update_timestamp DESC, load_timestamp DESC) AS row_num
    FROM {{ source('bronze', 'bz_users') }}
    WHERE user_id IS NOT NULL
),

deduped_users AS (
    SELECT *
    FROM bronze_users
    WHERE row_num = 1
),

data_quality_checks AS (
    SELECT *,
           CASE 
               WHEN user_id IS NULL THEN 0.0
               WHEN user_name IS NULL OR TRIM(user_name) = '' THEN 0.2
               WHEN email IS NULL OR TRIM(email) = '' OR NOT REGEXP_LIKE(email, '^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}$') THEN 0.3
               WHEN plan_type NOT IN ('Free', 'Pro', 'Business', 'Enterprise') THEN 0.5
               ELSE 1.0
           END AS data_quality_score,
           
           CASE 
               WHEN user_id IS NULL OR user_name IS NULL OR TRIM(user_name) = '' OR 
                    email IS NULL OR TRIM(email) = '' OR 
                    NOT REGEXP_LIKE(email, '^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}$') THEN 'error'
               ELSE 'active'
           END AS record_status
    FROM deduped_users
),

valid_records AS (
    SELECT 
        user_id,
        TRIM(user_name) AS user_name,
        LOWER(TRIM(email)) AS email,
        TRIM(company) AS company,
        CASE 
            WHEN UPPER(plan_type) IN ('FREE', 'PRO', 'BUSINESS', 'ENTERPRISE') THEN UPPER(plan_type)
            ELSE 'FREE'
        END AS plan_type,
        load_timestamp,
        update_timestamp,
        source_system,
        DATE(load_timestamp) AS load_date,
        DATE(update_timestamp) AS update_date,
        data_quality_score,
        record_status
    FROM data_quality_checks
    WHERE record_status = 'active'
      AND data_quality_score >= 0.7
)

SELECT * FROM valid_records

{% if is_incremental() %}
    WHERE update_timestamp > (SELECT MAX(update_timestamp) FROM {{ this }})
{% endif %}
