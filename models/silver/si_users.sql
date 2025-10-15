{{
    config(
        materialized='incremental',
        unique_key='user_id',
        on_schema_change='fail',
        pre_hook="INSERT INTO {{ ref('si_process_audit') }} (execution_id, pipeline_name, start_time, status, source_system, target_system, process_type, load_date, update_date) SELECT '{{ invocation_id }}', 'si_users_transform', current_timestamp(), 'STARTED', 'BRONZE', 'SILVER', 'ETL', current_date(), current_date() WHERE '{{ this.name }}' != 'si_process_audit'",
        post_hook="INSERT INTO {{ ref('si_process_audit') }} (execution_id, pipeline_name, end_time, status, records_processed, source_system, target_system, process_type, load_date, update_date) SELECT '{{ invocation_id }}', 'si_users_transform', current_timestamp(), 'COMPLETED', (SELECT count(*) FROM {{ this }}), 'BRONZE', 'SILVER', 'ETL', current_date(), current_date() WHERE '{{ this.name }}' != 'si_process_audit'"
    )
}}

-- Transform bronze users to silver users with data quality checks
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
        ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY update_timestamp DESC, load_timestamp DESC) as rn
    FROM {{ source('bronze', 'bz_users') }}
    WHERE user_id IS NOT NULL
),

-- Data quality validation and cleansing
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
            WHEN plan_type IN ('Free', 'Pro', 'Business', 'Enterprise') THEN plan_type
            ELSE 'Free'
        END AS plan_type,
        load_timestamp,
        update_timestamp,
        source_system,
        -- Calculate data quality score
        CASE 
            WHEN user_name IS NOT NULL 
                AND email IS NOT NULL 
                AND email RLIKE '^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\\.[A-Za-z]{2,}$'
                AND plan_type IN ('Free', 'Pro', 'Business', 'Enterprise')
            THEN 1.00
            ELSE 0.50
        END AS data_quality_score,
        CASE 
            WHEN user_name IS NOT NULL 
                AND email IS NOT NULL 
                AND email RLIKE '^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\\.[A-Za-z]{2,}$'
            THEN 'active'
            ELSE 'error'
        END AS record_status
    FROM bronze_users
    WHERE rn = 1  -- Deduplication: keep latest record
        AND user_name IS NOT NULL
        AND email IS NOT NULL
        AND email RLIKE '^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\\.[A-Za-z]{2,}$'
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
    DATE(load_timestamp) AS load_date,
    DATE(update_timestamp) AS update_date,
    data_quality_score,
    record_status
FROM cleaned_users

{% if is_incremental() %}
  WHERE update_timestamp > (SELECT COALESCE(MAX(update_timestamp), '1900-01-01') FROM {{ this }})
{% endif %}
