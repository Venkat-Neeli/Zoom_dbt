{{ config(
    materialized='incremental',
    unique_key='user_id',
    on_schema_change='sync_all_columns',
    tags=['silver', 'users'],
    pre_hook="INSERT INTO {{ ref('si_process_audit') }} (execution_id, pipeline_name, start_time, status, source_system, target_system, process_type, load_date, update_date) SELECT '{{ invocation_id }}' || '_users_start', 'si_users_transform', CURRENT_TIMESTAMP(), 'STARTED', 'BRONZE', 'SILVER', 'ETL', CURRENT_DATE(), CURRENT_DATE() WHERE '{{ this.name }}' != 'si_process_audit'",
    post_hook="INSERT INTO {{ ref('si_process_audit') }} (execution_id, pipeline_name, end_time, status, records_processed, source_system, target_system, process_type, load_date, update_date) SELECT '{{ invocation_id }}' || '_users_end', 'si_users_transform', CURRENT_TIMESTAMP(), 'COMPLETED', (SELECT COUNT(*) FROM {{ this }}), 'BRONZE', 'SILVER', 'ETL', CURRENT_DATE(), CURRENT_DATE() WHERE '{{ this.name }}' != 'si_process_audit'"
) }}

-- Data Quality and Transformation Logic
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
                     load_timestamp DESC,
                     CASE WHEN user_name IS NOT NULL THEN 1 ELSE 0 END +
                     CASE WHEN email IS NOT NULL THEN 1 ELSE 0 END +
                     CASE WHEN company IS NOT NULL THEN 1 ELSE 0 END +
                     CASE WHEN plan_type IS NOT NULL THEN 1 ELSE 0 END DESC
        ) AS row_rank
    FROM {{ source('bronze', 'bz_users') }}
    WHERE user_id IS NOT NULL
),

deduped_users AS (
    SELECT *
    FROM bronze_users
    WHERE row_rank = 1
),

data_quality_checks AS (
    SELECT 
        user_id,
        TRIM(user_name) AS user_name_clean,
        LOWER(TRIM(email)) AS email_clean,
        TRIM(company) AS company_clean,
        CASE 
            WHEN UPPER(TRIM(plan_type)) IN ('FREE', 'PRO', 'BUSINESS', 'ENTERPRISE') 
            THEN UPPER(TRIM(plan_type))
            ELSE 'UNKNOWN'
        END AS plan_type_clean,
        load_timestamp,
        update_timestamp,
        source_system,
        -- Data Quality Score Calculation
        CASE 
            WHEN user_id IS NOT NULL 
                AND TRIM(user_name) IS NOT NULL AND TRIM(user_name) != ''
                AND email_clean RLIKE '^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\\.[A-Za-z]{2,}$'
                AND plan_type_clean IN ('FREE', 'PRO', 'BUSINESS', 'ENTERPRISE')
            THEN 1.00
            WHEN user_id IS NOT NULL 
                AND TRIM(user_name) IS NOT NULL AND TRIM(user_name) != ''
                AND email_clean IS NOT NULL
            THEN 0.75
            WHEN user_id IS NOT NULL 
                AND TRIM(user_name) IS NOT NULL AND TRIM(user_name) != ''
            THEN 0.50
            ELSE 0.25
        END AS data_quality_score,
        -- Record Status
        CASE 
            WHEN user_id IS NOT NULL 
                AND TRIM(user_name) IS NOT NULL AND TRIM(user_name) != ''
                AND email_clean RLIKE '^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\\.[A-Za-z]{2,}$'
            THEN 'ACTIVE'
            ELSE 'ERROR'
        END AS record_status
    FROM deduped_users
),

final_transform AS (
    SELECT 
        user_id,
        COALESCE(user_name_clean, '000') AS user_name,
        COALESCE(email_clean, '000') AS email,
        COALESCE(company_clean, '000') AS company,
        plan_type_clean AS plan_type,
        load_timestamp,
        update_timestamp,
        source_system,
        DATE(load_timestamp) AS load_date,
        DATE(update_timestamp) AS update_date,
        data_quality_score,
        record_status
    FROM data_quality_checks
    WHERE record_status = 'ACTIVE'
)

SELECT * FROM final_transform

{% if is_incremental() %}
    WHERE update_timestamp > (SELECT COALESCE(MAX(update_timestamp), '1900-01-01') FROM {{ this }})
{% endif %}
