{{
    config(
        materialized='incremental',
        unique_key='user_id',
        on_schema_change='sync_all_columns',
        pre_hook="{% if not (this.name == 'si_process_audit') %}INSERT INTO {{ ref('si_process_audit') }} (execution_id, pipeline_name, start_time, status, source_system, target_system, process_type, load_date, update_date) VALUES ('{{ invocation_id }}_{{ this.name }}', '{{ this.name }}', CURRENT_TIMESTAMP(), 'STARTED', 'BRONZE', 'SILVER', 'ETL', CURRENT_DATE(), CURRENT_DATE()){% endif %}",
        post_hook="{% if not (this.name == 'si_process_audit') %}UPDATE {{ ref('si_process_audit') }} SET end_time = CURRENT_TIMESTAMP(), status = 'SUCCESS', records_processed = (SELECT COUNT(*) FROM {{ this }}), records_successful = (SELECT COUNT(*) FROM {{ this }} WHERE record_status = 'active'), records_failed = (SELECT COUNT(*) FROM {{ this }} WHERE record_status = 'error'), processing_duration_seconds = DATEDIFF(second, start_time, CURRENT_TIMESTAMP()) WHERE execution_id = '{{ invocation_id }}_{{ this.name }}'{% endif %}"
    )
}}

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
        ) as row_num
    FROM {{ source('bronze', 'bz_users') }}
    WHERE user_id IS NOT NULL
),

deduped_users AS (
    SELECT *
    FROM bronze_users
    WHERE row_num = 1
),

data_quality_checks AS (
    SELECT 
        *,
        -- Null checks
        CASE WHEN user_id IS NOT NULL AND user_name IS NOT NULL AND email IS NOT NULL THEN 1 ELSE 0 END as null_check,
        -- Format checks
        CASE WHEN REGEXP_LIKE(email, '^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\\.[A-Za-z]{2,}$') THEN 1 ELSE 0 END as format_check,
        -- Domain checks
        CASE WHEN plan_type IN ('Free', 'Pro', 'Business', 'Enterprise') THEN 1 ELSE 0 END as domain_check
    FROM deduped_users
),

transformed_users AS (
    SELECT 
        user_id,
        TRIM(user_name) as user_name,
        LOWER(TRIM(email)) as email,
        CASE WHEN TRIM(company) = '' THEN '000' ELSE TRIM(company) END as company,
        CASE 
            WHEN UPPER(plan_type) = 'FREE' THEN 'Free'
            WHEN UPPER(plan_type) = 'PRO' THEN 'Pro'
            WHEN UPPER(plan_type) = 'BUSINESS' THEN 'Business'
            WHEN UPPER(plan_type) = 'ENTERPRISE' THEN 'Enterprise'
            ELSE plan_type
        END as plan_type,
        load_timestamp,
        update_timestamp,
        source_system,
        DATE(load_timestamp) as load_date,
        DATE(update_timestamp) as update_date,
        {{ calculate_data_quality_score('null_check = 1', 'format_check = 1', 'domain_check = 1') }} as data_quality_score,
        CASE 
            WHEN null_check = 1 AND format_check = 1 AND domain_check = 1 THEN 'active'
            ELSE 'error'
        END as record_status
    FROM data_quality_checks
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

{% if is_incremental() %}
    AND update_timestamp > (SELECT MAX(update_timestamp) FROM {{ this }})
{% endif %}
