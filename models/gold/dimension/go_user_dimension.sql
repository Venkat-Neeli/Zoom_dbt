{{ config(
    materialized='table',
    cluster_by=['user_id', 'load_date'],
    tags=['dimension', 'gold'],
    pre_hook="INSERT INTO {{ ref('go_process_audit') }} (execution_id, pipeline_name, process_type, start_time, status, source_system, target_system, load_date, update_date) SELECT '{{ dbt_utils.generate_surrogate_key(['go_user_dimension', run_started_at]) }}', 'go_user_dimension', 'DIMENSION_LOAD', CURRENT_TIMESTAMP(), 'STARTED', 'SILVER', 'GOLD', CURRENT_DATE(), CURRENT_DATE() WHERE '{{ this.name }}' != 'go_process_audit'",
    post_hook="UPDATE {{ ref('go_process_audit') }} SET end_time = CURRENT_TIMESTAMP(), status = 'COMPLETED', processing_duration_seconds = DATEDIFF('second', start_time, CURRENT_TIMESTAMP()) WHERE execution_id = '{{ dbt_utils.generate_surrogate_key(['go_user_dimension', run_started_at]) }}' AND '{{ this.name }}' != 'go_process_audit'"
) }}

WITH user_base AS (
    SELECT 
        u.user_id,
        u.user_name,
        u.email,
        u.company,
        u.plan_type,
        u.load_date,
        u.update_date,
        u.source_system,
        u.record_status,
        ROW_NUMBER() OVER (PARTITION BY u.user_id ORDER BY u.update_timestamp DESC) AS rn
    FROM {{ source('silver', 'si_users') }} u
    WHERE u.record_status = 'ACTIVE'
        AND u.data_quality_score >= 0.7
),

latest_license AS (
    SELECT 
        l.assigned_to_user_id,
        l.license_type,
        ROW_NUMBER() OVER (PARTITION BY l.assigned_to_user_id ORDER BY l.start_date DESC) AS rn
    FROM {{ source('silver', 'si_licenses') }} l
    WHERE l.record_status = 'ACTIVE'
        AND l.data_quality_score >= 0.7
        AND l.start_date <= CURRENT_DATE()
        AND (l.end_date IS NULL OR l.end_date >= CURRENT_DATE())
),

user_dimension AS (
    SELECT 
        {{ dbt_utils.generate_surrogate_key(['ub.user_id']) }} AS user_dim_id,
        ub.user_id,
        COALESCE(TRIM(ub.user_name), 'Unknown User') AS user_name,
        COALESCE(TRIM(ub.email), 'no-email@unknown.com') AS email_address,
        CASE 
            WHEN ub.plan_type = 'Pro' THEN 'Professional'
            WHEN ub.plan_type = 'Basic' THEN 'Basic'
            WHEN ub.plan_type = 'Enterprise' THEN 'Enterprise'
            ELSE 'Standard'
        END AS user_type,
        CASE 
            WHEN ub.record_status = 'ACTIVE' THEN 'Active'
            WHEN ub.record_status = 'INACTIVE' THEN 'Inactive'
            ELSE 'Unknown'
        END AS account_status,
        COALESCE(ll.license_type, 'No License') AS license_type,
        NULL AS department_name,
        NULL AS job_title,
        NULL AS time_zone,
        NULL AS account_creation_date,
        NULL AS last_login_date,
        NULL AS language_preference,
        NULL AS phone_number,
        ub.load_date,
        CURRENT_DATE() AS update_date,
        ub.source_system
    FROM user_base ub
    LEFT JOIN latest_license ll ON ub.user_id = ll.assigned_to_user_id AND ll.rn = 1
    WHERE ub.rn = 1
)

SELECT 
    user_dim_id::VARCHAR(50) AS user_dim_id,
    user_id::VARCHAR(50) AS user_id,
    user_name::VARCHAR(255) AS user_name,
    email_address::VARCHAR(320) AS email_address,
    user_type::VARCHAR(50) AS user_type,
    account_status::VARCHAR(50) AS account_status,
    license_type::VARCHAR(100) AS license_type,
    department_name::VARCHAR(200) AS department_name,
    job_title::VARCHAR(200) AS job_title,
    time_zone::VARCHAR(50) AS time_zone,
    account_creation_date,
    last_login_date,
    language_preference::VARCHAR(50) AS language_preference,
    phone_number::VARCHAR(50) AS phone_number,
    load_date,
    update_date,
    source_system::VARCHAR(100) AS source_system
FROM user_dimension
