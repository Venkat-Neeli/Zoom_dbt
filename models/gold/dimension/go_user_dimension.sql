{{ config(
    materialized='table',
    cluster_by=['user_id', 'load_date'],
    pre_hook="INSERT INTO {{ this.schema }}.go_process_audit (execution_id, pipeline_name, process_type, start_time, status, source_system, target_system, user_executed, load_date) SELECT UUID_STRING(), 'User Dimension Load', 'DBT_MODEL', CURRENT_TIMESTAMP(), 'STARTED', 'SILVER', 'GOLD', 'DBT_CLOUD', CURRENT_DATE() WHERE '{{ this.name }}' != 'go_process_audit'",
    post_hook="INSERT INTO {{ this.schema }}.go_process_audit (execution_id, pipeline_name, process_type, start_time, end_time, status, records_processed, source_system, target_system, user_executed, processing_duration_seconds, load_date) SELECT UUID_STRING(), 'User Dimension Load', 'DBT_MODEL', CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP(), 'COMPLETED', (SELECT COUNT(*) FROM {{ this }}), 'SILVER', 'GOLD', 'DBT_CLOUD', 0, CURRENT_DATE() WHERE '{{ this.name }}' != 'go_process_audit'"
) }}

-- Gold User Dimension
WITH user_base AS (
    SELECT 
        user_id,
        user_name,
        email,
        company,
        plan_type,
        record_status,
        load_date,
        update_date,
        source_system,
        ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY update_timestamp DESC) as rn
    FROM {{ source('silver', 'si_users') }}
    WHERE record_status = 'ACTIVE'
),

license_info AS (
    SELECT 
        assigned_to_user_id,
        license_type,
        ROW_NUMBER() OVER (PARTITION BY assigned_to_user_id ORDER BY start_date DESC) as rn
    FROM {{ source('silver', 'si_licenses') }}
    WHERE record_status = 'ACTIVE'
),

user_dimension AS (
    SELECT 
        UUID_STRING() AS user_dim_id,
        u.user_id,
        COALESCE(u.user_name, 'Unknown User') AS user_name,
        COALESCE(u.email, 'unknown@email.com') AS email_address,
        CASE 
            WHEN u.plan_type = 'Pro' THEN 'Professional'
            WHEN u.plan_type = 'Basic' THEN 'Basic'
            WHEN u.plan_type = 'Enterprise' THEN 'Enterprise'
            ELSE 'Standard'
        END AS user_type,
        CASE 
            WHEN u.record_status = 'ACTIVE' THEN 'Active'
            WHEN u.record_status = 'INACTIVE' THEN 'Inactive'
            ELSE 'Unknown'
        END AS account_status,
        COALESCE(l.license_type, 'No License') AS license_type,
        NULL AS department_name,
        NULL AS job_title,
        NULL AS time_zone,
        NULL AS account_creation_date,
        NULL AS last_login_date,
        NULL AS language_preference,
        NULL AS phone_number,
        u.load_date,
        u.update_date,
        u.source_system
    FROM user_base u
    LEFT JOIN license_info l ON u.user_id = l.assigned_to_user_id AND l.rn = 1
    WHERE u.rn = 1
)

SELECT * FROM user_dimension
