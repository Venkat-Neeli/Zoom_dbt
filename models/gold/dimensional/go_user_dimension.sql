{{ config(
    materialized='table',
    cluster_by=['user_id', 'load_date']
) }}

-- User Dimension Transformation
WITH user_base AS (
    SELECT 
        user_id,
        user_name,
        email,
        company,
        plan_type,
        load_date,
        update_date,
        source_system,
        record_status,
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
)

SELECT 
    UUID_STRING() AS user_dim_id,
    ub.user_id,
    COALESCE(TRIM(ub.user_name), 'Unknown User')::VARCHAR(255) AS user_name,
    COALESCE(TRIM(ub.email), 'no-email@unknown.com')::VARCHAR(320) AS email_address,
    CASE 
        WHEN ub.plan_type = 'Pro' THEN 'Professional'
        WHEN ub.plan_type = 'Basic' THEN 'Basic'
        WHEN ub.plan_type = 'Enterprise' THEN 'Enterprise'
        ELSE 'Standard'
    END::VARCHAR(50) AS user_type,
    CASE 
        WHEN ub.record_status = 'ACTIVE' THEN 'Active'
        WHEN ub.record_status = 'INACTIVE' THEN 'Inactive'
        ELSE 'Unknown'
    END::VARCHAR(50) AS account_status,
    COALESCE(li.license_type, 'No License')::VARCHAR(100) AS license_type,
    NULL::VARCHAR(200) AS department_name,
    NULL::VARCHAR(200) AS job_title,
    NULL::VARCHAR(50) AS time_zone,
    NULL::DATE AS account_creation_date,
    NULL::DATE AS last_login_date,
    NULL::VARCHAR(50) AS language_preference,
    NULL::VARCHAR(50) AS phone_number,
    ub.load_date,
    CURRENT_DATE() AS update_date,
    ub.source_system
FROM user_base ub
LEFT JOIN license_info li ON ub.user_id = li.assigned_to_user_id AND li.rn = 1
WHERE ub.rn = 1
