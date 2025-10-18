{{ config(
    materialized='table',
    cluster_by=['user_id', 'load_date']
) }}

-- Gold User Dimension Table
WITH user_base AS (
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
        record_status,
        ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY update_timestamp DESC) as rn
    FROM {{ source('silver', 'si_users') }}
    WHERE record_status = 'ACTIVE'
),

license_info AS (
    SELECT 
        assigned_to_user_id as user_id,
        license_type,
        start_date as license_start_date,
        end_date as license_end_date,
        ROW_NUMBER() OVER (PARTITION BY assigned_to_user_id ORDER BY start_date DESC) as license_rn
    FROM {{ source('silver', 'si_licenses') }}
    WHERE record_status = 'ACTIVE'
),

final_transform AS (
    SELECT 
        UUID_STRING() as user_dim_id,
        ub.user_id,
        COALESCE(TRIM(ub.user_name), 'Unknown User') as user_name,
        COALESCE(TRIM(ub.email), 'no-email@unknown.com') as email_address,
        CASE 
            WHEN ub.plan_type = 'Pro' THEN 'Professional'
            WHEN ub.plan_type = 'Basic' THEN 'Basic'
            WHEN ub.plan_type = 'Enterprise' THEN 'Enterprise'
            ELSE 'Standard'
        END as user_type,
        CASE 
            WHEN ub.record_status = 'ACTIVE' THEN 'Active'
            WHEN ub.record_status = 'INACTIVE' THEN 'Inactive'
            ELSE 'Unknown'
        END as account_status,
        COALESCE(li.license_type, 'No License') as license_type,
        COALESCE(TRIM(ub.company), 'Individual') as department_name,
        NULL as job_title,
        NULL as time_zone,
        NULL as account_creation_date,
        NULL as last_login_date,
        NULL as language_preference,
        NULL as phone_number,
        ub.load_date,
        CURRENT_DATE() as update_date,
        ub.source_system
    FROM user_base ub
    LEFT JOIN license_info li ON ub.user_id = li.user_id AND li.license_rn = 1
    WHERE ub.rn = 1
      AND ub.user_id IS NOT NULL
)

SELECT * FROM final_transform
