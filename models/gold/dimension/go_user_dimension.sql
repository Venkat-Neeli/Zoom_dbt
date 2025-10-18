{{ config(
    materialized='table'
) }}

-- Gold User Dimension Table
-- Transforms Silver user data into business-ready user dimension
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
        data_quality_score,
        record_status
    FROM {{ source('silver', 'si_users') }}
    WHERE record_status = 'ACTIVE'
      AND data_quality_score >= 0.7
),

license_info AS (
    SELECT
        assigned_to_user_id,
        license_type,
        start_date,
        end_date,
        ROW_NUMBER() OVER (PARTITION BY assigned_to_user_id ORDER BY start_date DESC) AS rn
    FROM {{ source('silver', 'si_licenses') }}
    WHERE record_status = 'ACTIVE'
),

latest_license AS (
    SELECT
        assigned_to_user_id,
        license_type
    FROM license_info
    WHERE rn = 1
),

user_dimension AS (
    SELECT
        -- Surrogate key generation
        CONCAT('UD_', ub.user_id) AS user_dim_id,
        
        -- Direct mappings from Silver
        ub.user_id,
        COALESCE(TRIM(ub.user_name), 'Unknown User') AS user_name,
        COALESCE(TRIM(ub.email), 'no-email@unknown.com') AS email_address,
        
        -- Derived fields with transformations
        CASE 
            WHEN UPPER(TRIM(ub.plan_type)) IN ('PRO', 'PROFESSIONAL') THEN 'Professional'
            WHEN UPPER(TRIM(ub.plan_type)) IN ('BASIC', 'FREE') THEN 'Basic'
            WHEN UPPER(TRIM(ub.plan_type)) IN ('ENTERPRISE', 'BUSINESS') THEN 'Enterprise'
            ELSE 'Standard'
        END AS user_type,
        
        CASE 
            WHEN UPPER(TRIM(ub.record_status)) = 'ACTIVE' THEN 'Active'
            WHEN UPPER(TRIM(ub.record_status)) = 'INACTIVE' THEN 'Inactive'
            WHEN UPPER(TRIM(ub.record_status)) = 'SUSPENDED' THEN 'Suspended'
            ELSE 'Unknown'
        END AS account_status,
        
        -- License information from joined table
        COALESCE(ll.license_type, 'No License') AS license_type,
        
        -- Placeholder fields (not available in Silver schema)
        CAST(NULL AS VARCHAR(200)) AS department_name,
        CAST(NULL AS VARCHAR(200)) AS job_title,
        CAST(NULL AS VARCHAR(50)) AS time_zone,
        CAST(NULL AS DATE) AS account_creation_date,
        CAST(NULL AS DATE) AS last_login_date,
        CAST(NULL AS VARCHAR(50)) AS language_preference,
        CAST(NULL AS VARCHAR(50)) AS phone_number,
        
        -- Audit fields
        ub.load_date,
        CURRENT_DATE() AS update_date,
        ub.source_system
        
    FROM user_base ub
    LEFT JOIN latest_license ll ON ub.user_id = ll.assigned_to_user_id
)

SELECT * FROM user_dimension
