{{ config(
    materialized='table'
) }}

-- Gold Organization Dimension Table
-- Creates organization dimension from user company information
WITH organization_base AS (
    SELECT DISTINCT
        company,
        source_system,
        load_date,
        update_date
    FROM {{ source('silver', 'si_users') }}
    WHERE record_status = 'ACTIVE'
      AND company IS NOT NULL
      AND TRIM(company) != ''
),

organization_dimension AS (
    SELECT
        -- Surrogate key generation
        CONCAT('OD_', company) AS organization_dim_id,
        
        -- Use company as organization_id since no separate org table exists
        CONCAT('ORG_', company) AS organization_id,
        
        TRIM(company) AS organization_name,
        
        -- Placeholder fields (not available in Silver schema)
        CAST(NULL AS VARCHAR(200)) AS industry_classification,
        CASE 
            WHEN LENGTH(company) > 50 THEN 'Large'
            WHEN LENGTH(company) > 20 THEN 'Medium'
            ELSE 'Small'
        END AS organization_size,
        CAST(NULL AS VARCHAR(320)) AS primary_contact_email,
        CAST(NULL AS VARCHAR(1000)) AS billing_address,
        CAST(NULL AS VARCHAR(255)) AS account_manager_name,
        CAST(NULL AS DATE) AS contract_start_date,
        CAST(NULL AS DATE) AS contract_end_date,
        CAST(NULL AS NUMBER) AS maximum_user_limit,
        CAST(NULL AS NUMBER) AS storage_quota_gb,
        CAST(NULL AS VARCHAR(100)) AS security_policy_level,
        
        -- Audit fields
        load_date,
        CURRENT_DATE() AS update_date,
        source_system
        
    FROM organization_base
)

SELECT * FROM organization_dimension
