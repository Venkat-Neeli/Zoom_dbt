{{ config(
    materialized='table',
    cluster_by=['organization_id', 'load_date']
) }}

-- Gold Organization Dimension Table
WITH organization_base AS (
    SELECT 
        company,
        COUNT(*) as user_count,
        MIN(load_date) as first_seen_date,
        MAX(update_date) as last_updated_date,
        MAX(source_system) as source_system
    FROM {{ source('silver', 'si_users') }}
    WHERE company IS NOT NULL 
      AND TRIM(company) != ''
      AND record_status = 'ACTIVE'
    GROUP BY company
),

organization_transform AS (
    SELECT 
        UUID_STRING() as organization_dim_id,
        UPPER(REPLACE(company, ' ', '_')) as organization_id,
        TRIM(company) as organization_name,
        NULL as industry_classification,
        CASE 
            WHEN user_count >= 1000 THEN 'Enterprise'
            WHEN user_count >= 100 THEN 'Large'
            WHEN user_count >= 10 THEN 'Medium'
            ELSE 'Small'
        END as organization_size,
        NULL as primary_contact_email,
        NULL as billing_address,
        NULL as account_manager_name,
        NULL as contract_start_date,
        NULL as contract_end_date,
        user_count as maximum_user_limit,
        NULL as storage_quota_gb,
        'Standard' as security_policy_level,
        first_seen_date as load_date,
        CURRENT_DATE() as update_date,
        source_system
    FROM organization_base
)

SELECT * FROM organization_transform
