{{ config(
    materialized='table',
    cluster_by=['organization_id', 'load_date']
) }}

-- Organization Dimension Transformation
WITH organization_base AS (
    SELECT DISTINCT
        company,
        MIN(load_date) AS load_date,
        MAX(update_date) AS update_date,
        FIRST_VALUE(source_system) OVER (PARTITION BY company ORDER BY update_timestamp DESC) AS source_system
    FROM {{ source('silver', 'si_users') }}
    WHERE company IS NOT NULL 
      AND company != ''
      AND record_status = 'ACTIVE'
    GROUP BY company
)

SELECT 
    UUID_STRING() AS organization_dim_id,
    UPPER(TRIM(company))::VARCHAR(50) AS organization_id,
    TRIM(company)::VARCHAR(500) AS organization_name,
    NULL::VARCHAR(200) AS industry_classification,
    NULL::VARCHAR(50) AS organization_size,
    NULL::VARCHAR(320) AS primary_contact_email,
    NULL::VARCHAR(1000) AS billing_address,
    NULL::VARCHAR(255) AS account_manager_name,
    NULL::DATE AS contract_start_date,
    NULL::DATE AS contract_end_date,
    NULL::NUMBER AS maximum_user_limit,
    NULL::NUMBER AS storage_quota_gb,
    NULL::VARCHAR(100) AS security_policy_level,
    load_date,
    CURRENT_DATE() AS update_date,
    source_system
FROM organization_base
