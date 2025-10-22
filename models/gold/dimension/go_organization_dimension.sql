{{ config(
    materialized='table',
    cluster_by=['organization_id', 'load_date'],
    pre_hook="INSERT INTO {{ this.schema }}.go_process_audit (execution_id, pipeline_name, process_type, start_time, status, source_system, target_system, user_executed, load_date) SELECT UUID_STRING(), 'Organization Dimension Load', 'DBT_MODEL', CURRENT_TIMESTAMP(), 'STARTED', 'SILVER', 'GOLD', 'DBT_CLOUD', CURRENT_DATE() WHERE '{{ this.name }}' != 'go_process_audit'",
    post_hook="INSERT INTO {{ this.schema }}.go_process_audit (execution_id, pipeline_name, process_type, start_time, end_time, status, records_processed, source_system, target_system, user_executed, processing_duration_seconds, load_date) SELECT UUID_STRING(), 'Organization Dimension Load', 'DBT_MODEL', CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP(), 'COMPLETED', (SELECT COUNT(*) FROM {{ this }}), 'SILVER', 'GOLD', 'DBT_CLOUD', 0, CURRENT_DATE() WHERE '{{ this.name }}' != 'go_process_audit'"
) }}

-- Gold Organization Dimension
WITH organization_base AS (
    SELECT DISTINCT
        company,
        load_date,
        update_date,
        source_system
    FROM {{ source('silver', 'si_users') }}
    WHERE company IS NOT NULL 
    AND record_status = 'ACTIVE'
),

organization_dimension AS (
    SELECT 
        UUID_STRING() AS organization_dim_id,
        COALESCE(company, 'Unknown Organization') AS organization_id,
        COALESCE(company, 'Unknown Organization') AS organization_name,
        NULL AS industry_classification,
        NULL AS organization_size,
        NULL AS primary_contact_email,
        NULL AS billing_address,
        NULL AS account_manager_name,
        NULL AS contract_start_date,
        NULL AS contract_end_date,
        NULL AS maximum_user_limit,
        NULL AS storage_quota_gb,
        NULL AS security_policy_level,
        load_date,
        update_date,
        source_system
    FROM organization_base
)

SELECT * FROM organization_dimension
