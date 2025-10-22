{{ config(
    materialized='table',
    cluster_by=['organization_id', 'load_date'],
    tags=['dimension', 'gold'],
    pre_hook="INSERT INTO {{ ref('go_process_audit') }} (execution_id, pipeline_name, process_type, start_time, status, source_system, target_system, user_executed, server_name, load_date, update_date) SELECT '{{ dbt_utils.generate_surrogate_key(['invocation_id', 'this.name']) }}', 'GO_ORGANIZATION_DIMENSION', 'DIMENSION_BUILD', CURRENT_TIMESTAMP(), 'STARTED', 'SILVER', 'GOLD', CURRENT_USER(), 'DBT_CLOUD', CURRENT_DATE(), CURRENT_DATE() WHERE '{{ this.name }}' != 'go_process_audit'",
    post_hook="UPDATE {{ ref('go_process_audit') }} SET end_time = CURRENT_TIMESTAMP(), status = 'COMPLETED', records_processed = (SELECT COUNT(*) FROM {{ this }}), records_successful = (SELECT COUNT(*) FROM {{ this }}), processing_duration_seconds = DATEDIFF('second', start_time, CURRENT_TIMESTAMP()) WHERE execution_id = '{{ dbt_utils.generate_surrogate_key(['invocation_id', 'this.name']) }}' AND '{{ this.name }}' != 'go_process_audit'"
) }}

-- Gold Organization Dimension Table
-- Creates organization dimension from user company information

WITH organization_base AS (
    SELECT DISTINCT
        company AS organization_name,
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
        {{ dbt_utils.generate_surrogate_key(['organization_name']) }} AS organization_dim_id,
        {{ dbt_utils.generate_surrogate_key(['organization_name']) }} AS organization_id,
        TRIM(organization_name) AS organization_name,
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
        CURRENT_DATE() AS update_date,
        source_system
    FROM organization_base
)

SELECT
    organization_dim_id::VARCHAR(50) AS organization_dim_id,
    organization_id::VARCHAR(50) AS organization_id,
    organization_name::VARCHAR(500) AS organization_name,
    industry_classification::VARCHAR(200) AS industry_classification,
    organization_size::VARCHAR(50) AS organization_size,
    primary_contact_email::VARCHAR(320) AS primary_contact_email,
    billing_address::VARCHAR(1000) AS billing_address,
    account_manager_name::VARCHAR(255) AS account_manager_name,
    contract_start_date,
    contract_end_date,
    maximum_user_limit,
    storage_quota_gb,
    security_policy_level::VARCHAR(100) AS security_policy_level,
    load_date,
    update_date,
    source_system::VARCHAR(100) AS source_system
FROM organization_dimension
