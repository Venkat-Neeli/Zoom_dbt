{{
  config(
    materialized='table',
    pre_hook="INSERT INTO {{ ref('bz_audit_log') }} (source_table, load_timestamp, processed_by, processing_time, status) SELECT 'bz_users', CURRENT_TIMESTAMP(), 'DBT_SYSTEM', 0, 'STARTED' WHERE '{{ this.name }}' != 'bz_audit_log'",
    post_hook="INSERT INTO {{ ref('bz_audit_log') }} (source_table, load_timestamp, processed_by, processing_time, status) SELECT 'bz_users', CURRENT_TIMESTAMP(), 'DBT_SYSTEM', DATEDIFF('second', (SELECT MAX(load_timestamp) FROM {{ ref('bz_audit_log') }} WHERE source_table = 'bz_users' AND status = 'STARTED'), CURRENT_TIMESTAMP()), 'COMPLETED' WHERE '{{ this.name }}' != 'bz_audit_log'"
  )
}}

-- Bronze layer transformation for users table
-- This model performs 1:1 mapping from RAW.users to BRONZE.bz_users
-- with data quality checks and audit information

WITH source_data AS (
    -- Extract data from raw users table
    SELECT 
        user_id,
        user_name,
        email,
        company,
        plan_type,
        load_timestamp,
        update_timestamp,
        source_system
    FROM {{ source('raw_data', 'users') }}
),

data_quality_checks AS (
    -- Apply data quality validations
    SELECT 
        user_id,
        COALESCE(user_name, 'UNKNOWN') as user_name,
        COALESCE(email, 'NO_EMAIL') as email,
        COALESCE(company, 'UNKNOWN') as company,
        COALESCE(plan_type, 'UNKNOWN') as plan_type,
        COALESCE(load_timestamp, CURRENT_TIMESTAMP()) as load_timestamp,
        CURRENT_TIMESTAMP() as update_timestamp,
        COALESCE(source_system, 'ZOOM_PLATFORM') as source_system
    FROM source_data
    WHERE user_id IS NOT NULL -- Filter out records with null primary key
)

-- Final select with audit columns
SELECT 
    user_id,
    user_name,
    email,
    company,
    plan_type,
    load_timestamp,
    update_timestamp,
    source_system
FROM data_quality_checks
