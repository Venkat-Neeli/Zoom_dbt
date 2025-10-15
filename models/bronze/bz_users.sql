{{ config(
    materialized='table',
    pre_hook="{% if this.name != 'bz_audit_log' %}INSERT INTO {{ ref('bz_audit_log') }} (source_table, load_timestamp, processed_by, processing_time, status) VALUES ('BZ_USERS', CURRENT_TIMESTAMP(), 'DBT_SYSTEM', 0, 'STARTED'){% endif %}",
    post_hook="{% if this.name != 'bz_audit_log' %}INSERT INTO {{ ref('bz_audit_log') }} (source_table, load_timestamp, processed_by, processing_time, status) VALUES ('BZ_USERS', CURRENT_TIMESTAMP(), 'DBT_SYSTEM', DATEDIFF('second', (SELECT MAX(load_timestamp) FROM {{ ref('bz_audit_log') }} WHERE source_table = 'BZ_USERS' AND status = 'STARTED'), CURRENT_TIMESTAMP()), 'COMPLETED'){% endif %}"
) }}

-- Raw to Bronze transformation for Users table
WITH source_data AS (
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

-- Data quality checks and transformations
cleaned_data AS (
    SELECT 
        COALESCE(user_id, 'UNKNOWN') as user_id,
        COALESCE(user_name, 'UNKNOWN') as user_name,
        COALESCE(email, 'UNKNOWN') as email,
        COALESCE(company, 'UNKNOWN') as company,
        COALESCE(plan_type, 'UNKNOWN') as plan_type,
        CURRENT_TIMESTAMP()::TIMESTAMP_NTZ as load_timestamp,
        CURRENT_TIMESTAMP()::TIMESTAMP_NTZ as update_timestamp,
        'ZOOM_PLATFORM'::STRING as source_system
    FROM source_data
)

SELECT 
    user_id,
    user_name,
    email,
    company,
    plan_type,
    load_timestamp,
    update_timestamp,
    source_system
FROM cleaned_data
