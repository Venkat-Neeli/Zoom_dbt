{{ config(
    materialized='table',
    tags=['gold', 'fact']
) }}

SELECT
    user_id,
    user_name,
    email,
    company,
    plan_type,
    load_date,
    update_date,
    data_quality_score,
    record_status,
    load_timestamp,
    update_timestamp,
    source_system
FROM {{ ref('si_users') }}
WHERE record_status = 'ACTIVE'
  AND data_quality_score >= 0.8
