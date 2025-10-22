{{ config(
    materialized='table'
) }}

WITH usage_base AS (
    SELECT 
        DATE(usage_date) AS usage_date,
        load_date,
        source_system,
        COUNT(*) AS feature_usage_count
    FROM {{ ref('si_feature_usage') }}
    WHERE record_status = 'ACTIVE'
    GROUP BY DATE(usage_date), load_date, source_system
),

user_base AS (
    SELECT 
        user_id,
        company AS organization_id,
        load_date,
        source_system
    FROM {{ ref('si_users') }}
    WHERE record_status = 'ACTIVE'
)

SELECT 
    CONCAT('UF_', ub.user_id, '_', ug.usage_date::STRING) AS usage_fact_id,
    ub.user_id,
    COALESCE(ub.organization_id, 'INDIVIDUAL') AS organization_id,
    ug.usage_date,
    0 AS meeting_count,
    0 AS total_meeting_minutes,
    0 AS webinar_count,
    0 AS total_webinar_minutes,
    0.0 AS recording_storage_gb,
    COALESCE(ug.feature_usage_count, 0) AS feature_usage_count,
    0 AS unique_participants_hosted,
    COALESCE(ug.load_date, ub.load_date) AS load_date,
    CURRENT_DATE() AS update_date,
    COALESCE(ug.source_system, ub.source_system) AS source_system
FROM user_base ub
CROSS JOIN usage_base ug
