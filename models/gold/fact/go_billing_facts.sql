{{ config(
    materialized='table',
    schema='gold',
    cluster_by=['load_date'],
    pre_hook="ALTER SESSION SET TIMEZONE = 'UTC'",
    post_hook=[
        "ALTER TABLE {{ this }} SET CHANGE_TRACKING = TRUE",
        "GRANT SELECT ON {{ this }} TO ROLE ANALYTICS_READER"
    ]
) }}

WITH billing_base AS (
    SELECT 
        event_id,
        user_id,
        event_type,
        amount,
        event_date,
        load_timestamp,
        update_timestamp,
        source_system,
        load_date,
        update_date,
        data_quality_score,
        record_status
    FROM {{ ref('si_billing_events') }}
    WHERE record_status = 'ACTIVE'
        AND data_quality_score >= {{ var('min_quality_score', 3.0) }}
        AND amount IS NOT NULL
),

user_info AS (
    SELECT 
        user_id,
        user_name,
        email,
        company,
        plan_type
    FROM {{ ref('si_users') }}
    WHERE record_status = 'ACTIVE'
),

license_info AS (
    SELECT 
        assigned_to_user_id,
        license_type,
        start_date,
        end_date
    FROM {{ ref('si_licenses') }}
    WHERE record_status = 'ACTIVE'
),

billing_metrics AS (
    SELECT 
        bb.*,
        CASE 
            WHEN bb.amount >= 1000 THEN 'High Value'
            WHEN bb.amount >= 100 THEN 'Medium Value'
            WHEN bb.amount >= 10 THEN 'Low Value'
            ELSE 'Minimal Value'
        END as amount_category,
        EXTRACT(MONTH FROM bb.event_date) as billing_month,
        EXTRACT(YEAR FROM bb.event_date) as billing_year,
        EXTRACT(QUARTER FROM bb.event_date) as billing_quarter
    FROM billing_base bb
)

SELECT 
    UUID_STRING() as billing_fact_key,
    bm.event_id,
    bm.user_id,
    ui.user_name,
    ui.email,
    ui.company,
    ui.plan_type,
    li.license_type,
    bm.event_type,
    bm.amount,
    bm.amount_category,
    bm.event_date,
    bm.billing_month,
    bm.billing_year,
    bm.billing_quarter,
    CASE 
        WHEN bm.event_type IN ('SUBSCRIPTION', 'RENEWAL') THEN 'Recurring'
        WHEN bm.event_type IN ('UPGRADE', 'ADDON') THEN 'Expansion'
        WHEN bm.event_type IN ('REFUND', 'CHARGEBACK') THEN 'Negative'
        ELSE 'Other'
    END as revenue_type,
    bm.data_quality_score,
    bm.source_system,
    bm.load_date,
    bm.update_date,
    CURRENT_TIMESTAMP() as created_at,
    CURRENT_TIMESTAMP() as updated_at,
    'SUCCESS' as process_status
FROM billing_metrics bm
LEFT JOIN user_info ui ON bm.user_id = ui.user_id
LEFT JOIN license_info li ON bm.user_id = li.assigned_to_user_id
