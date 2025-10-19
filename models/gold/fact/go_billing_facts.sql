{{ config(
    materialized='table',
    cluster_by=['load_date']
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
        AND data_quality_score >= 0.8
        AND event_id IS NOT NULL
        AND user_id IS NOT NULL
        AND amount IS NOT NULL
),

user_context AS (
    SELECT 
        user_id,
        user_name,
        email,
        company,
        plan_type
    FROM {{ ref('si_users') }}
    WHERE record_status = 'ACTIVE'
),

license_context AS (
    SELECT 
        assigned_to_user_id,
        license_type,
        start_date as license_start_date,
        end_date as license_end_date
    FROM {{ ref('si_licenses') }}
    WHERE record_status = 'ACTIVE'
)

SELECT 
    -- Primary Keys
    bb.event_id,
    bb.user_id,
    
    -- Billing Details
    bb.event_type,
    bb.amount,
    bb.event_date,
    
    -- User Context
    uc.user_name,
    uc.email,
    uc.company,
    uc.plan_type,
    
    -- License Context
    lc.license_type,
    lc.license_start_date,
    lc.license_end_date,
    
    -- Amount Classifications
    CASE 
        WHEN bb.amount <= 0 THEN 'CREDIT_OR_FREE'
        WHEN bb.amount <= 50 THEN 'LOW_VALUE'
        WHEN bb.amount <= 200 THEN 'MEDIUM_VALUE'
        WHEN bb.amount <= 500 THEN 'HIGH_VALUE'
        ELSE 'ENTERPRISE_VALUE'
    END as amount_category,
    
    -- Event Type Classifications
    CASE 
        WHEN bb.event_type IN ('SUBSCRIPTION', 'RENEWAL') THEN 'RECURRING_REVENUE'
        WHEN bb.event_type IN ('UPGRADE', 'ADD_ON') THEN 'EXPANSION_REVENUE'
        WHEN bb.event_type IN ('REFUND', 'CHARGEBACK') THEN 'REVENUE_LOSS'
        ELSE 'OTHER_REVENUE'
    END as revenue_category,
    
    -- Time Dimensions
    DATE(bb.event_date) as billing_date,
    EXTRACT(MONTH FROM bb.event_date) as billing_month,
    EXTRACT(YEAR FROM bb.event_date) as billing_year,
    EXTRACT(QUARTER FROM bb.event_date) as billing_quarter,
    DAYOFWEEK(bb.event_date) as billing_day_of_week,
    
    -- Calculated Fields
    CASE 
        WHEN bb.event_type IN ('REFUND', 'CHARGEBACK') THEN bb.amount * -1
        ELSE bb.amount
    END as net_revenue_impact,
    
    -- Quality and Audit Fields
    bb.data_quality_score,
    bb.source_system,
    bb.load_date,
    bb.update_date,
    CURRENT_TIMESTAMP() as created_at,
    CURRENT_TIMESTAMP() as updated_at
    
FROM billing_base bb
LEFT JOIN user_context uc ON bb.user_id = uc.user_id
LEFT JOIN license_context lc ON bb.user_id = lc.assigned_to_user_id
