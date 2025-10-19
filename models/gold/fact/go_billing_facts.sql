{{ config(
    materialized='table',
    cluster_by=['event_date', 'user_id'],
    pre_hook="INSERT INTO {{ ref('audit_log') }} (table_name, operation, timestamp) VALUES ('go_billing_facts', 'transform_start', CURRENT_TIMESTAMP())",
    post_hook="INSERT INTO {{ ref('audit_log') }} (table_name, operation, timestamp) VALUES ('go_billing_facts', 'transform_complete', CURRENT_TIMESTAMP())"
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
        AND data_quality_score >= 0.8
),

license_context AS (
    SELECT 
        assigned_to_user_id,
        license_type,
        start_date,
        end_date,
        ROW_NUMBER() OVER (PARTITION BY assigned_to_user_id ORDER BY start_date DESC) as rn
    FROM {{ ref('si_licenses') }}
    WHERE record_status = 'ACTIVE'
        AND data_quality_score >= 0.8
),

final AS (
    SELECT 
        -- Primary Keys
        CONCAT('BF_', bb.event_id, '_', bb.user_id) as billing_fact_id,
        bb.event_id,
        bb.user_id,
        
        -- Organization Context
        COALESCE(uc.company, 'INDIVIDUAL') as organization_id,
        
        -- Event Details
        UPPER(TRIM(bb.event_type)) as event_type,
        ROUND(bb.amount, 2) as amount,
        bb.event_date,
        
        -- Time Dimensions
        EXTRACT(YEAR FROM bb.event_date) as billing_year,
        EXTRACT(MONTH FROM bb.event_date) as billing_month,
        EXTRACT(QUARTER FROM bb.event_date) as billing_quarter,
        DATE_TRUNC('month', bb.event_date) as billing_period_start,
        LAST_DAY(bb.event_date) as billing_period_end,
        
        -- Payment Details
        'Credit Card' as payment_method,
        CASE 
            WHEN bb.amount > 0 THEN 'Completed' 
            ELSE 'Refunded' 
        END as transaction_status,
        'USD' as currency_code,
        
        -- Calculated Financial Metrics
        ROUND(bb.amount * 0.08, 2) as tax_amount,
        0.00 as discount_amount,
        ROUND(bb.amount, 2) as net_amount_before_tax,
        
        -- User and License Context
        uc.user_name,
        uc.email,
        uc.plan_type,
        lc.license_type,
        
        -- Revenue Classification
        CASE 
            WHEN bb.event_type ILIKE '%subscription%' THEN 'RECURRING'
            WHEN bb.event_type ILIKE '%upgrade%' OR bb.event_type ILIKE '%addon%' THEN 'EXPANSION'
            WHEN bb.amount < 0 THEN 'LOSS'
            ELSE 'OTHER'
        END as revenue_category,
        
        -- Amount Classification
        CASE 
            WHEN bb.amount >= 1000 THEN 'HIGH_VALUE'
            WHEN bb.amount >= 100 THEN 'MEDIUM_VALUE'
            WHEN bb.amount > 0 THEN 'LOW_VALUE'
            ELSE 'NEGATIVE'
        END as amount_category,
        
        -- Net Revenue Impact
        CASE 
            WHEN bb.amount > 0 THEN bb.amount
            ELSE bb.amount * -1
        END as net_revenue_impact,
        
        -- Audit Fields
        bb.load_date,
        CURRENT_DATE() as update_date,
        bb.source_system,
        CURRENT_TIMESTAMP() as created_at,
        CURRENT_TIMESTAMP() as updated_at
        
    FROM billing_base bb
    LEFT JOIN user_context uc ON bb.user_id = uc.user_id
    LEFT JOIN license_context lc ON bb.user_id = lc.assigned_to_user_id AND lc.rn = 1
)

SELECT * FROM final
