{{ config(
    materialized='incremental',
    unique_key='event_id',
    on_schema_change='fail'
) }}

-- Silver Billing Events Table Transformation
WITH bronze_billing_events AS (
    SELECT 
        event_id,
        user_id,
        event_type,
        amount,
        event_date,
        load_timestamp,
        update_timestamp,
        source_system,
        ROW_NUMBER() OVER (
            PARTITION BY event_id 
            ORDER BY update_timestamp DESC, 
                     load_timestamp DESC
        ) AS row_num
    FROM {{ source('bronze', 'bz_billing_events') }}
    WHERE event_id IS NOT NULL
),

data_quality_checks AS (
    SELECT 
        *,
        -- Calculate data quality score
        CASE 
            WHEN event_id IS NULL THEN 0.0
            WHEN user_id IS NULL THEN 0.2
            WHEN event_type IS NULL OR TRIM(event_type) = '' THEN 0.3
            WHEN event_type NOT IN ('Subscription Fee', 'Subscription Renewal', 'Add-on Purchase', 'Refund') THEN 0.4
            WHEN amount IS NULL OR amount < 0 THEN 0.5
            WHEN event_date IS NULL THEN 0.6
            ELSE 1.0
        END AS data_quality_score,
        
        -- Set record status
        CASE 
            WHEN event_id IS NULL OR user_id IS NULL 
                 OR event_type IS NULL OR TRIM(event_type) = ''
                 OR amount IS NULL OR amount < 0
                 OR event_date IS NULL THEN 'error'
            ELSE 'active'
        END AS record_status
    FROM bronze_billing_events
    WHERE row_num = 1
),

final_transform AS (
    SELECT 
        event_id,
        user_id,
        CASE 
            WHEN UPPER(TRIM(event_type)) = 'SUBSCRIPTION FEE' THEN 'Subscription Fee'
            WHEN UPPER(TRIM(event_type)) = 'SUBSCRIPTION RENEWAL' THEN 'Subscription Renewal'
            WHEN UPPER(TRIM(event_type)) = 'ADD-ON PURCHASE' THEN 'Add-on Purchase'
            WHEN UPPER(TRIM(event_type)) = 'REFUND' THEN 'Refund'
            ELSE TRIM(event_type)
        END AS event_type,
        amount,
        event_date,
        load_timestamp,
        update_timestamp,
        source_system,
        DATE(load_timestamp) AS load_date,
        DATE(update_timestamp) AS update_date,
        data_quality_score,
        record_status
    FROM data_quality_checks
    WHERE record_status = 'active'  -- Only pass clean records to Silver
)

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
FROM final_transform

{% if is_incremental() %}
    WHERE update_timestamp > (SELECT COALESCE(MAX(update_timestamp), '1900-01-01') FROM {{ this }})
{% endif %}
