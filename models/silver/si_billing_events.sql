{{ config(
    materialized='incremental',
    unique_key='event_id',
    on_schema_change='fail'
) }}

-- Transform bronze billing events to silver with data quality checks
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
        ROW_NUMBER() OVER (PARTITION BY event_id ORDER BY update_timestamp DESC, load_timestamp DESC) as rn
    FROM {{ source('bronze', 'bz_billing_events') }}
    WHERE event_id IS NOT NULL
),

-- Data quality validation and transformation
validated_billing_events AS (
    SELECT 
        event_id,
        user_id,
        CASE 
            WHEN event_type IN ('Subscription Fee', 'Subscription Renewal', 'Add-on Purchase', 'Refund') THEN event_type
            ELSE 'Other' -- Standardize unknown event types
        END AS event_type,
        amount,
        event_date,
        load_timestamp,
        update_timestamp,
        source_system,
        -- Data quality score calculation
        CASE 
            WHEN user_id IS NOT NULL 
                AND event_type IN ('Subscription Fee', 'Subscription Renewal', 'Add-on Purchase', 'Refund')
                AND amount >= 0
                AND event_date IS NOT NULL
            THEN 1.00
            ELSE 0.75
        END AS data_quality_score,
        -- Record status
        CASE 
            WHEN user_id IS NULL OR amount IS NULL OR event_date IS NULL THEN 'error'
            WHEN amount < 0 THEN 'error'
            ELSE 'active'
        END AS record_status
    FROM bronze_billing_events
    WHERE rn = 1  -- Deduplication: keep latest record
        AND user_id IS NOT NULL
        AND amount >= 0
        AND event_date IS NOT NULL
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
    DATE(load_timestamp) AS load_date,
    DATE(update_timestamp) AS update_date,
    data_quality_score,
    record_status
FROM validated_billing_events

{% if is_incremental() %}
    WHERE update_timestamp > (SELECT MAX(update_timestamp) FROM {{ this }})
{% endif %}
