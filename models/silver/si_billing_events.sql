{{ config(
    materialized='incremental',
    unique_key='event_id',
    on_schema_change='fail'
) }}

WITH bronze_billing_events AS (
    SELECT *
    FROM {{ source('bronze', 'bz_billing_events') }}
    {% if is_incremental() %}
        WHERE update_timestamp > (SELECT MAX(update_timestamp) FROM {{ this }})
    {% endif %}
),

-- Data Quality and Deduplication
deduped_billing_events AS (
    SELECT *,
        ROW_NUMBER() OVER (
            PARTITION BY event_id 
            ORDER BY update_timestamp DESC, 
                     load_timestamp DESC,
                     CASE WHEN user_id IS NOT NULL THEN 1 ELSE 0 END +
                     CASE WHEN event_type IS NOT NULL THEN 1 ELSE 0 END +
                     CASE WHEN amount IS NOT NULL THEN 1 ELSE 0 END +
                     CASE WHEN event_date IS NOT NULL THEN 1 ELSE 0 END DESC
        ) AS row_rank
    FROM bronze_billing_events
),

-- Data Quality Checks and Transformations
transformed_billing_events AS (
    SELECT
        event_id,
        user_id,
        CASE 
            WHEN event_type IN ('Subscription Fee', 'Subscription Renewal', 'Add-on Purchase', 'Refund') THEN event_type
            ELSE 'Other'
        END AS event_type,
        CASE 
            WHEN amount < 0 THEN 0
            ELSE amount
        END AS amount,
        event_date,
        load_timestamp,
        update_timestamp,
        source_system,
        DATE(load_timestamp) AS load_date,
        DATE(update_timestamp) AS update_date,
        
        -- Data Quality Score Calculation
        CASE 
            WHEN event_id IS NULL OR user_id IS NULL OR event_type IS NULL OR amount IS NULL OR event_date IS NULL THEN 0.0
            WHEN amount < 0 THEN 0.5
            WHEN event_type NOT IN ('Subscription Fee', 'Subscription Renewal', 'Add-on Purchase', 'Refund') THEN 0.7
            ELSE 1.0
        END AS data_quality_score,
        
        -- Record Status
        CASE 
            WHEN event_id IS NULL OR user_id IS NULL OR event_type IS NULL OR amount IS NULL OR event_date IS NULL THEN 'error'
            ELSE 'active'
        END AS record_status
        
    FROM deduped_billing_events
    WHERE row_rank = 1
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
FROM transformed_billing_events
WHERE record_status = 'active'
  AND event_id IS NOT NULL
  AND user_id IS NOT NULL
  AND event_type IS NOT NULL
  AND amount IS NOT NULL
  AND event_date IS NOT NULL
