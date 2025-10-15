{{
    config(
        materialized='incremental',
        unique_key='event_id',
        on_schema_change='fail',
        pre_hook="{% if this.name != 'si_process_audit' %}INSERT INTO {{ ref('si_process_audit') }} (execution_id, pipeline_name, start_time, status, source_system, target_system, process_type, load_date, update_date) SELECT '{{ invocation_id }}', 'si_billing_events_transform', current_timestamp(), 'STARTED', 'BRONZE', 'SILVER', 'ETL', current_date(), current_date(){% endif %}",
        post_hook="{% if this.name != 'si_process_audit' %}INSERT INTO {{ ref('si_process_audit') }} (execution_id, pipeline_name, end_time, status, records_processed, source_system, target_system, process_type, load_date, update_date) SELECT '{{ invocation_id }}', 'si_billing_events_transform', current_timestamp(), 'COMPLETED', (SELECT count(*) FROM {{ this }}), 'BRONZE', 'SILVER', 'ETL', current_date(), current_date(){% endif %}"
    )
}}

-- Transform bronze billing events to silver billing events with data quality checks
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
    FROM {{ ref('bz_billing_events') }}
    WHERE event_id IS NOT NULL
        AND user_id IS NOT NULL
        AND event_type IS NOT NULL
        AND amount IS NOT NULL
        AND event_date IS NOT NULL
),

-- Data quality validation and cleansing
cleaned_billing_events AS (
    SELECT
        event_id,
        user_id,
        CASE 
            WHEN event_type IN ('Subscription Fee', 'Subscription Renewal', 'Add-on Purchase', 'Refund') THEN event_type
            ELSE 'Other'
        END AS event_type,
        amount,
        event_date,
        load_timestamp,
        update_timestamp,
        source_system,
        -- Calculate data quality score
        CASE 
            WHEN amount >= 0 
                AND event_type IN ('Subscription Fee', 'Subscription Renewal', 'Add-on Purchase', 'Refund')
            THEN 1.00
            ELSE 0.50
        END AS data_quality_score,
        CASE 
            WHEN amount >= 0
            THEN 'active'
            ELSE 'error'
        END AS record_status
    FROM bronze_billing_events
    WHERE rn = 1  -- Deduplication: keep latest record
        AND amount >= 0
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
FROM cleaned_billing_events

{% if is_incremental() %}
  WHERE update_timestamp > (SELECT COALESCE(MAX(update_timestamp), '1900-01-01') FROM {{ this }})
{% endif %}
