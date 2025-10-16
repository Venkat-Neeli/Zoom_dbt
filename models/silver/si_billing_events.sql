{{ config(
    materialized='incremental',
    unique_key='event_id',
    on_schema_change='fail',
    tags=['silver', 'billing_events'],
    pre_hook="INSERT INTO {{ ref('si_process_audit') }} (execution_id, pipeline_name, start_time, status, source_system, target_system, process_type, load_date, update_date) SELECT '{{ dbt_utils.generate_surrogate_key(['CURRENT_TIMESTAMP()', 'RANDOM()']) }}', 'si_billing_events_transformation', CURRENT_TIMESTAMP(), 'STARTED', 'BRONZE', 'SILVER', 'ETL', CURRENT_DATE(), CURRENT_DATE() WHERE '{{ this.name }}' != 'si_process_audit'",
    post_hook="INSERT INTO {{ ref('si_process_audit') }} (execution_id, pipeline_name, end_time, status, records_processed, source_system, target_system, process_type, load_date, update_date) SELECT '{{ dbt_utils.generate_surrogate_key(['CURRENT_TIMESTAMP()', 'RANDOM()']) }}', 'si_billing_events_transformation', CURRENT_TIMESTAMP(), 'COMPLETED', (SELECT COUNT(*) FROM {{ this }}), 'BRONZE', 'SILVER', 'ETL', CURRENT_DATE(), CURRENT_DATE() WHERE '{{ this.name }}' != 'si_process_audit'"
) }}

WITH bronze_billing_events AS (
    SELECT 
        bbe.event_id,
        bbe.user_id,
        bbe.event_type,
        bbe.amount,
        bbe.event_date,
        bbe.load_timestamp,
        bbe.update_timestamp,
        bbe.source_system,
        ROW_NUMBER() OVER (
            PARTITION BY bbe.event_id 
            ORDER BY bbe.update_timestamp DESC, 
                     bbe.load_timestamp DESC
        ) AS row_num
    FROM {{ source('bronze', 'bz_billing_events') }} bbe
    WHERE bbe.event_id IS NOT NULL
      AND bbe.user_id IS NOT NULL
      AND bbe.event_type IS NOT NULL
      AND bbe.amount >= 0
      AND bbe.event_type IN ('Subscription Fee', 'Subscription Renewal', 'Add-on Purchase', 'Refund')
),

cleaned_billing_events AS (
    SELECT 
        event_id,
        user_id,
        CASE 
            WHEN event_type = 'Monthly Fee' THEN 'Subscription Fee'
            WHEN event_type = 'Renewal' THEN 'Subscription Renewal'
            WHEN event_type = 'Add-on' THEN 'Add-on Purchase'
            ELSE event_type
        END AS event_type,
        amount,
        event_date,
        load_timestamp,
        update_timestamp,
        source_system,
        DATE(load_timestamp) AS load_date,
        DATE(update_timestamp) AS update_date,
        -- Data Quality Score
        CASE 
            WHEN event_id IS NOT NULL 
                 AND user_id IS NOT NULL 
                 AND event_type IS NOT NULL 
                 AND amount >= 0
            THEN 1.0
            ELSE 0.8
        END AS data_quality_score,
        'active' AS record_status
    FROM bronze_billing_events
    WHERE row_num = 1
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
FROM cleaned_billing_events

{% if is_incremental() %}
  WHERE update_timestamp > (SELECT MAX(update_timestamp) FROM {{ this }})
{% endif %}
