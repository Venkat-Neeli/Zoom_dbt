{{
  config(
    materialized='table',
    pre_hook="INSERT INTO {{ ref('bz_audit_log') }} (source_table, load_timestamp, processed_by, processing_time, status) SELECT 'bz_billing_events', CURRENT_TIMESTAMP(), 'DBT_SYSTEM', 0, 'STARTED' WHERE '{{ this.name }}' != 'bz_audit_log'",
    post_hook="INSERT INTO {{ ref('bz_audit_log') }} (source_table, load_timestamp, processed_by, processing_time, status) SELECT 'bz_billing_events', CURRENT_TIMESTAMP(), 'DBT_SYSTEM', DATEDIFF('second', (SELECT MAX(load_timestamp) FROM {{ ref('bz_audit_log') }} WHERE source_table = 'bz_billing_events' AND status = 'STARTED'), CURRENT_TIMESTAMP()), 'COMPLETED' WHERE '{{ this.name }}' != 'bz_audit_log'"
  )
}}

WITH source_data AS (
    SELECT 
        event_id,
        user_id,
        event_type,
        amount,
        event_date,
        load_timestamp,
        update_timestamp,
        source_system
    FROM {{ source('RAW', 'billing_events') }}
    WHERE event_id IS NOT NULL -- Basic data quality check
),

final AS (
    SELECT 
        event_id,
        user_id,
        event_type,
        amount,
        event_date,
        COALESCE(load_timestamp, CURRENT_TIMESTAMP()) as load_timestamp,
        COALESCE(update_timestamp, CURRENT_TIMESTAMP()) as update_timestamp,
        COALESCE(source_system, 'ZOOM_PLATFORM') as source_system
    FROM source_data
)

SELECT * FROM final
