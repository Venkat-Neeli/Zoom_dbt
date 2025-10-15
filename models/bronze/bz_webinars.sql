{{ config(
    materialized='table',
    pre_hook="INSERT INTO {{ ref('bz_audit_log') }} (source_table, load_timestamp, processed_by, processing_time, status) SELECT 'bz_webinars', CURRENT_TIMESTAMP(), 'DBT_SYSTEM', 0, 'STARTED' WHERE '{{ this.name }}' != 'bz_audit_log'",
    post_hook="INSERT INTO {{ ref('bz_audit_log') }} (source_table, load_timestamp, processed_by, processing_time, status) SELECT 'bz_webinars', CURRENT_TIMESTAMP(), 'DBT_SYSTEM', DATEDIFF('second', (SELECT MAX(load_timestamp) FROM {{ ref('bz_audit_log') }} WHERE source_table = 'bz_webinars' AND status = 'STARTED'), CURRENT_TIMESTAMP()), 'COMPLETED' WHERE '{{ this.name }}' != 'bz_audit_log'"
) }}

-- Bronze layer transformation for webinars table
-- This model performs 1:1 mapping from RAW.webinars to BRONZE.bz_webinars

WITH source_data AS (
    -- Extract data from raw webinars table
    SELECT 
        webinar_id,
        host_id,
        webinar_topic,
        start_time,
        end_time,
        registrants,
        load_timestamp,
        update_timestamp,
        source_system
    FROM {{ source('raw_data', 'webinars') }}
),

data_quality_checks AS (
    -- Apply data quality validations
    SELECT 
        webinar_id,
        host_id,
        COALESCE(webinar_topic, 'NO_TOPIC') as webinar_topic,
        start_time,
        end_time,
        COALESCE(registrants, 0) as registrants,
        load_timestamp,
        update_timestamp,
        COALESCE(source_system, 'ZOOM_PLATFORM') as source_system
    FROM source_data
    WHERE webinar_id IS NOT NULL
)

-- Final select with audit columns
SELECT 
    webinar_id,
    host_id,
    webinar_topic,
    start_time,
    end_time,
    registrants,
    CURRENT_TIMESTAMP() as load_timestamp,
    CURRENT_TIMESTAMP() as update_timestamp,
    'ZOOM_PLATFORM' as source_system
FROM data_quality_checks
