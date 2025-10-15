{{ config(
    materialized='table',
    pre_hook="{% if this.name != 'bz_audit_log' %}INSERT INTO {{ ref('bz_audit_log') }} (source_table, load_timestamp, processed_by, processing_time, status) VALUES ('BZ_WEBINARS', CURRENT_TIMESTAMP(), 'DBT_SYSTEM', 0, 'STARTED'){% endif %}",
    post_hook="{% if this.name != 'bz_audit_log' %}INSERT INTO {{ ref('bz_audit_log') }} (source_table, load_timestamp, processed_by, processing_time, status) VALUES ('BZ_WEBINARS', CURRENT_TIMESTAMP(), 'DBT_SYSTEM', DATEDIFF('second', (SELECT MAX(load_timestamp) FROM {{ ref('bz_audit_log') }} WHERE source_table = 'BZ_WEBINARS' AND status = 'STARTED'), CURRENT_TIMESTAMP()), 'COMPLETED'){% endif %}"
) }}

-- Raw to Bronze transformation for Webinars table
WITH source_data AS (
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

-- Data quality checks and transformations
cleaned_data AS (
    SELECT 
        COALESCE(webinar_id, 'UNKNOWN') as webinar_id,
        COALESCE(host_id, 'UNKNOWN') as host_id,
        COALESCE(webinar_topic, 'UNKNOWN') as webinar_topic,
        start_time,
        end_time,
        COALESCE(registrants, 0) as registrants,
        CURRENT_TIMESTAMP()::TIMESTAMP_NTZ as load_timestamp,
        CURRENT_TIMESTAMP()::TIMESTAMP_NTZ as update_timestamp,
        'ZOOM_PLATFORM'::STRING as source_system
    FROM source_data
)

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
FROM cleaned_data
