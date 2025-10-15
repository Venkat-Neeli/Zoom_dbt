{{ config(
    materialized='table',
    pre_hook="{% if this.name != 'bz_audit_log' %}INSERT INTO {{ ref('bz_audit_log') }} (source_table, load_timestamp, processed_by, processing_time, status) VALUES ('BZ_FEATURE_USAGE', CURRENT_TIMESTAMP(), 'DBT_SYSTEM', 0, 'STARTED'){% endif %}",
    post_hook="{% if this.name != 'bz_audit_log' %}INSERT INTO {{ ref('bz_audit_log') }} (source_table, load_timestamp, processed_by, processing_time, status) VALUES ('BZ_FEATURE_USAGE', CURRENT_TIMESTAMP(), 'DBT_SYSTEM', DATEDIFF('second', (SELECT MAX(load_timestamp) FROM {{ ref('bz_audit_log') }} WHERE source_table = 'BZ_FEATURE_USAGE' AND status = 'STARTED'), CURRENT_TIMESTAMP()), 'COMPLETED'){% endif %}"
) }}

-- Raw to Bronze transformation for Feature Usage table
WITH source_data AS (
    SELECT 
        usage_id,
        meeting_id,
        feature_name,
        usage_count,
        usage_date,
        load_timestamp,
        update_timestamp,
        source_system
    FROM {{ source('raw_data', 'feature_usage') }}
),

-- Data quality checks and transformations
cleaned_data AS (
    SELECT 
        COALESCE(usage_id, 'UNKNOWN') as usage_id,
        COALESCE(meeting_id, 'UNKNOWN') as meeting_id,
        COALESCE(feature_name, 'UNKNOWN') as feature_name,
        COALESCE(usage_count, 0) as usage_count,
        usage_date,
        CURRENT_TIMESTAMP()::TIMESTAMP_NTZ as load_timestamp,
        CURRENT_TIMESTAMP()::TIMESTAMP_NTZ as update_timestamp,
        'ZOOM_PLATFORM'::STRING as source_system
    FROM source_data
)

SELECT 
    usage_id,
    meeting_id,
    feature_name,
    usage_count,
    usage_date,
    load_timestamp,
    update_timestamp,
    source_system
FROM cleaned_data
