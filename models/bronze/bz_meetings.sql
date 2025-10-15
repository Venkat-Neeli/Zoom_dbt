{{ config(
    materialized='table',
    pre_hook="{% if this.name != 'bz_audit_log' %}INSERT INTO {{ ref('bz_audit_log') }} (source_table, load_timestamp, processed_by, processing_time, status) VALUES ('BZ_MEETINGS', CURRENT_TIMESTAMP(), 'DBT_SYSTEM', 0, 'STARTED'){% endif %}",
    post_hook="{% if this.name != 'bz_audit_log' %}INSERT INTO {{ ref('bz_audit_log') }} (source_table, load_timestamp, processed_by, processing_time, status) VALUES ('BZ_MEETINGS', CURRENT_TIMESTAMP(), 'DBT_SYSTEM', DATEDIFF('second', (SELECT MAX(load_timestamp) FROM {{ ref('bz_audit_log') }} WHERE source_table = 'BZ_MEETINGS' AND status = 'STARTED'), CURRENT_TIMESTAMP()), 'COMPLETED'){% endif %}"
) }}

-- Raw to Bronze transformation for Meetings table
WITH source_data AS (
    SELECT 
        meeting_id,
        host_id,
        meeting_topic,
        start_time,
        end_time,
        duration_minutes,
        load_timestamp,
        update_timestamp,
        source_system
    FROM {{ source('raw_data', 'meetings') }}
),

-- Data quality checks and transformations
cleaned_data AS (
    SELECT 
        COALESCE(meeting_id, 'UNKNOWN') as meeting_id,
        COALESCE(host_id, 'UNKNOWN') as host_id,
        COALESCE(meeting_topic, 'UNKNOWN') as meeting_topic,
        start_time,
        end_time,
        COALESCE(duration_minutes, 0) as duration_minutes,
        CURRENT_TIMESTAMP()::TIMESTAMP_NTZ as load_timestamp,
        CURRENT_TIMESTAMP()::TIMESTAMP_NTZ as update_timestamp,
        'ZOOM_PLATFORM'::STRING as source_system
    FROM source_data
)

SELECT 
    meeting_id,
    host_id,
    meeting_topic,
    start_time,
    end_time,
    duration_minutes,
    load_timestamp,
    update_timestamp,
    source_system
FROM cleaned_data
