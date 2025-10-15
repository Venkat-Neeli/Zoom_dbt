{{ config(
    materialized='table',
    pre_hook="{% if this.name != 'bz_audit_log' %}INSERT INTO {{ ref('bz_audit_log') }} (source_table, load_timestamp, processed_by, processing_time, status) VALUES ('BZ_SUPPORT_TICKETS', CURRENT_TIMESTAMP(), 'DBT_SYSTEM', 0, 'STARTED'){% endif %}",
    post_hook="{% if this.name != 'bz_audit_log' %}INSERT INTO {{ ref('bz_audit_log') }} (source_table, load_timestamp, processed_by, processing_time, status) VALUES ('BZ_SUPPORT_TICKETS', CURRENT_TIMESTAMP(), 'DBT_SYSTEM', DATEDIFF('second', (SELECT MAX(load_timestamp) FROM {{ ref('bz_audit_log') }} WHERE source_table = 'BZ_SUPPORT_TICKETS' AND status = 'STARTED'), CURRENT_TIMESTAMP()), 'COMPLETED'){% endif %}"
) }}

-- Raw to Bronze transformation for Support Tickets table
WITH source_data AS (
    SELECT 
        ticket_id,
        user_id,
        ticket_type,
        resolution_status,
        open_date,
        load_timestamp,
        update_timestamp,
        source_system
    FROM {{ source('raw_data', 'support_tickets') }}
),

-- Data quality checks and transformations
cleaned_data AS (
    SELECT 
        COALESCE(ticket_id, 'UNKNOWN') as ticket_id,
        COALESCE(user_id, 'UNKNOWN') as user_id,
        COALESCE(ticket_type, 'UNKNOWN') as ticket_type,
        COALESCE(resolution_status, 'UNKNOWN') as resolution_status,
        open_date,
        CURRENT_TIMESTAMP()::TIMESTAMP_NTZ as load_timestamp,
        CURRENT_TIMESTAMP()::TIMESTAMP_NTZ as update_timestamp,
        'ZOOM_PLATFORM'::STRING as source_system
    FROM source_data
)

SELECT 
    ticket_id,
    user_id,
    ticket_type,
    resolution_status,
    open_date,
    load_timestamp,
    update_timestamp,
    source_system
FROM cleaned_data
