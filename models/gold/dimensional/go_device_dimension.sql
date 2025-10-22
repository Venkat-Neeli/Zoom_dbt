{{ config(
    materialized='table',
    cluster_by=['device_type', 'load_date'],
    tags=['dimension', 'gold'],
    pre_hook="INSERT INTO {{ ref('go_process_audit') }} (execution_id, pipeline_name, process_type, start_time, status, source_system, target_system, user_executed, server_name, load_date, update_date) SELECT '{{ dbt_utils.generate_surrogate_key(['invocation_id', 'this.name']) }}', 'GO_DEVICE_DIMENSION', 'DIMENSION_BUILD', CURRENT_TIMESTAMP(), 'STARTED', 'SILVER', 'GOLD', CURRENT_USER(), 'DBT_CLOUD', CURRENT_DATE(), CURRENT_DATE() WHERE '{{ this.name }}' != 'go_process_audit'",
    post_hook="UPDATE {{ ref('go_process_audit') }} SET end_time = CURRENT_TIMESTAMP(), status = 'COMPLETED', records_processed = (SELECT COUNT(*) FROM {{ this }}), records_successful = (SELECT COUNT(*) FROM {{ this }}), processing_duration_seconds = DATEDIFF('second', start_time, CURRENT_TIMESTAMP()) WHERE execution_id = '{{ dbt_utils.generate_surrogate_key(['invocation_id', 'this.name']) }}' AND '{{ this.name }}' != 'go_process_audit'"
) }}

-- Gold Device Dimension Table
-- Creates device dimension with default values since device info not available in Silver

WITH device_base AS (
    SELECT DISTINCT
        participant_id,
        source_system,
        load_date,
        update_date
    FROM {{ source('silver', 'si_participants') }}
    WHERE record_status = 'ACTIVE'
),

device_dimension AS (
    SELECT
        {{ dbt_utils.generate_surrogate_key(['participant_id']) }} AS device_dim_id,
        {{ dbt_utils.generate_surrogate_key(['participant_id']) }} AS device_connection_id,
        'Desktop' AS device_type,
        'Unknown' AS operating_system,
        'Unknown' AS application_version,
        'WiFi' AS network_connection_type,
        'Computer' AS device_category,
        'Desktop' AS platform_family,
        load_date,
        CURRENT_DATE() AS update_date,
        source_system
    FROM device_base
)

SELECT
    device_dim_id::VARCHAR(50) AS device_dim_id,
    device_connection_id::VARCHAR(50) AS device_connection_id,
    device_type::VARCHAR(100) AS device_type,
    operating_system::VARCHAR(100) AS operating_system,
    application_version::VARCHAR(50) AS application_version,
    network_connection_type::VARCHAR(50) AS network_connection_type,
    device_category::VARCHAR(50) AS device_category,
    platform_family::VARCHAR(50) AS platform_family,
    load_date,
    update_date,
    source_system::VARCHAR(100) AS source_system
FROM device_dimension
