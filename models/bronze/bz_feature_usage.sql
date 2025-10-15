{{ config(
    materialized='table',
    pre_hook="INSERT INTO {{ ref('bz_audit_log') }} (source_table, load_timestamp, processed_by, processing_time, status) SELECT 'bz_feature_usage', CURRENT_TIMESTAMP(), 'DBT_SYSTEM', 0, 'STARTED' WHERE '{{ this.name }}' != 'bz_audit_log'",
    post_hook="INSERT INTO {{ ref('bz_audit_log') }} (source_table, load_timestamp, processed_by, processing_time, status) SELECT 'bz_feature_usage', CURRENT_TIMESTAMP(), 'DBT_SYSTEM', DATEDIFF('second', (SELECT MAX(load_timestamp) FROM {{ ref('bz_audit_log') }} WHERE source_table = 'bz_feature_usage' AND status = 'STARTED'), CURRENT_TIMESTAMP()), 'COMPLETED' WHERE '{{ this.name }}' != 'bz_audit_log'"
) }}

-- Bronze layer transformation for feature_usage table
-- This model performs 1:1 mapping from RAW.feature_usage to BRONZE.bz_feature_usage

WITH source_data AS (
    -- Extract data from raw feature_usage table
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

data_quality_checks AS (
    -- Apply data quality validations
    SELECT 
        usage_id,
        meeting_id,
        COALESCE(feature_name, 'UNKNOWN_FEATURE') as feature_name,
        COALESCE(usage_count, 0) as usage_count,
        usage_date,
        load_timestamp,
        update_timestamp,
        COALESCE(source_system, 'ZOOM_PLATFORM') as source_system
    FROM source_data
    WHERE usage_id IS NOT NULL
)

-- Final select with audit columns
SELECT 
    usage_id,
    meeting_id,
    feature_name,
    usage_count,
    usage_date,
    CURRENT_TIMESTAMP() as load_timestamp,
    CURRENT_TIMESTAMP() as update_timestamp,
    'ZOOM_PLATFORM' as source_system
FROM data_quality_checks
