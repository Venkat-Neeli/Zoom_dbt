{{ config(
    materialized='incremental',
    unique_key='usage_id',
    on_schema_change='fail'
) }}

-- Transform bronze feature usage to silver with data quality checks
WITH bronze_feature_usage AS (
    SELECT 
        usage_id,
        meeting_id,
        feature_name,
        usage_count,
        usage_date,
        load_timestamp,
        update_timestamp,
        source_system,
        ROW_NUMBER() OVER (PARTITION BY usage_id ORDER BY update_timestamp DESC, load_timestamp DESC) as rn
    FROM {{ source('bronze', 'bz_feature_usage') }}
    WHERE usage_id IS NOT NULL
),

-- Data quality validation and transformation
validated_feature_usage AS (
    SELECT 
        usage_id,
        meeting_id,
        CASE 
            WHEN feature_name IN ('Screen Sharing', 'Chat', 'Recording', 'Whiteboard', 'Virtual Background') THEN feature_name
            ELSE 'Other' -- Standardize unknown features
        END AS feature_name,
        usage_count,
        usage_date,
        load_timestamp,
        update_timestamp,
        source_system,
        -- Data quality score calculation
        CASE 
            WHEN meeting_id IS NOT NULL 
                AND feature_name IN ('Screen Sharing', 'Chat', 'Recording', 'Whiteboard', 'Virtual Background')
                AND usage_count >= 0
                AND usage_date IS NOT NULL
            THEN 1.00
            ELSE 0.75
        END AS data_quality_score,
        -- Record status
        CASE 
            WHEN meeting_id IS NULL OR usage_count IS NULL OR usage_date IS NULL THEN 'error'
            WHEN usage_count < 0 THEN 'error'
            ELSE 'active'
        END AS record_status
    FROM bronze_feature_usage
    WHERE rn = 1  -- Deduplication: keep latest record
        AND meeting_id IS NOT NULL
        AND usage_count >= 0
        AND usage_date IS NOT NULL
)

SELECT 
    usage_id,
    meeting_id,
    feature_name,
    usage_count,
    usage_date,
    load_timestamp,
    update_timestamp,
    source_system,
    DATE(load_timestamp) AS load_date,
    DATE(update_timestamp) AS update_date,
    data_quality_score,
    record_status
FROM validated_feature_usage

{% if is_incremental() %}
    WHERE update_timestamp > (SELECT MAX(update_timestamp) FROM {{ this }})
{% endif %}
