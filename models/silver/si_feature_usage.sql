{{ config(
    materialized='incremental',
    unique_key='usage_id',
    on_schema_change='fail'
) }}

-- Silver Feature Usage Table Transformation
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
        ROW_NUMBER() OVER (
            PARTITION BY usage_id 
            ORDER BY update_timestamp DESC, 
                     load_timestamp DESC
        ) AS row_num
    FROM {{ source('bronze', 'bz_feature_usage') }}
    WHERE usage_id IS NOT NULL
),

data_quality_checks AS (
    SELECT 
        *,
        -- Calculate data quality score
        CASE 
            WHEN usage_id IS NULL THEN 0.0
            WHEN meeting_id IS NULL THEN 0.2
            WHEN feature_name IS NULL OR TRIM(feature_name) = '' THEN 0.3
            WHEN feature_name NOT IN ('Screen Sharing', 'Chat', 'Recording', 'Whiteboard', 'Virtual Background') THEN 0.4
            WHEN usage_count IS NULL OR usage_count < 0 THEN 0.5
            WHEN usage_date IS NULL THEN 0.6
            ELSE 1.0
        END AS data_quality_score,
        
        -- Set record status
        CASE 
            WHEN usage_id IS NULL OR meeting_id IS NULL 
                 OR feature_name IS NULL OR TRIM(feature_name) = ''
                 OR usage_count IS NULL OR usage_count < 0
                 OR usage_date IS NULL THEN 'error'
            ELSE 'active'
        END AS record_status
    FROM bronze_feature_usage
    WHERE row_num = 1
),

final_transform AS (
    SELECT 
        usage_id,
        meeting_id,
        CASE 
            WHEN UPPER(TRIM(feature_name)) = 'SCREEN SHARING' THEN 'Screen Sharing'
            WHEN UPPER(TRIM(feature_name)) = 'CHAT' THEN 'Chat'
            WHEN UPPER(TRIM(feature_name)) = 'RECORDING' THEN 'Recording'
            WHEN UPPER(TRIM(feature_name)) = 'WHITEBOARD' THEN 'Whiteboard'
            WHEN UPPER(TRIM(feature_name)) = 'VIRTUAL BACKGROUND' THEN 'Virtual Background'
            ELSE TRIM(feature_name)
        END AS feature_name,
        usage_count,
        usage_date,
        load_timestamp,
        update_timestamp,
        source_system,
        DATE(load_timestamp) AS load_date,
        DATE(update_timestamp) AS update_date,
        data_quality_score,
        record_status
    FROM data_quality_checks
    WHERE record_status = 'active'  -- Only pass clean records to Silver
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
    load_date,
    update_date,
    data_quality_score,
    record_status
FROM final_transform

{% if is_incremental() %}
    WHERE update_timestamp > (SELECT COALESCE(MAX(update_timestamp), '1900-01-01') FROM {{ this }})
{% endif %}
