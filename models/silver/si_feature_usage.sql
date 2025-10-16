{{ config(
    materialized='incremental',
    unique_key='usage_id',
    on_schema_change='fail'
) }}

WITH bronze_feature_usage AS (
    SELECT *
    FROM {{ source('bronze', 'bz_feature_usage') }}
    {% if is_incremental() %}
        WHERE update_timestamp > (SELECT MAX(update_timestamp) FROM {{ this }})
    {% endif %}
),

-- Data Quality and Deduplication
deduped_feature_usage AS (
    SELECT *,
        ROW_NUMBER() OVER (
            PARTITION BY usage_id 
            ORDER BY update_timestamp DESC, 
                     load_timestamp DESC,
                     CASE WHEN meeting_id IS NOT NULL THEN 1 ELSE 0 END +
                     CASE WHEN feature_name IS NOT NULL THEN 1 ELSE 0 END +
                     CASE WHEN usage_count IS NOT NULL THEN 1 ELSE 0 END +
                     CASE WHEN usage_date IS NOT NULL THEN 1 ELSE 0 END DESC
        ) AS row_rank
    FROM bronze_feature_usage
),

-- Data Quality Checks and Transformations
transformed_feature_usage AS (
    SELECT
        usage_id,
        meeting_id,
        CASE 
            WHEN feature_name IN ('Screen Sharing', 'Chat', 'Recording', 'Whiteboard', 'Virtual Background') THEN feature_name
            ELSE 'Other'
        END AS feature_name,
        CASE 
            WHEN usage_count < 0 THEN 0
            ELSE usage_count
        END AS usage_count,
        usage_date,
        load_timestamp,
        update_timestamp,
        source_system,
        DATE(load_timestamp) AS load_date,
        DATE(update_timestamp) AS update_date,
        
        -- Data Quality Score Calculation
        CASE 
            WHEN usage_id IS NULL OR meeting_id IS NULL OR feature_name IS NULL OR usage_count IS NULL OR usage_date IS NULL THEN 0.0
            WHEN usage_count < 0 THEN 0.5
            WHEN feature_name NOT IN ('Screen Sharing', 'Chat', 'Recording', 'Whiteboard', 'Virtual Background') THEN 0.7
            ELSE 1.0
        END AS data_quality_score,
        
        -- Record Status
        CASE 
            WHEN usage_id IS NULL OR meeting_id IS NULL OR feature_name IS NULL OR usage_count IS NULL OR usage_date IS NULL THEN 'error'
            ELSE 'active'
        END AS record_status
        
    FROM deduped_feature_usage
    WHERE row_rank = 1
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
FROM transformed_feature_usage
WHERE record_status = 'active'
  AND usage_id IS NOT NULL
  AND meeting_id IS NOT NULL
  AND feature_name IS NOT NULL
  AND usage_count IS NOT NULL
  AND usage_date IS NOT NULL
