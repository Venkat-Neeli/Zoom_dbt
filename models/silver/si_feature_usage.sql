{{ config(
    materialized='incremental',
    unique_key='usage_id',
    pre_hook="INSERT INTO {{ ref('si_process_audit') }} (execution_id, pipeline_name, start_time, status, source_system, target_system, process_type, load_date, update_date) SELECT '{{ dbt_utils.generate_surrogate_key([invocation_id, 'feature_usage']) }}', 'si_feature_usage_transform', CURRENT_TIMESTAMP(), 'RUNNING', 'BRONZE', 'SILVER', 'TRANSFORMATION', CURRENT_DATE(), CURRENT_DATE() WHERE '{{ this.name }}' != 'si_process_audit'",
    post_hook="UPDATE {{ ref('si_process_audit') }} SET end_time = CURRENT_TIMESTAMP(), status = 'SUCCESS', records_processed = (SELECT COUNT(*) FROM {{ this }}), records_successful = (SELECT COUNT(*) FROM {{ this }}), processing_duration_seconds = DATEDIFF('second', start_time, CURRENT_TIMESTAMP()) WHERE execution_id = '{{ dbt_utils.generate_surrogate_key([invocation_id, 'feature_usage']) }}' AND '{{ this.name }}' != 'si_process_audit'"
) }}

-- Data Quality and Transformation Logic for Feature Usage
WITH bronze_feature_usage AS (
    SELECT *
    FROM {{ source('bronze', 'bz_feature_usage') }}
    {% if is_incremental() %}
        WHERE update_timestamp > (SELECT MAX(update_timestamp) FROM {{ this }})
    {% endif %}
),

-- Data Quality Checks and Cleansing
cleansed_feature_usage AS (
    SELECT 
        usage_id,
        meeting_id,
        CASE 
            WHEN feature_name IN ('Screen Sharing', 'Chat', 'Recording', 'Whiteboard', 'Virtual Background') THEN feature_name
            ELSE 'Other' -- Standardize unknown features
        END AS feature_name,
        CASE 
            WHEN usage_count < 0 THEN 0
            ELSE usage_count
        END AS usage_count,
        usage_date,
        load_timestamp,
        update_timestamp,
        source_system,
        -- Derived columns
        DATE(load_timestamp) AS load_date,
        DATE(update_timestamp) AS update_date,
        -- Data Quality Score Calculation
        CASE 
            WHEN usage_id IS NOT NULL 
                AND meeting_id IS NOT NULL 
                AND feature_name IS NOT NULL
                AND usage_count >= 0
                AND usage_date IS NOT NULL
            THEN 1.00
            ELSE 0.50
        END AS data_quality_score,
        -- Record Status
        CASE 
            WHEN usage_id IS NULL OR meeting_id IS NULL OR feature_name IS NULL OR usage_date IS NULL THEN 'error'
            ELSE 'active'
        END AS record_status,
        -- Deduplication ranking
        ROW_NUMBER() OVER (
            PARTITION BY usage_id 
            ORDER BY update_timestamp DESC, 
                     (CASE WHEN meeting_id IS NOT NULL THEN 1 ELSE 0 END +
                      CASE WHEN feature_name IS NOT NULL THEN 1 ELSE 0 END +
                      CASE WHEN usage_count IS NOT NULL THEN 1 ELSE 0 END) DESC,
                     usage_id DESC
        ) AS row_rank
    FROM bronze_feature_usage
    WHERE usage_id IS NOT NULL
),

-- Final deduplicated and validated data
final_feature_usage AS (
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
    FROM cleansed_feature_usage
    WHERE row_rank = 1
        AND record_status = 'active'
)

SELECT * FROM final_feature_usage
