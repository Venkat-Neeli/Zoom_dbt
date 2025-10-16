{{ config(
    materialized='incremental',
    unique_key='usage_id',
    on_schema_change='fail',
    tags=['silver', 'feature_usage'],
    pre_hook="INSERT INTO {{ ref('si_process_audit') }} (execution_id, pipeline_name, start_time, status, source_system, target_system, process_type, load_date, update_date) SELECT '{{ dbt_utils.generate_surrogate_key(['CURRENT_TIMESTAMP()', 'RANDOM()']) }}', 'si_feature_usage_transformation', CURRENT_TIMESTAMP(), 'STARTED', 'BRONZE', 'SILVER', 'ETL', CURRENT_DATE(), CURRENT_DATE() WHERE '{{ this.name }}' != 'si_process_audit'",
    post_hook="INSERT INTO {{ ref('si_process_audit') }} (execution_id, pipeline_name, end_time, status, records_processed, source_system, target_system, process_type, load_date, update_date) SELECT '{{ dbt_utils.generate_surrogate_key(['CURRENT_TIMESTAMP()', 'RANDOM()']) }}', 'si_feature_usage_transformation', CURRENT_TIMESTAMP(), 'COMPLETED', (SELECT COUNT(*) FROM {{ this }}), 'BRONZE', 'SILVER', 'ETL', CURRENT_DATE(), CURRENT_DATE() WHERE '{{ this.name }}' != 'si_process_audit'"
) }}

WITH bronze_feature_usage AS (
    SELECT 
        bfu.usage_id,
        bfu.meeting_id,
        bfu.feature_name,
        bfu.usage_count,
        bfu.usage_date,
        bfu.load_timestamp,
        bfu.update_timestamp,
        bfu.source_system,
        ROW_NUMBER() OVER (
            PARTITION BY bfu.usage_id 
            ORDER BY bfu.update_timestamp DESC, 
                     bfu.load_timestamp DESC
        ) AS row_num
    FROM {{ source('bronze', 'bz_feature_usage') }} bfu
    WHERE bfu.usage_id IS NOT NULL
      AND bfu.meeting_id IS NOT NULL
      AND bfu.feature_name IS NOT NULL
      AND bfu.usage_count >= 0
      AND bfu.feature_name IN ('Screen Sharing', 'Chat', 'Recording', 'Whiteboard', 'Virtual Background')
),

cleaned_feature_usage AS (
    SELECT 
        usage_id,
        meeting_id,
        CASE 
            WHEN feature_name = 'Screen Share' THEN 'Screen Sharing'
            WHEN feature_name = 'Messaging' THEN 'Chat'
            ELSE feature_name
        END AS feature_name,
        usage_count,
        usage_date,
        load_timestamp,
        update_timestamp,
        source_system,
        DATE(load_timestamp) AS load_date,
        DATE(update_timestamp) AS update_date,
        -- Data Quality Score
        CASE 
            WHEN usage_id IS NOT NULL 
                 AND meeting_id IS NOT NULL 
                 AND feature_name IS NOT NULL 
                 AND usage_count >= 0
            THEN 1.0
            ELSE 0.8
        END AS data_quality_score,
        'active' AS record_status
    FROM bronze_feature_usage
    WHERE row_num = 1
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
FROM cleaned_feature_usage

{% if is_incremental() %}
  WHERE update_timestamp > (SELECT MAX(update_timestamp) FROM {{ this }})
{% endif %}
