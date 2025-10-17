{{
    config(
        materialized='incremental',
        unique_key='usage_id',
        on_schema_change='sync_all_columns',
        pre_hook="INSERT INTO {{ ref('si_process_audit') }} (execution_id, pipeline_name, start_time, status, source_system, target_system, process_type, load_date, update_date) SELECT '{{ dbt_utils.generate_surrogate_key(['current_timestamp()']) }}', 'si_feature_usage', CURRENT_TIMESTAMP(), 'STARTED', 'Bronze', 'Silver', 'ETL', CURRENT_DATE(), CURRENT_DATE() WHERE '{{ this.name }}' != 'si_process_audit'",
        post_hook="INSERT INTO {{ ref('si_process_audit') }} (execution_id, pipeline_name, end_time, status, records_processed, source_system, target_system, process_type, load_date, update_date) SELECT '{{ dbt_utils.generate_surrogate_key(['current_timestamp()']) }}', 'si_feature_usage', CURRENT_TIMESTAMP(), 'COMPLETED', (SELECT COUNT(*) FROM {{ this }}), 'Bronze', 'Silver', 'ETL', CURRENT_DATE(), CURRENT_DATE() WHERE '{{ this.name }}' != 'si_process_audit'"
    )
}}

-- Silver Feature Usage transformation with data quality checks
WITH source_data AS (
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
        ) as row_num
    FROM {{ source('bronze', 'bz_feature_usage') }}
    WHERE usage_id IS NOT NULL
      AND meeting_id IS NOT NULL
      AND feature_name IS NOT NULL
      AND usage_count >= 0
),

deduped_data AS (
    SELECT 
        usage_id,
        meeting_id,
        CASE 
            WHEN feature_name IN ('Screen Sharing', 'Chat', 'Recording', 'Whiteboard', 'Virtual Background') 
            THEN feature_name
            ELSE 'Other'
        END AS feature_name,
        usage_count,
        usage_date,
        load_timestamp,
        update_timestamp,
        source_system,
        DATE(load_timestamp) AS load_date,
        DATE(update_timestamp) AS update_date,
        -- Data quality score calculation
        CASE 
            WHEN usage_id IS NOT NULL 
                 AND meeting_id IS NOT NULL 
                 AND feature_name IS NOT NULL 
                 AND usage_count >= 0
            THEN 1.00
            ELSE 0.75
        END AS data_quality_score,
        'active' AS record_status
    FROM source_data
    WHERE row_num = 1
)

SELECT * FROM deduped_data

{% if is_incremental() %}
    WHERE update_timestamp > (SELECT MAX(update_timestamp) FROM {{ this }})
{% endif %}
