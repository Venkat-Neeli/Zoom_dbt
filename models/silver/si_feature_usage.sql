{{ config(
    materialized='incremental',
    unique_key='usage_id',
    on_schema_change='sync_all_columns',
    tags=['silver', 'feature_usage'],
    pre_hook="INSERT INTO {{ ref('si_process_audit') }} (execution_id, pipeline_name, start_time, status, source_system, target_system, process_type, load_date, update_date) SELECT '{{ invocation_id }}' || '_feature_usage_start', 'si_feature_usage_transform', CURRENT_TIMESTAMP(), 'STARTED', 'BRONZE', 'SILVER', 'ETL', CURRENT_DATE(), CURRENT_DATE() WHERE '{{ this.name }}' != 'si_process_audit'",
    post_hook="INSERT INTO {{ ref('si_process_audit') }} (execution_id, pipeline_name, end_time, status, records_processed, source_system, target_system, process_type, load_date, update_date) SELECT '{{ invocation_id }}' || '_feature_usage_end', 'si_feature_usage_transform', CURRENT_TIMESTAMP(), 'COMPLETED', (SELECT COUNT(*) FROM {{ this }}), 'BRONZE', 'SILVER', 'ETL', CURRENT_DATE(), CURRENT_DATE() WHERE '{{ this.name }}' != 'si_process_audit'"
) }}

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
                     load_timestamp DESC,
                     CASE WHEN usage_count IS NOT NULL THEN 1 ELSE 0 END DESC
        ) AS row_rank
    FROM {{ source('bronze', 'bz_feature_usage') }}
    WHERE usage_id IS NOT NULL
),

deduped_feature_usage AS (
    SELECT *
    FROM bronze_feature_usage
    WHERE row_rank = 1
),

data_quality_checks AS (
    SELECT 
        usage_id,
        meeting_id,
        CASE 
            WHEN UPPER(TRIM(feature_name)) IN ('SCREEN SHARING', 'CHAT', 'RECORDING', 'WHITEBOARD', 'VIRTUAL BACKGROUND') 
            THEN UPPER(TRIM(feature_name))
            ELSE 'OTHER'
        END AS feature_name_clean,
        usage_count,
        usage_date,
        load_timestamp,
        update_timestamp,
        source_system,
        -- Data Quality Score Calculation
        CASE 
            WHEN usage_id IS NOT NULL 
                AND meeting_id IS NOT NULL
                AND feature_name IS NOT NULL
                AND usage_count IS NOT NULL AND usage_count >= 0
                AND usage_date IS NOT NULL
            THEN 1.00
            WHEN usage_id IS NOT NULL 
                AND meeting_id IS NOT NULL
                AND feature_name IS NOT NULL
            THEN 0.75
            WHEN usage_id IS NOT NULL 
                AND meeting_id IS NOT NULL
            THEN 0.50
            ELSE 0.25
        END AS data_quality_score,
        -- Record Status
        CASE 
            WHEN usage_id IS NOT NULL 
                AND meeting_id IS NOT NULL
                AND feature_name IS NOT NULL
                AND usage_count IS NOT NULL AND usage_count >= 0
                AND usage_date IS NOT NULL
            THEN 'ACTIVE'
            ELSE 'ERROR'
        END AS record_status
    FROM deduped_feature_usage
),

final_transform AS (
    SELECT 
        usage_id,
        meeting_id,
        feature_name_clean AS feature_name,
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
    WHERE record_status = 'ACTIVE'
)

SELECT * FROM final_transform

{% if is_incremental() %}
    WHERE update_timestamp > (SELECT COALESCE(MAX(update_timestamp), '1900-01-01') FROM {{ this }})
{% endif %}
