{{
    config(
        materialized='incremental',
        unique_key='usage_id',
        on_schema_change='sync_all_columns',
        pre_hook="{% if not (this.name == 'si_process_audit') %}INSERT INTO {{ ref('si_process_audit') }} (execution_id, pipeline_name, start_time, status, source_system, target_system, process_type, load_date, update_date) VALUES ('{{ invocation_id }}_{{ this.name }}', '{{ this.name }}', CURRENT_TIMESTAMP(), 'STARTED', 'BRONZE', 'SILVER', 'ETL', CURRENT_DATE(), CURRENT_DATE()){% endif %}",
        post_hook="{% if not (this.name == 'si_process_audit') %}UPDATE {{ ref('si_process_audit') }} SET end_time = CURRENT_TIMESTAMP(), status = 'SUCCESS', records_processed = (SELECT COUNT(*) FROM {{ this }}), records_successful = (SELECT COUNT(*) FROM {{ this }} WHERE record_status = 'active'), records_failed = (SELECT COUNT(*) FROM {{ this }} WHERE record_status = 'error'), processing_duration_seconds = DATEDIFF(second, start_time, CURRENT_TIMESTAMP()) WHERE execution_id = '{{ invocation_id }}_{{ this.name }}'{% endif %}"
    )
}}

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
        ) as row_num
    FROM {{ ref('bz_feature_usage') }}
    WHERE usage_id IS NOT NULL
),

deduped_feature_usage AS (
    SELECT *
    FROM bronze_feature_usage
    WHERE row_num = 1
),

data_quality_checks AS (
    SELECT 
        *,
        -- Null checks
        CASE WHEN usage_id IS NOT NULL AND meeting_id IS NOT NULL AND feature_name IS NOT NULL AND usage_count IS NOT NULL AND usage_date IS NOT NULL THEN 1 ELSE 0 END as null_check,
        -- Range checks
        CASE WHEN usage_count >= 0 THEN 1 ELSE 0 END as range_check,
        -- Domain checks
        CASE WHEN feature_name IN ('Screen Sharing', 'Chat', 'Recording', 'Whiteboard', 'Virtual Background') THEN 1 ELSE 0 END as domain_check,
        -- Referential integrity
        CASE WHEN EXISTS (SELECT 1 FROM {{ ref('si_meetings') }} m WHERE m.meeting_id = deduped_feature_usage.meeting_id) THEN 1 ELSE 0 END as ref_check
    FROM deduped_feature_usage
),

transformed_feature_usage AS (
    SELECT 
        usage_id,
        meeting_id,
        CASE 
            WHEN UPPER(TRIM(feature_name)) = 'SCREEN SHARING' THEN 'Screen Sharing'
            WHEN UPPER(TRIM(feature_name)) = 'CHAT' THEN 'Chat'
            WHEN UPPER(TRIM(feature_name)) = 'RECORDING' THEN 'Recording'
            WHEN UPPER(TRIM(feature_name)) = 'WHITEBOARD' THEN 'Whiteboard'
            WHEN UPPER(TRIM(feature_name)) = 'VIRTUAL BACKGROUND' THEN 'Virtual Background'
            ELSE feature_name
        END as feature_name,
        usage_count,
        usage_date,
        load_timestamp,
        update_timestamp,
        source_system,
        DATE(load_timestamp) as load_date,
        DATE(update_timestamp) as update_date,
        {{ calculate_data_quality_score('null_check = 1', 'range_check = 1 AND domain_check = 1', 'ref_check = 1') }} as data_quality_score,
        CASE 
            WHEN null_check = 1 AND range_check = 1 AND domain_check = 1 AND ref_check = 1 THEN 'active'
            ELSE 'error'
        END as record_status
    FROM data_quality_checks
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

{% if is_incremental() %}
    AND update_timestamp > (SELECT MAX(update_timestamp) FROM {{ this }})
{% endif %}
