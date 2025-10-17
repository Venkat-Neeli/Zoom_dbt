{{
    config(
        materialized='incremental',
        unique_key='webinar_id',
        on_schema_change='sync_all_columns',
        pre_hook="{% if not (this.name == 'si_process_audit') %}INSERT INTO {{ ref('si_process_audit') }} (execution_id, pipeline_name, start_time, status, source_system, target_system, process_type, load_date, update_date) VALUES ('{{ invocation_id }}_{{ this.name }}', '{{ this.name }}', CURRENT_TIMESTAMP(), 'STARTED', 'BRONZE', 'SILVER', 'ETL', CURRENT_DATE(), CURRENT_DATE()){% endif %}",
        post_hook="{% if not (this.name == 'si_process_audit') %}UPDATE {{ ref('si_process_audit') }} SET end_time = CURRENT_TIMESTAMP(), status = 'SUCCESS', records_processed = (SELECT COUNT(*) FROM {{ this }}), records_successful = (SELECT COUNT(*) FROM {{ this }} WHERE record_status = 'active'), records_failed = (SELECT COUNT(*) FROM {{ this }} WHERE record_status = 'error'), processing_duration_seconds = DATEDIFF(second, start_time, CURRENT_TIMESTAMP()) WHERE execution_id = '{{ invocation_id }}_{{ this.name }}'{% endif %}"
    )
}}

WITH bronze_webinars AS (
    SELECT 
        webinar_id,
        host_id,
        webinar_topic,
        start_time,
        end_time,
        registrants,
        load_timestamp,
        update_timestamp,
        source_system,
        ROW_NUMBER() OVER (
            PARTITION BY webinar_id 
            ORDER BY update_timestamp DESC, 
                     load_timestamp DESC
        ) as row_num
    FROM {{ ref('bz_webinars') }}
    WHERE webinar_id IS NOT NULL
),

deduped_webinars AS (
    SELECT *
    FROM bronze_webinars
    WHERE row_num = 1
),

data_quality_checks AS (
    SELECT 
        *,
        -- Null checks
        CASE WHEN webinar_id IS NOT NULL AND host_id IS NOT NULL AND webinar_topic IS NOT NULL AND start_time IS NOT NULL AND end_time IS NOT NULL AND registrants IS NOT NULL THEN 1 ELSE 0 END as null_check,
        -- Range checks
        CASE WHEN end_time > start_time AND registrants >= 0 THEN 1 ELSE 0 END as range_check,
        -- Referential integrity
        CASE WHEN EXISTS (SELECT 1 FROM {{ ref('si_users') }} u WHERE u.user_id = deduped_webinars.host_id) THEN 1 ELSE 0 END as ref_check
    FROM deduped_webinars
),

transformed_webinars AS (
    SELECT 
        webinar_id,
        host_id,
        CASE WHEN TRIM(webinar_topic) = '' THEN '000' ELSE TRIM(webinar_topic) END as webinar_topic,
        start_time,
        end_time,
        registrants,
        load_timestamp,
        update_timestamp,
        source_system,
        DATE(load_timestamp) as load_date,
        DATE(update_timestamp) as update_date,
        {{ calculate_data_quality_score('null_check = 1', 'range_check = 1', 'ref_check = 1') }} as data_quality_score,
        CASE 
            WHEN null_check = 1 AND range_check = 1 AND ref_check = 1 THEN 'active'
            ELSE 'error'
        END as record_status
    FROM data_quality_checks
)

SELECT 
    webinar_id,
    host_id,
    webinar_topic,
    start_time,
    end_time,
    registrants,
    load_timestamp,
    update_timestamp,
    source_system,
    load_date,
    update_date,
    data_quality_score,
    record_status
FROM transformed_webinars
WHERE record_status = 'active'

{% if is_incremental() %}
    AND update_timestamp > (SELECT MAX(update_timestamp) FROM {{ this }})
{% endif %}
