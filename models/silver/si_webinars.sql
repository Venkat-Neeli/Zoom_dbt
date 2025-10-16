{{ config(
    materialized='incremental',
    unique_key='webinar_id',
    on_schema_change='sync_all_columns',
    tags=['silver', 'webinars'],
    pre_hook="INSERT INTO {{ ref('si_process_audit') }} (execution_id, pipeline_name, start_time, status, source_system, target_system, process_type, load_date, update_date) SELECT '{{ invocation_id }}' || '_webinars_start', 'si_webinars_transform', CURRENT_TIMESTAMP(), 'STARTED', 'BRONZE', 'SILVER', 'ETL', CURRENT_DATE(), CURRENT_DATE() WHERE '{{ this.name }}' != 'si_process_audit'",
    post_hook="INSERT INTO {{ ref('si_process_audit') }} (execution_id, pipeline_name, end_time, status, records_processed, source_system, target_system, process_type, load_date, update_date) SELECT '{{ invocation_id }}' || '_webinars_end', 'si_webinars_transform', CURRENT_TIMESTAMP(), 'COMPLETED', (SELECT COUNT(*) FROM {{ this }}), 'BRONZE', 'SILVER', 'ETL', CURRENT_DATE(), CURRENT_DATE() WHERE '{{ this.name }}' != 'si_process_audit'"
) }}

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
                     load_timestamp DESC,
                     CASE WHEN webinar_topic IS NOT NULL THEN 1 ELSE 0 END +
                     CASE WHEN registrants IS NOT NULL THEN 1 ELSE 0 END DESC
        ) AS row_rank
    FROM {{ source('bronze', 'bz_webinars') }}
    WHERE webinar_id IS NOT NULL
),

deduped_webinars AS (
    SELECT *
    FROM bronze_webinars
    WHERE row_rank = 1
),

data_quality_checks AS (
    SELECT 
        webinar_id,
        host_id,
        TRIM(webinar_topic) AS webinar_topic_clean,
        start_time,
        end_time,
        registrants,
        load_timestamp,
        update_timestamp,
        source_system,
        -- Data Quality Score Calculation
        CASE 
            WHEN webinar_id IS NOT NULL 
                AND host_id IS NOT NULL
                AND webinar_topic IS NOT NULL
                AND start_time IS NOT NULL
                AND end_time IS NOT NULL
                AND end_time > start_time
                AND registrants IS NOT NULL AND registrants >= 0
            THEN 1.00
            WHEN webinar_id IS NOT NULL 
                AND host_id IS NOT NULL
                AND start_time IS NOT NULL
                AND end_time IS NOT NULL
            THEN 0.75
            WHEN webinar_id IS NOT NULL 
                AND host_id IS NOT NULL
            THEN 0.50
            ELSE 0.25
        END AS data_quality_score,
        -- Record Status
        CASE 
            WHEN webinar_id IS NOT NULL 
                AND host_id IS NOT NULL
                AND webinar_topic IS NOT NULL
                AND start_time IS NOT NULL
                AND end_time IS NOT NULL
                AND end_time > start_time
                AND registrants IS NOT NULL AND registrants >= 0
            THEN 'ACTIVE'
            ELSE 'ERROR'
        END AS record_status
    FROM deduped_webinars
),

final_transform AS (
    SELECT 
        webinar_id,
        host_id,
        COALESCE(webinar_topic_clean, '000') AS webinar_topic,
        start_time,
        end_time,
        registrants,
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
