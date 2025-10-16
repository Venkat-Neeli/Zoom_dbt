{{ config(
    materialized='incremental',
    unique_key='webinar_id',
    on_schema_change='fail',
    tags=['silver', 'webinars'],
    pre_hook="INSERT INTO {{ ref('si_process_audit') }} (execution_id, pipeline_name, start_time, status, source_system, target_system, process_type, load_date, update_date) SELECT '{{ dbt_utils.generate_surrogate_key(['CURRENT_TIMESTAMP()', 'RANDOM()']) }}', 'si_webinars_transformation', CURRENT_TIMESTAMP(), 'STARTED', 'BRONZE', 'SILVER', 'ETL', CURRENT_DATE(), CURRENT_DATE() WHERE '{{ this.name }}' != 'si_process_audit'",
    post_hook="INSERT INTO {{ ref('si_process_audit') }} (execution_id, pipeline_name, end_time, status, records_processed, source_system, target_system, process_type, load_date, update_date) SELECT '{{ dbt_utils.generate_surrogate_key(['CURRENT_TIMESTAMP()', 'RANDOM()']) }}', 'si_webinars_transformation', CURRENT_TIMESTAMP(), 'COMPLETED', (SELECT COUNT(*) FROM {{ this }}), 'BRONZE', 'SILVER', 'ETL', CURRENT_DATE(), CURRENT_DATE() WHERE '{{ this.name }}' != 'si_process_audit'"
) }}

WITH bronze_webinars AS (
    SELECT 
        bw.webinar_id,
        bw.host_id,
        bw.webinar_topic,
        bw.start_time,
        bw.end_time,
        bw.registrants,
        bw.load_timestamp,
        bw.update_timestamp,
        bw.source_system,
        ROW_NUMBER() OVER (
            PARTITION BY bw.webinar_id 
            ORDER BY bw.update_timestamp DESC, 
                     bw.load_timestamp DESC
        ) AS row_num
    FROM {{ source('bronze', 'bz_webinars') }} bw
    WHERE bw.webinar_id IS NOT NULL
      AND bw.host_id IS NOT NULL
      AND bw.webinar_topic IS NOT NULL
      AND bw.start_time IS NOT NULL
      AND bw.end_time IS NOT NULL
      AND bw.end_time > bw.start_time
      AND bw.registrants >= 0
),

cleaned_webinars AS (
    SELECT 
        webinar_id,
        host_id,
        TRIM(webinar_topic) AS webinar_topic,
        start_time,
        end_time,
        registrants,
        load_timestamp,
        update_timestamp,
        source_system,
        DATE(load_timestamp) AS load_date,
        DATE(update_timestamp) AS update_date,
        -- Data Quality Score
        CASE 
            WHEN webinar_id IS NOT NULL 
                 AND host_id IS NOT NULL 
                 AND webinar_topic IS NOT NULL 
                 AND start_time IS NOT NULL 
                 AND end_time IS NOT NULL 
                 AND end_time > start_time
                 AND registrants >= 0
            THEN 1.0
            ELSE 0.8
        END AS data_quality_score,
        'active' AS record_status
    FROM bronze_webinars
    WHERE row_num = 1
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
FROM cleaned_webinars

{% if is_incremental() %}
  WHERE update_timestamp > (SELECT MAX(update_timestamp) FROM {{ this }})
{% endif %}
