{{ config(
    materialized='incremental',
    unique_key='webinar_id',
    on_schema_change='sync_all_columns'
) }}

WITH bronze_webinars AS (
    SELECT *,
           ROW_NUMBER() OVER (PARTITION BY webinar_id ORDER BY update_timestamp DESC, load_timestamp DESC) AS row_num
    FROM {{ source('bronze', 'bz_webinars') }}
    WHERE webinar_id IS NOT NULL
),

deduped_webinars AS (
    SELECT *
    FROM bronze_webinars
    WHERE row_num = 1
),

data_quality_checks AS (
    SELECT *,
           CASE 
               WHEN webinar_id IS NULL THEN 0.0
               WHEN host_id IS NULL THEN 0.2
               WHEN webinar_topic IS NULL OR TRIM(webinar_topic) = '' THEN 0.3
               WHEN start_time IS NULL OR end_time IS NULL THEN 0.4
               WHEN end_time <= start_time THEN 0.5
               WHEN registrants IS NULL OR registrants < 0 THEN 0.6
               ELSE 1.0
           END AS data_quality_score,
           
           CASE 
               WHEN webinar_id IS NULL OR host_id IS NULL OR 
                    webinar_topic IS NULL OR TRIM(webinar_topic) = '' OR
                    start_time IS NULL OR end_time IS NULL OR 
                    end_time <= start_time OR
                    registrants IS NULL OR registrants < 0 THEN 'error'
               ELSE 'active'
           END AS record_status
    FROM deduped_webinars
),

valid_records AS (
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
        data_quality_score,
        record_status
    FROM data_quality_checks
    WHERE record_status = 'active'
      AND data_quality_score >= 0.7
)

SELECT * FROM valid_records

{% if is_incremental() %}
    WHERE update_timestamp > (SELECT COALESCE(MAX(update_timestamp), '1900-01-01'::timestamp) FROM {{ this }})
{% endif %}
