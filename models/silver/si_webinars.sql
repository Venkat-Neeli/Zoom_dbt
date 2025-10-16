{{ config(
    materialized='incremental',
    unique_key='webinar_id',
    on_schema_change='fail'
) }}

WITH bronze_webinars AS (
    SELECT *
    FROM {{ source('bronze', 'bz_webinars') }}
    {% if is_incremental() %}
        WHERE update_timestamp > (SELECT MAX(update_timestamp) FROM {{ this }})
    {% endif %}
),

-- Data Quality and Deduplication
deduped_webinars AS (
    SELECT *,
        ROW_NUMBER() OVER (
            PARTITION BY webinar_id 
            ORDER BY update_timestamp DESC, 
                     load_timestamp DESC,
                     CASE WHEN host_id IS NOT NULL THEN 1 ELSE 0 END +
                     CASE WHEN webinar_topic IS NOT NULL THEN 1 ELSE 0 END +
                     CASE WHEN start_time IS NOT NULL THEN 1 ELSE 0 END +
                     CASE WHEN end_time IS NOT NULL THEN 1 ELSE 0 END DESC
        ) AS row_rank
    FROM bronze_webinars
),

-- Data Quality Checks and Transformations
transformed_webinars AS (
    SELECT
        webinar_id,
        host_id,
        TRIM(webinar_topic) AS webinar_topic,
        start_time,
        end_time,
        CASE 
            WHEN registrants < 0 THEN 0
            ELSE registrants
        END AS registrants,
        load_timestamp,
        update_timestamp,
        source_system,
        DATE(load_timestamp) AS load_date,
        DATE(update_timestamp) AS update_date,
        
        -- Data Quality Score Calculation
        CASE 
            WHEN webinar_id IS NULL OR host_id IS NULL OR webinar_topic IS NULL OR start_time IS NULL OR end_time IS NULL OR registrants IS NULL THEN 0.0
            WHEN end_time <= start_time THEN 0.3
            WHEN registrants < 0 THEN 0.5
            ELSE 1.0
        END AS data_quality_score,
        
        -- Record Status
        CASE 
            WHEN webinar_id IS NULL OR host_id IS NULL OR webinar_topic IS NULL OR start_time IS NULL OR end_time IS NULL OR registrants IS NULL THEN 'error'
            WHEN end_time <= start_time THEN 'error'
            ELSE 'active'
        END AS record_status
        
    FROM deduped_webinars
    WHERE row_rank = 1
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
  AND webinar_id IS NOT NULL
  AND host_id IS NOT NULL
  AND webinar_topic IS NOT NULL
  AND start_time IS NOT NULL
  AND end_time IS NOT NULL
  AND end_time > start_time
