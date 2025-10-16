{{ config(
    materialized='incremental',
    unique_key='webinar_id'
) }}

-- Data Quality and Transformation Logic for Webinars
WITH bronze_webinars AS (
    SELECT *
    FROM {{ source('bronze', 'bz_webinars') }}
    {% if is_incremental() %}
        WHERE update_timestamp > (SELECT MAX(update_timestamp) FROM {{ this }})
    {% endif %}
),

-- Data Quality Checks and Cleansing
cleansed_webinars AS (
    SELECT 
        webinar_id,
        host_id,
        CASE 
            WHEN TRIM(webinar_topic) = '' THEN '000'
            ELSE TRIM(webinar_topic)
        END AS webinar_topic,
        start_time,
        end_time,
        CASE 
            WHEN registrants < 0 THEN 0
            ELSE registrants
        END AS registrants,
        load_timestamp,
        update_timestamp,
        source_system,
        -- Derived columns
        DATE(load_timestamp) AS load_date,
        DATE(update_timestamp) AS update_date,
        -- Data Quality Score Calculation
        CASE 
            WHEN webinar_id IS NOT NULL 
                AND host_id IS NOT NULL 
                AND webinar_topic IS NOT NULL
                AND start_time IS NOT NULL 
                AND end_time IS NOT NULL
                AND end_time > start_time
                AND registrants >= 0
            THEN 1.00
            ELSE 0.50
        END AS data_quality_score,
        -- Record Status
        CASE 
            WHEN webinar_id IS NULL OR host_id IS NULL OR webinar_topic IS NULL THEN 'error'
            WHEN start_time IS NULL OR end_time IS NULL THEN 'error'
            WHEN end_time <= start_time THEN 'error'
            ELSE 'active'
        END AS record_status,
        -- Deduplication ranking
        ROW_NUMBER() OVER (
            PARTITION BY webinar_id 
            ORDER BY update_timestamp DESC, 
                     (CASE WHEN host_id IS NOT NULL THEN 1 ELSE 0 END +
                      CASE WHEN webinar_topic IS NOT NULL THEN 1 ELSE 0 END +
                      CASE WHEN registrants IS NOT NULL THEN 1 ELSE 0 END) DESC,
                     webinar_id DESC
        ) AS row_rank
    FROM bronze_webinars
    WHERE webinar_id IS NOT NULL
),

-- Final deduplicated and validated data
final_webinars AS (
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
    FROM cleansed_webinars
    WHERE row_rank = 1
        AND record_status = 'active'
)

SELECT * FROM final_webinars
