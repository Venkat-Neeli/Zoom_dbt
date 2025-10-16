{{ config(
    materialized='incremental',
    unique_key='webinar_id',
    on_schema_change='fail'
) }}

-- Transform bronze webinars to silver with data quality checks
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
        ROW_NUMBER() OVER (PARTITION BY webinar_id ORDER BY update_timestamp DESC, load_timestamp DESC) as rn
    FROM {{ source('bronze', 'bz_webinars') }}
    WHERE webinar_id IS NOT NULL
),

-- Data quality validation and transformation
validated_webinars AS (
    SELECT 
        webinar_id,
        host_id,
        CASE 
            WHEN TRIM(webinar_topic) = '' THEN '000'
            ELSE TRIM(webinar_topic)
        END AS webinar_topic,
        start_time,
        end_time,
        registrants,
        load_timestamp,
        update_timestamp,
        source_system,
        -- Data quality score calculation
        CASE 
            WHEN host_id IS NOT NULL 
                AND webinar_topic IS NOT NULL
                AND start_time IS NOT NULL 
                AND end_time IS NOT NULL
                AND end_time > start_time
                AND registrants >= 0
            THEN 1.00
            ELSE 0.75
        END AS data_quality_score,
        -- Record status
        CASE 
            WHEN host_id IS NULL OR webinar_topic IS NULL OR start_time IS NULL OR end_time IS NULL THEN 'error'
            WHEN end_time <= start_time THEN 'error'
            WHEN registrants < 0 THEN 'error'
            ELSE 'active'
        END AS record_status
    FROM bronze_webinars
    WHERE rn = 1  -- Deduplication: keep latest record
        AND host_id IS NOT NULL
        AND webinar_topic IS NOT NULL
        AND start_time IS NOT NULL
        AND end_time IS NOT NULL
        AND end_time > start_time
        AND registrants >= 0
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
    DATE(load_timestamp) AS load_date,
    DATE(update_timestamp) AS update_date,
    data_quality_score,
    record_status
FROM validated_webinars

{% if is_incremental() %}
    WHERE update_timestamp > (SELECT MAX(update_timestamp) FROM {{ this }})
{% endif %}
