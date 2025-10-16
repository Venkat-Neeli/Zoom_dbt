{{ config(
    materialized='incremental',
    unique_key='webinar_id',
    on_schema_change='fail',
    pre_hook="INSERT INTO {{ ref('si_process_audit') }} (execution_id, pipeline_name, start_time, status, source_system, target_system, process_type, user_executed, load_date, update_date) SELECT '{{ dbt_utils.generate_surrogate_key(['invocation_id', 'si_webinars']) }}', 'si_webinars', CURRENT_TIMESTAMP(), 'RUNNING', 'BRONZE', 'SILVER', 'ETL', CURRENT_USER(), CURRENT_DATE(), CURRENT_DATE() WHERE '{{ this.name }}' != 'si_process_audit'",
    post_hook="UPDATE {{ ref('si_process_audit') }} SET end_time = CURRENT_TIMESTAMP(), status = 'SUCCESS', records_processed = (SELECT COUNT(*) FROM {{ this }}), records_successful = (SELECT COUNT(*) FROM {{ this }} WHERE record_status = 'active'), processing_duration_seconds = DATEDIFF(second, start_time, CURRENT_TIMESTAMP()) WHERE execution_id = '{{ dbt_utils.generate_surrogate_key(['invocation_id', 'si_webinars']) }}' AND '{{ this.name }}' != 'si_process_audit'"
) }}

WITH bronze_webinars AS (
    SELECT *
    FROM {{ source('bronze', 'bz_webinars') }}
    {% if is_incremental() %}
        WHERE update_timestamp > (SELECT MAX(update_timestamp) FROM {{ this }})
    {% endif %}
),

-- Data Quality and Deduplication
cleaned_webinars AS (
    SELECT *,
        ROW_NUMBER() OVER (
            PARTITION BY webinar_id 
            ORDER BY update_timestamp DESC, 
                     load_timestamp DESC,
                     CASE WHEN host_id IS NOT NULL THEN 1 ELSE 0 END +
                     CASE WHEN webinar_topic IS NOT NULL THEN 1 ELSE 0 END +
                     CASE WHEN start_time IS NOT NULL THEN 1 ELSE 0 END +
                     CASE WHEN end_time IS NOT NULL THEN 1 ELSE 0 END +
                     CASE WHEN registrants IS NOT NULL THEN 1 ELSE 0 END DESC,
                     webinar_id DESC
        ) AS row_num
    FROM bronze_webinars
    WHERE webinar_id IS NOT NULL
      AND host_id IS NOT NULL
      AND webinar_topic IS NOT NULL
      AND start_time IS NOT NULL
      AND end_time IS NOT NULL
      AND end_time > start_time
      AND registrants IS NOT NULL
      AND registrants >= 0
),

-- Calculate Data Quality Score
final_webinars AS (
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
        -- Data Quality Score Calculation
        ROUND(
            (CASE WHEN webinar_id IS NOT NULL THEN 0.2 ELSE 0 END +
             CASE WHEN host_id IS NOT NULL THEN 0.2 ELSE 0 END +
             CASE WHEN webinar_topic IS NOT NULL AND LENGTH(TRIM(webinar_topic)) > 0 THEN 0.2 ELSE 0 END +
             CASE WHEN start_time IS NOT NULL AND end_time IS NOT NULL AND end_time > start_time THEN 0.2 ELSE 0 END +
             CASE WHEN registrants >= 0 THEN 0.2 ELSE 0 END), 2
        ) AS data_quality_score,
        'active' AS record_status
    FROM cleaned_webinars
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
FROM final_webinars
