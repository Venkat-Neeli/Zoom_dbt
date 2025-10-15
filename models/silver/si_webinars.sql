{{
    config(
        materialized='incremental',
        unique_key='webinar_id',
        on_schema_change='fail',
        pre_hook="{% if this.name != 'si_process_audit' %}INSERT INTO {{ ref('si_process_audit') }} (execution_id, pipeline_name, start_time, status, source_system, target_system, process_type, load_date, update_date) SELECT '{{ invocation_id }}', 'si_webinars_transform', current_timestamp(), 'STARTED', 'BRONZE', 'SILVER', 'ETL', current_date(), current_date(){% endif %}",
        post_hook="{% if this.name != 'si_process_audit' %}INSERT INTO {{ ref('si_process_audit') }} (execution_id, pipeline_name, end_time, status, records_processed, source_system, target_system, process_type, load_date, update_date) SELECT '{{ invocation_id }}', 'si_webinars_transform', current_timestamp(), 'COMPLETED', (SELECT count(*) FROM {{ this }}), 'BRONZE', 'SILVER', 'ETL', current_date(), current_date(){% endif %}"
    )
}}

-- Transform bronze webinars to silver webinars with data quality checks
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
    FROM {{ ref('bz_webinars') }}
    WHERE webinar_id IS NOT NULL
        AND host_id IS NOT NULL
        AND webinar_topic IS NOT NULL
        AND start_time IS NOT NULL
        AND end_time IS NOT NULL
        AND registrants IS NOT NULL
),

-- Data quality validation and cleansing
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
        -- Calculate data quality score
        CASE 
            WHEN start_time < end_time 
                AND registrants >= 0
            THEN 1.00
            ELSE 0.50
        END AS data_quality_score,
        CASE 
            WHEN start_time < end_time 
                AND registrants >= 0
            THEN 'active'
            ELSE 'error'
        END AS record_status
    FROM bronze_webinars
    WHERE rn = 1  -- Deduplication: keep latest record
        AND start_time < end_time
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
FROM cleaned_webinars

{% if is_incremental() %}
  WHERE update_timestamp > (SELECT COALESCE(MAX(update_timestamp), '1900-01-01') FROM {{ this }})
{% endif %}
