{{
    config(
        materialized='incremental',
        unique_key='meeting_id',
        on_schema_change='fail',
        pre_hook="INSERT INTO {{ ref('si_process_audit') }} (execution_id, pipeline_name, start_time, status, source_system, target_system, process_type, load_date, update_date) SELECT '{{ invocation_id }}', 'si_meetings_transform', current_timestamp(), 'STARTED', 'BRONZE', 'SILVER', 'ETL', current_date(), current_date() WHERE '{{ this.name }}' != 'si_process_audit'",
        post_hook="INSERT INTO {{ ref('si_process_audit') }} (execution_id, pipeline_name, end_time, status, records_processed, source_system, target_system, process_type, load_date, update_date) SELECT '{{ invocation_id }}', 'si_meetings_transform', current_timestamp(), 'COMPLETED', (SELECT count(*) FROM {{ this }}), 'BRONZE', 'SILVER', 'ETL', current_date(), current_date() WHERE '{{ this.name }}' != 'si_process_audit'"
    )
}}

-- Transform bronze meetings to silver meetings with data quality checks
WITH bronze_meetings AS (
    SELECT 
        meeting_id,
        host_id,
        meeting_topic,
        start_time,
        end_time,
        duration_minutes,
        load_timestamp,
        update_timestamp,
        source_system,
        ROW_NUMBER() OVER (PARTITION BY meeting_id ORDER BY update_timestamp DESC, load_timestamp DESC) as rn
    FROM {{ source('bronze', 'bz_meetings') }}
    WHERE meeting_id IS NOT NULL
        AND host_id IS NOT NULL
        AND start_time IS NOT NULL
        AND end_time IS NOT NULL
),

-- Data quality validation and cleansing
cleaned_meetings AS (
    SELECT
        meeting_id,
        host_id,
        CASE 
            WHEN TRIM(meeting_topic) = '' THEN '000'
            ELSE TRIM(meeting_topic)
        END AS meeting_topic,
        start_time,
        end_time,
        duration_minutes,
        load_timestamp,
        update_timestamp,
        source_system,
        -- Calculate data quality score
        CASE 
            WHEN start_time < end_time 
                AND duration_minutes > 0 
                AND duration_minutes <= 1440
            THEN 1.00
            ELSE 0.50
        END AS data_quality_score,
        CASE 
            WHEN start_time < end_time 
                AND duration_minutes > 0 
                AND duration_minutes <= 1440
            THEN 'active'
            ELSE 'error'
        END AS record_status
    FROM bronze_meetings
    WHERE rn = 1  -- Deduplication: keep latest record
        AND start_time < end_time
        AND duration_minutes > 0
        AND duration_minutes <= 1440
)

SELECT
    meeting_id,
    host_id,
    meeting_topic,
    start_time,
    end_time,
    duration_minutes,
    load_timestamp,
    update_timestamp,
    source_system,
    DATE(load_timestamp) AS load_date,
    DATE(update_timestamp) AS update_date,
    data_quality_score,
    record_status
FROM cleaned_meetings

{% if is_incremental() %}
  WHERE update_timestamp > (SELECT COALESCE(MAX(update_timestamp), '1900-01-01') FROM {{ this }})
{% endif %}
