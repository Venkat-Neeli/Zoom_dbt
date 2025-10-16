{{ config(
    materialized='incremental',
    unique_key='meeting_id',
    on_schema_change='fail',
    pre_hook="INSERT INTO {{ ref('si_process_audit') }} (execution_id, pipeline_name, start_time, status, source_system, target_system, process_type, user_executed, load_date, update_date) SELECT '{{ dbt_utils.generate_surrogate_key(['invocation_id', 'si_meetings']) }}', 'si_meetings', CURRENT_TIMESTAMP(), 'RUNNING', 'BRONZE', 'SILVER', 'ETL', CURRENT_USER(), CURRENT_DATE(), CURRENT_DATE() WHERE '{{ this.name }}' != 'si_process_audit'",
    post_hook="UPDATE {{ ref('si_process_audit') }} SET end_time = CURRENT_TIMESTAMP(), status = 'SUCCESS', records_processed = (SELECT COUNT(*) FROM {{ this }}), records_successful = (SELECT COUNT(*) FROM {{ this }} WHERE record_status = 'active'), processing_duration_seconds = DATEDIFF(second, start_time, CURRENT_TIMESTAMP()) WHERE execution_id = '{{ dbt_utils.generate_surrogate_key(['invocation_id', 'si_meetings']) }}' AND '{{ this.name }}' != 'si_process_audit'"
) }}

WITH bronze_meetings AS (
    SELECT *
    FROM {{ source('bronze', 'bz_meetings') }}
    {% if is_incremental() %}
        WHERE update_timestamp > (SELECT MAX(update_timestamp) FROM {{ this }})
    {% endif %}
),

-- Data Quality and Deduplication
cleaned_meetings AS (
    SELECT *,
        ROW_NUMBER() OVER (
            PARTITION BY meeting_id 
            ORDER BY update_timestamp DESC, 
                     load_timestamp DESC,
                     CASE WHEN host_id IS NOT NULL THEN 1 ELSE 0 END +
                     CASE WHEN meeting_topic IS NOT NULL THEN 1 ELSE 0 END +
                     CASE WHEN start_time IS NOT NULL THEN 1 ELSE 0 END +
                     CASE WHEN end_time IS NOT NULL THEN 1 ELSE 0 END DESC,
                     meeting_id DESC
        ) AS row_num
    FROM bronze_meetings
    WHERE meeting_id IS NOT NULL
      AND host_id IS NOT NULL
      AND start_time IS NOT NULL
      AND end_time IS NOT NULL
      AND end_time > start_time
      AND duration_minutes > 0
      AND duration_minutes <= 1440
),

-- Calculate Data Quality Score
final_meetings AS (
    SELECT 
        meeting_id,
        host_id,
        TRIM(COALESCE(meeting_topic, '000')) AS meeting_topic,
        start_time,
        end_time,
        duration_minutes,
        load_timestamp,
        update_timestamp,
        source_system,
        DATE(load_timestamp) AS load_date,
        DATE(update_timestamp) AS update_date,
        -- Data Quality Score Calculation
        ROUND(
            (CASE WHEN meeting_id IS NOT NULL THEN 0.2 ELSE 0 END +
             CASE WHEN host_id IS NOT NULL THEN 0.2 ELSE 0 END +
             CASE WHEN start_time IS NOT NULL THEN 0.2 ELSE 0 END +
             CASE WHEN end_time IS NOT NULL AND end_time > start_time THEN 0.2 ELSE 0 END +
             CASE WHEN duration_minutes > 0 AND duration_minutes <= 1440 THEN 0.2 ELSE 0 END), 2
        ) AS data_quality_score,
        'active' AS record_status
    FROM cleaned_meetings
    WHERE row_num = 1
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
    load_date,
    update_date,
    data_quality_score,
    record_status
FROM final_meetings
