{{ config(
    materialized='incremental',
    unique_key='meeting_id',
    on_schema_change='fail'
) }}

-- Silver Meetings Table Transformation
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
        ROW_NUMBER() OVER (
            PARTITION BY meeting_id 
            ORDER BY update_timestamp DESC, 
                     load_timestamp DESC
        ) AS row_num
    FROM {{ source('bronze', 'bz_meetings') }}
    WHERE meeting_id IS NOT NULL
),

data_quality_checks AS (
    SELECT 
        *,
        -- Calculate data quality score
        CASE 
            WHEN meeting_id IS NULL THEN 0.0
            WHEN host_id IS NULL THEN 0.2
            WHEN start_time IS NULL OR end_time IS NULL THEN 0.3
            WHEN end_time <= start_time THEN 0.4
            WHEN duration_minutes IS NULL OR duration_minutes <= 0 OR duration_minutes > 1440 THEN 0.5
            ELSE 1.0
        END AS data_quality_score,
        
        -- Set record status
        CASE 
            WHEN meeting_id IS NULL OR host_id IS NULL 
                 OR start_time IS NULL OR end_time IS NULL 
                 OR end_time <= start_time 
                 OR duration_minutes IS NULL OR duration_minutes <= 0 OR duration_minutes > 1440 THEN 'error'
            ELSE 'active'
        END AS record_status
    FROM bronze_meetings
    WHERE row_num = 1
),

final_transform AS (
    SELECT 
        meeting_id,
        host_id,
        TRIM(meeting_topic) AS meeting_topic,
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
    FROM data_quality_checks
    WHERE record_status = 'active'  -- Only pass clean records to Silver
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
FROM final_transform

{% if is_incremental() %}
    WHERE update_timestamp > (SELECT COALESCE(MAX(update_timestamp), '1900-01-01') FROM {{ this }})
{% endif %}
