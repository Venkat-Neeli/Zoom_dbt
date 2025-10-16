{{ config(
    materialized='incremental',
    unique_key='meeting_id',
    on_schema_change='fail'
) }}

WITH bronze_meetings AS (
    SELECT *
    FROM {{ source('bronze', 'bz_meetings') }}
    {% if is_incremental() %}
        WHERE update_timestamp > (SELECT MAX(update_timestamp) FROM {{ this }})
    {% endif %}
),

-- Data Quality and Deduplication
deduped_meetings AS (
    SELECT *,
        ROW_NUMBER() OVER (
            PARTITION BY meeting_id 
            ORDER BY update_timestamp DESC, 
                     load_timestamp DESC,
                     CASE WHEN host_id IS NOT NULL THEN 1 ELSE 0 END +
                     CASE WHEN meeting_topic IS NOT NULL THEN 1 ELSE 0 END +
                     CASE WHEN start_time IS NOT NULL THEN 1 ELSE 0 END +
                     CASE WHEN end_time IS NOT NULL THEN 1 ELSE 0 END DESC
        ) AS row_rank
    FROM bronze_meetings
),

-- Data Quality Checks and Transformations
transformed_meetings AS (
    SELECT
        meeting_id,
        host_id,
        TRIM(meeting_topic) AS meeting_topic,
        start_time,
        end_time,
        CASE 
            WHEN duration_minutes <= 0 OR duration_minutes > 1440 THEN NULL
            ELSE duration_minutes
        END AS duration_minutes,
        load_timestamp,
        update_timestamp,
        source_system,
        DATE(load_timestamp) AS load_date,
        DATE(update_timestamp) AS update_date,
        
        -- Data Quality Score Calculation
        CASE 
            WHEN meeting_id IS NULL OR host_id IS NULL OR start_time IS NULL OR end_time IS NULL THEN 0.0
            WHEN end_time <= start_time THEN 0.3
            WHEN duration_minutes <= 0 OR duration_minutes > 1440 THEN 0.5
            ELSE 1.0
        END AS data_quality_score,
        
        -- Record Status
        CASE 
            WHEN meeting_id IS NULL OR host_id IS NULL OR start_time IS NULL OR end_time IS NULL THEN 'error'
            WHEN end_time <= start_time THEN 'error'
            ELSE 'active'
        END AS record_status
        
    FROM deduped_meetings
    WHERE row_rank = 1
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
FROM transformed_meetings
WHERE record_status = 'active'
  AND meeting_id IS NOT NULL
  AND host_id IS NOT NULL
  AND start_time IS NOT NULL
  AND end_time IS NOT NULL
  AND end_time > start_time
