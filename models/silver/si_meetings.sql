{{ config(
    materialized='incremental',
    unique_key='meeting_id'
) }}

-- Data Quality and Transformation Logic for Meetings
WITH bronze_meetings AS (
    SELECT *
    FROM {{ source('bronze', 'bz_meetings') }}
    {% if is_incremental() %}
        WHERE update_timestamp > (SELECT MAX(update_timestamp) FROM {{ this }})
    {% endif %}
),

-- Data Quality Checks and Cleansing
cleansed_meetings AS (
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
        -- Derived columns
        DATE(load_timestamp) AS load_date,
        DATE(update_timestamp) AS update_date,
        -- Data Quality Score Calculation
        CASE 
            WHEN meeting_id IS NOT NULL 
                AND host_id IS NOT NULL 
                AND start_time IS NOT NULL 
                AND end_time IS NOT NULL
                AND end_time > start_time
                AND duration_minutes > 0
                AND duration_minutes <= 1440
            THEN 1.00
            ELSE 0.50
        END AS data_quality_score,
        -- Record Status
        CASE 
            WHEN meeting_id IS NULL OR host_id IS NULL OR start_time IS NULL OR end_time IS NULL THEN 'error'
            WHEN end_time <= start_time THEN 'error'
            WHEN duration_minutes <= 0 OR duration_minutes > 1440 THEN 'error'
            ELSE 'active'
        END AS record_status,
        -- Deduplication ranking
        ROW_NUMBER() OVER (
            PARTITION BY meeting_id 
            ORDER BY update_timestamp DESC, 
                     (CASE WHEN host_id IS NOT NULL THEN 1 ELSE 0 END +
                      CASE WHEN meeting_topic IS NOT NULL THEN 1 ELSE 0 END +
                      CASE WHEN duration_minutes IS NOT NULL THEN 1 ELSE 0 END) DESC,
                     meeting_id DESC
        ) AS row_rank
    FROM bronze_meetings
    WHERE meeting_id IS NOT NULL
),

-- Final deduplicated and validated data
final_meetings AS (
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
    FROM cleansed_meetings
    WHERE row_rank = 1
        AND record_status = 'active'
)

SELECT * FROM final_meetings
