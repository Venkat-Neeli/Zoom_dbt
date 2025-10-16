{{ config(
    materialized='incremental',
    unique_key='meeting_id',
    on_schema_change='fail',
    tags=['silver', 'meetings'],
    pre_hook="INSERT INTO {{ ref('si_process_audit') }} (execution_id, pipeline_name, start_time, status, source_system, target_system, process_type, load_date, update_date) SELECT '{{ dbt_utils.generate_surrogate_key(['CURRENT_TIMESTAMP()', 'RANDOM()']) }}', 'si_meetings_transformation', CURRENT_TIMESTAMP(), 'STARTED', 'BRONZE', 'SILVER', 'ETL', CURRENT_DATE(), CURRENT_DATE() WHERE '{{ this.name }}' != 'si_process_audit'",
    post_hook="INSERT INTO {{ ref('si_process_audit') }} (execution_id, pipeline_name, end_time, status, records_processed, source_system, target_system, process_type, load_date, update_date) SELECT '{{ dbt_utils.generate_surrogate_key(['CURRENT_TIMESTAMP()', 'RANDOM()']) }}', 'si_meetings_transformation', CURRENT_TIMESTAMP(), 'COMPLETED', (SELECT COUNT(*) FROM {{ this }}), 'BRONZE', 'SILVER', 'ETL', CURRENT_DATE(), CURRENT_DATE() WHERE '{{ this.name }}' != 'si_process_audit'"
) }}

WITH bronze_meetings AS (
    SELECT 
        bm.meeting_id,
        bm.host_id,
        bm.meeting_topic,
        bm.start_time,
        bm.end_time,
        bm.duration_minutes,
        bm.load_timestamp,
        bm.update_timestamp,
        bm.source_system,
        ROW_NUMBER() OVER (
            PARTITION BY bm.meeting_id 
            ORDER BY bm.update_timestamp DESC, 
                     bm.load_timestamp DESC
        ) AS row_num
    FROM {{ source('bronze', 'bz_meetings') }} bm
    WHERE bm.meeting_id IS NOT NULL
      AND bm.host_id IS NOT NULL
      AND bm.start_time IS NOT NULL
      AND bm.end_time IS NOT NULL
      AND bm.end_time > bm.start_time
      AND bm.duration_minutes > 0
      AND bm.duration_minutes <= 1440
),

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
        DATE(load_timestamp) AS load_date,
        DATE(update_timestamp) AS update_date,
        -- Data Quality Score
        CASE 
            WHEN meeting_id IS NOT NULL 
                 AND host_id IS NOT NULL 
                 AND start_time IS NOT NULL 
                 AND end_time IS NOT NULL 
                 AND end_time > start_time
                 AND duration_minutes > 0
                 AND duration_minutes <= 1440
            THEN 1.0
            ELSE 0.8
        END AS data_quality_score,
        'active' AS record_status
    FROM bronze_meetings
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
FROM cleaned_meetings

{% if is_incremental() %}
  WHERE update_timestamp > (SELECT MAX(update_timestamp) FROM {{ this }})
{% endif %}
