{{ config(
    materialized='incremental',
    unique_key='meeting_id',
    on_schema_change='sync_all_columns',
    tags=['silver', 'meetings'],
    pre_hook="INSERT INTO {{ ref('si_process_audit') }} (execution_id, pipeline_name, start_time, status, source_system, target_system, process_type, load_date, update_date) SELECT '{{ invocation_id }}' || '_meetings_start', 'si_meetings_transform', CURRENT_TIMESTAMP(), 'STARTED', 'BRONZE', 'SILVER', 'ETL', CURRENT_DATE(), CURRENT_DATE() WHERE '{{ this.name }}' != 'si_process_audit'",
    post_hook="INSERT INTO {{ ref('si_process_audit') }} (execution_id, pipeline_name, end_time, status, records_processed, source_system, target_system, process_type, load_date, update_date) SELECT '{{ invocation_id }}' || '_meetings_end', 'si_meetings_transform', CURRENT_TIMESTAMP(), 'COMPLETED', (SELECT COUNT(*) FROM {{ this }}), 'BRONZE', 'SILVER', 'ETL', CURRENT_DATE(), CURRENT_DATE() WHERE '{{ this.name }}' != 'si_process_audit'"
) }}

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
                     load_timestamp DESC,
                     CASE WHEN meeting_topic IS NOT NULL THEN 1 ELSE 0 END +
                     CASE WHEN start_time IS NOT NULL THEN 1 ELSE 0 END +
                     CASE WHEN end_time IS NOT NULL THEN 1 ELSE 0 END +
                     CASE WHEN duration_minutes IS NOT NULL THEN 1 ELSE 0 END DESC
        ) AS row_rank
    FROM {{ source('bronze', 'bz_meetings') }}
    WHERE meeting_id IS NOT NULL
),

deduped_meetings AS (
    SELECT *
    FROM bronze_meetings
    WHERE row_rank = 1
),

data_quality_checks AS (
    SELECT 
        meeting_id,
        host_id,
        TRIM(meeting_topic) AS meeting_topic_clean,
        start_time,
        end_time,
        duration_minutes,
        load_timestamp,
        update_timestamp,
        source_system,
        -- Data Quality Score Calculation
        CASE 
            WHEN meeting_id IS NOT NULL 
                AND host_id IS NOT NULL
                AND start_time IS NOT NULL
                AND end_time IS NOT NULL
                AND end_time > start_time
                AND duration_minutes > 0 AND duration_minutes <= 1440
            THEN 1.00
            WHEN meeting_id IS NOT NULL 
                AND host_id IS NOT NULL
                AND start_time IS NOT NULL
                AND end_time IS NOT NULL
            THEN 0.75
            WHEN meeting_id IS NOT NULL 
                AND host_id IS NOT NULL
            THEN 0.50
            ELSE 0.25
        END AS data_quality_score,
        -- Record Status
        CASE 
            WHEN meeting_id IS NOT NULL 
                AND host_id IS NOT NULL
                AND start_time IS NOT NULL
                AND end_time IS NOT NULL
                AND end_time > start_time
                AND duration_minutes > 0 AND duration_minutes <= 1440
            THEN 'ACTIVE'
            ELSE 'ERROR'
        END AS record_status
    FROM deduped_meetings
),

final_transform AS (
    SELECT 
        meeting_id,
        host_id,
        COALESCE(meeting_topic_clean, '000') AS meeting_topic,
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
    WHERE record_status = 'ACTIVE'
)

SELECT * FROM final_transform

{% if is_incremental() %}
    WHERE update_timestamp > (SELECT COALESCE(MAX(update_timestamp), '1900-01-01') FROM {{ this }})
{% endif %}
