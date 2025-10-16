{{ config(
    materialized='incremental',
    unique_key='participant_id',
    on_schema_change='fail',
    pre_hook="INSERT INTO {{ ref('si_process_audit') }} (execution_id, pipeline_name, start_time, status, source_system, target_system, process_type, user_executed, load_date, update_date) SELECT '{{ dbt_utils.generate_surrogate_key(['invocation_id', 'si_participants']) }}', 'si_participants', CURRENT_TIMESTAMP(), 'RUNNING', 'BRONZE', 'SILVER', 'ETL', CURRENT_USER(), CURRENT_DATE(), CURRENT_DATE() WHERE '{{ this.name }}' != 'si_process_audit'",
    post_hook="UPDATE {{ ref('si_process_audit') }} SET end_time = CURRENT_TIMESTAMP(), status = 'SUCCESS', records_processed = (SELECT COUNT(*) FROM {{ this }}), records_successful = (SELECT COUNT(*) FROM {{ this }} WHERE record_status = 'active'), processing_duration_seconds = DATEDIFF(second, start_time, CURRENT_TIMESTAMP()) WHERE execution_id = '{{ dbt_utils.generate_surrogate_key(['invocation_id', 'si_participants']) }}' AND '{{ this.name }}' != 'si_process_audit'"
) }}

WITH bronze_participants AS (
    SELECT *
    FROM {{ source('bronze', 'bz_participants') }}
    {% if is_incremental() %}
        WHERE update_timestamp > (SELECT MAX(update_timestamp) FROM {{ this }})
    {% endif %}
),

-- Data Quality and Deduplication
cleaned_participants AS (
    SELECT *,
        ROW_NUMBER() OVER (
            PARTITION BY participant_id 
            ORDER BY update_timestamp DESC, 
                     load_timestamp DESC,
                     CASE WHEN meeting_id IS NOT NULL THEN 1 ELSE 0 END +
                     CASE WHEN user_id IS NOT NULL THEN 1 ELSE 0 END +
                     CASE WHEN join_time IS NOT NULL THEN 1 ELSE 0 END +
                     CASE WHEN leave_time IS NOT NULL THEN 1 ELSE 0 END DESC,
                     participant_id DESC
        ) AS row_num
    FROM bronze_participants
    WHERE participant_id IS NOT NULL
      AND meeting_id IS NOT NULL
      AND join_time IS NOT NULL
      AND leave_time IS NOT NULL
      AND leave_time > join_time
),

-- Calculate Data Quality Score
final_participants AS (
    SELECT 
        participant_id,
        meeting_id,
        user_id,
        join_time,
        leave_time,
        load_timestamp,
        update_timestamp,
        source_system,
        DATE(load_timestamp) AS load_date,
        DATE(update_timestamp) AS update_date,
        -- Data Quality Score Calculation
        ROUND(
            (CASE WHEN participant_id IS NOT NULL THEN 0.25 ELSE 0 END +
             CASE WHEN meeting_id IS NOT NULL THEN 0.25 ELSE 0 END +
             CASE WHEN join_time IS NOT NULL THEN 0.25 ELSE 0 END +
             CASE WHEN leave_time IS NOT NULL AND leave_time > join_time THEN 0.25 ELSE 0 END), 2
        ) AS data_quality_score,
        'active' AS record_status
    FROM cleaned_participants
    WHERE row_num = 1
)

SELECT 
    participant_id,
    meeting_id,
    user_id,
    join_time,
    leave_time,
    load_timestamp,
    update_timestamp,
    source_system,
    load_date,
    update_date,
    data_quality_score,
    record_status
FROM final_participants
