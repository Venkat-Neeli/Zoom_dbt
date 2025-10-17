{{ config(
    materialized='incremental',
    unique_key='participant_id',
    on_schema_change='sync_all_columns'
) }}

WITH bronze_participants AS (
    SELECT *,
           ROW_NUMBER() OVER (PARTITION BY participant_id ORDER BY update_timestamp DESC, load_timestamp DESC) AS row_num
    FROM {{ source('bronze', 'bz_participants') }}
    WHERE participant_id IS NOT NULL
),

deduped_participants AS (
    SELECT *
    FROM bronze_participants
    WHERE row_num = 1
),

data_quality_checks AS (
    SELECT *,
           CASE 
               WHEN participant_id IS NULL THEN 0.0
               WHEN meeting_id IS NULL THEN 0.2
               WHEN join_time IS NULL OR leave_time IS NULL THEN 0.3
               WHEN leave_time <= join_time THEN 0.4
               ELSE 1.0
           END AS data_quality_score,
           
           CASE 
               WHEN participant_id IS NULL OR meeting_id IS NULL OR 
                    join_time IS NULL OR leave_time IS NULL OR 
                    leave_time <= join_time THEN 'error'
               ELSE 'active'
           END AS record_status
    FROM deduped_participants
),

valid_records AS (
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
        data_quality_score,
        record_status
    FROM data_quality_checks
    WHERE record_status = 'active'
      AND data_quality_score >= 0.7
)

SELECT * FROM valid_records

{% if is_incremental() %}
    WHERE update_timestamp > (SELECT COALESCE(MAX(update_timestamp), '1900-01-01'::timestamp) FROM {{ this }})
{% endif %}
