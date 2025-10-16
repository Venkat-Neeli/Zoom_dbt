{{ config(
    materialized='incremental',
    unique_key='participant_id',
    on_schema_change='fail',
    tags=['silver', 'participants'],
    pre_hook="INSERT INTO {{ ref('si_process_audit') }} (execution_id, pipeline_name, start_time, status, source_system, target_system, process_type, load_date, update_date) SELECT '{{ dbt_utils.generate_surrogate_key(['CURRENT_TIMESTAMP()', 'RANDOM()']) }}', 'si_participants_transformation', CURRENT_TIMESTAMP(), 'STARTED', 'BRONZE', 'SILVER', 'ETL', CURRENT_DATE(), CURRENT_DATE() WHERE '{{ this.name }}' != 'si_process_audit'",
    post_hook="INSERT INTO {{ ref('si_process_audit') }} (execution_id, pipeline_name, end_time, status, records_processed, source_system, target_system, process_type, load_date, update_date) SELECT '{{ dbt_utils.generate_surrogate_key(['CURRENT_TIMESTAMP()', 'RANDOM()']) }}', 'si_participants_transformation', CURRENT_TIMESTAMP(), 'COMPLETED', (SELECT COUNT(*) FROM {{ this }}), 'BRONZE', 'SILVER', 'ETL', CURRENT_DATE(), CURRENT_DATE() WHERE '{{ this.name }}' != 'si_process_audit'"
) }}

WITH bronze_participants AS (
    SELECT 
        bp.participant_id,
        bp.meeting_id,
        bp.user_id,
        bp.join_time,
        bp.leave_time,
        bp.load_timestamp,
        bp.update_timestamp,
        bp.source_system,
        ROW_NUMBER() OVER (
            PARTITION BY bp.participant_id 
            ORDER BY bp.update_timestamp DESC, 
                     bp.load_timestamp DESC
        ) AS row_num
    FROM {{ source('bronze', 'bz_participants') }} bp
    WHERE bp.participant_id IS NOT NULL
      AND bp.meeting_id IS NOT NULL
      AND bp.join_time IS NOT NULL
      AND bp.leave_time IS NOT NULL
      AND bp.leave_time > bp.join_time
),

cleaned_participants AS (
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
        -- Data Quality Score
        CASE 
            WHEN participant_id IS NOT NULL 
                 AND meeting_id IS NOT NULL 
                 AND join_time IS NOT NULL 
                 AND leave_time IS NOT NULL 
                 AND leave_time > join_time
            THEN 1.0
            ELSE 0.8
        END AS data_quality_score,
        'active' AS record_status
    FROM bronze_participants
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
FROM cleaned_participants

{% if is_incremental() %}
  WHERE update_timestamp > (SELECT MAX(update_timestamp) FROM {{ this }})
{% endif %}
