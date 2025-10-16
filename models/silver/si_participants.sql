{{ config(
    materialized='incremental',
    unique_key='participant_id',
    on_schema_change='sync_all_columns',
    tags=['silver', 'participants'],
    pre_hook="INSERT INTO {{ ref('si_process_audit') }} (execution_id, pipeline_name, start_time, status, source_system, target_system, process_type, load_date, update_date) SELECT '{{ invocation_id }}' || '_participants_start', 'si_participants_transform', CURRENT_TIMESTAMP(), 'STARTED', 'BRONZE', 'SILVER', 'ETL', CURRENT_DATE(), CURRENT_DATE() WHERE '{{ this.name }}' != 'si_process_audit'",
    post_hook="INSERT INTO {{ ref('si_process_audit') }} (execution_id, pipeline_name, end_time, status, records_processed, source_system, target_system, process_type, load_date, update_date) SELECT '{{ invocation_id }}' || '_participants_end', 'si_participants_transform', CURRENT_TIMESTAMP(), 'COMPLETED', (SELECT COUNT(*) FROM {{ this }}), 'BRONZE', 'SILVER', 'ETL', CURRENT_DATE(), CURRENT_DATE() WHERE '{{ this.name }}' != 'si_process_audit'"
) }}

WITH bronze_participants AS (
    SELECT 
        participant_id,
        meeting_id,
        user_id,
        join_time,
        leave_time,
        load_timestamp,
        update_timestamp,
        source_system,
        ROW_NUMBER() OVER (
            PARTITION BY participant_id 
            ORDER BY update_timestamp DESC, 
                     load_timestamp DESC,
                     CASE WHEN join_time IS NOT NULL THEN 1 ELSE 0 END +
                     CASE WHEN leave_time IS NOT NULL THEN 1 ELSE 0 END DESC
        ) AS row_rank
    FROM {{ source('bronze', 'bz_participants') }}
    WHERE participant_id IS NOT NULL
),

deduped_participants AS (
    SELECT *
    FROM bronze_participants
    WHERE row_rank = 1
),

data_quality_checks AS (
    SELECT 
        participant_id,
        meeting_id,
        user_id,
        join_time,
        leave_time,
        load_timestamp,
        update_timestamp,
        source_system,
        -- Data Quality Score Calculation
        CASE 
            WHEN participant_id IS NOT NULL 
                AND meeting_id IS NOT NULL
                AND join_time IS NOT NULL
                AND leave_time IS NOT NULL
                AND leave_time > join_time
            THEN 1.00
            WHEN participant_id IS NOT NULL 
                AND meeting_id IS NOT NULL
                AND join_time IS NOT NULL
            THEN 0.75
            WHEN participant_id IS NOT NULL 
                AND meeting_id IS NOT NULL
            THEN 0.50
            ELSE 0.25
        END AS data_quality_score,
        -- Record Status
        CASE 
            WHEN participant_id IS NOT NULL 
                AND meeting_id IS NOT NULL
                AND join_time IS NOT NULL
                AND leave_time IS NOT NULL
                AND leave_time > join_time
            THEN 'ACTIVE'
            ELSE 'ERROR'
        END AS record_status
    FROM deduped_participants
),

final_transform AS (
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
    WHERE record_status = 'ACTIVE'
)

SELECT * FROM final_transform

{% if is_incremental() %}
    WHERE update_timestamp > (SELECT COALESCE(MAX(update_timestamp), '1900-01-01') FROM {{ this }})
{% endif %}
