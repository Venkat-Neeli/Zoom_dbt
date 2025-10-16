{{ config(
    materialized='incremental',
    unique_key='participant_id',
    pre_hook="INSERT INTO {{ ref('si_process_audit') }} (execution_id, pipeline_name, start_time, status, source_system, target_system, process_type, load_date, update_date) SELECT '{{ dbt_utils.generate_surrogate_key([invocation_id, 'participants']) }}', 'si_participants_transform', CURRENT_TIMESTAMP(), 'RUNNING', 'BRONZE', 'SILVER', 'TRANSFORMATION', CURRENT_DATE(), CURRENT_DATE() WHERE '{{ this.name }}' != 'si_process_audit'",
    post_hook="UPDATE {{ ref('si_process_audit') }} SET end_time = CURRENT_TIMESTAMP(), status = 'SUCCESS', records_processed = (SELECT COUNT(*) FROM {{ this }}), records_successful = (SELECT COUNT(*) FROM {{ this }}), processing_duration_seconds = DATEDIFF('second', start_time, CURRENT_TIMESTAMP()) WHERE execution_id = '{{ dbt_utils.generate_surrogate_key([invocation_id, 'participants']) }}' AND '{{ this.name }}' != 'si_process_audit'"
) }}

-- Data Quality and Transformation Logic for Participants
WITH bronze_participants AS (
    SELECT *
    FROM {{ source('bronze', 'bz_participants') }}
    {% if is_incremental() %}
        WHERE update_timestamp > (SELECT MAX(update_timestamp) FROM {{ this }})
    {% endif %}
),

-- Data Quality Checks and Cleansing
cleansed_participants AS (
    SELECT 
        participant_id,
        meeting_id,
        user_id,
        join_time,
        leave_time,
        load_timestamp,
        update_timestamp,
        source_system,
        -- Derived columns
        DATE(load_timestamp) AS load_date,
        DATE(update_timestamp) AS update_date,
        -- Data Quality Score Calculation
        CASE 
            WHEN participant_id IS NOT NULL 
                AND meeting_id IS NOT NULL 
                AND join_time IS NOT NULL 
                AND leave_time IS NOT NULL
                AND leave_time > join_time
            THEN 1.00
            ELSE 0.50
        END AS data_quality_score,
        -- Record Status
        CASE 
            WHEN participant_id IS NULL OR meeting_id IS NULL OR join_time IS NULL OR leave_time IS NULL THEN 'error'
            WHEN leave_time <= join_time THEN 'error'
            ELSE 'active'
        END AS record_status,
        -- Deduplication ranking
        ROW_NUMBER() OVER (
            PARTITION BY participant_id 
            ORDER BY update_timestamp DESC, 
                     (CASE WHEN meeting_id IS NOT NULL THEN 1 ELSE 0 END +
                      CASE WHEN user_id IS NOT NULL THEN 1 ELSE 0 END +
                      CASE WHEN join_time IS NOT NULL THEN 1 ELSE 0 END) DESC,
                     participant_id DESC
        ) AS row_rank
    FROM bronze_participants
    WHERE participant_id IS NOT NULL
),

-- Final deduplicated and validated data
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
        load_date,
        update_date,
        data_quality_score,
        record_status
    FROM cleansed_participants
    WHERE row_rank = 1
        AND record_status = 'active'
)

SELECT * FROM final_participants
