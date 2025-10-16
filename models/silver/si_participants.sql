{{ config(
    materialized='incremental',
    unique_key='participant_id',
    on_schema_change='fail'
) }}

-- Transform bronze participants to silver with data quality checks
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
        ROW_NUMBER() OVER (PARTITION BY participant_id ORDER BY update_timestamp DESC, load_timestamp DESC) as rn
    FROM {{ source('bronze', 'bz_participants') }}
    WHERE participant_id IS NOT NULL
),

-- Data quality validation and transformation
validated_participants AS (
    SELECT 
        participant_id,
        meeting_id,
        user_id,
        join_time,
        leave_time,
        load_timestamp,
        update_timestamp,
        source_system,
        -- Data quality score calculation
        CASE 
            WHEN meeting_id IS NOT NULL 
                AND join_time IS NOT NULL 
                AND leave_time IS NOT NULL
                AND leave_time > join_time
            THEN 1.00
            ELSE 0.75
        END AS data_quality_score,
        -- Record status
        CASE 
            WHEN meeting_id IS NULL OR join_time IS NULL OR leave_time IS NULL THEN 'error'
            WHEN leave_time <= join_time THEN 'error'
            ELSE 'active'
        END AS record_status
    FROM bronze_participants
    WHERE rn = 1  -- Deduplication: keep latest record
        AND meeting_id IS NOT NULL
        AND join_time IS NOT NULL
        AND leave_time IS NOT NULL
        AND leave_time > join_time
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
    DATE(load_timestamp) AS load_date,
    DATE(update_timestamp) AS update_date,
    data_quality_score,
    record_status
FROM validated_participants

{% if is_incremental() %}
    WHERE update_timestamp > (SELECT MAX(update_timestamp) FROM {{ this }})
{% endif %}
