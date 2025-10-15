{{
    config(
        materialized='incremental',
        unique_key='participant_id',
        on_schema_change='fail',
        pre_hook="{% if this.name != 'si_process_audit' %}INSERT INTO {{ ref('si_process_audit') }} (execution_id, pipeline_name, start_time, status, source_system, target_system, process_type, load_date, update_date) SELECT '{{ invocation_id }}', 'si_participants_transform', current_timestamp(), 'STARTED', 'BRONZE', 'SILVER', 'ETL', current_date(), current_date(){% endif %}",
        post_hook="{% if this.name != 'si_process_audit' %}INSERT INTO {{ ref('si_process_audit') }} (execution_id, pipeline_name, end_time, status, records_processed, source_system, target_system, process_type, load_date, update_date) SELECT '{{ invocation_id }}', 'si_participants_transform', current_timestamp(), 'COMPLETED', (SELECT count(*) FROM {{ this }}), 'BRONZE', 'SILVER', 'ETL', current_date(), current_date(){% endif %}"
    )
}}

-- Transform bronze participants to silver participants with data quality checks
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
    FROM {{ ref('bz_participants') }}
    WHERE participant_id IS NOT NULL
        AND meeting_id IS NOT NULL
        AND join_time IS NOT NULL
        AND leave_time IS NOT NULL
),

-- Data quality validation and cleansing
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
        -- Calculate data quality score
        CASE 
            WHEN join_time < leave_time
            THEN 1.00
            ELSE 0.50
        END AS data_quality_score,
        CASE 
            WHEN join_time < leave_time
            THEN 'active'
            ELSE 'error'
        END AS record_status
    FROM bronze_participants
    WHERE rn = 1  -- Deduplication: keep latest record
        AND join_time < leave_time
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
FROM cleaned_participants

{% if is_incremental() %}
  WHERE update_timestamp > (SELECT COALESCE(MAX(update_timestamp), '1900-01-01') FROM {{ this }})
{% endif %}
