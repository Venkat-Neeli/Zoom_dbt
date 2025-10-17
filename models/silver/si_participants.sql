{{
    config(
        materialized='incremental',
        unique_key='participant_id',
        on_schema_change='sync_all_columns',
        pre_hook="{% if not (this.name == 'si_process_audit') %}INSERT INTO {{ ref('si_process_audit') }} (execution_id, pipeline_name, start_time, status, source_system, target_system, process_type, load_date, update_date) VALUES ('{{ invocation_id }}_{{ this.name }}', '{{ this.name }}', CURRENT_TIMESTAMP(), 'STARTED', 'BRONZE', 'SILVER', 'ETL', CURRENT_DATE(), CURRENT_DATE()){% endif %}",
        post_hook="{% if not (this.name == 'si_process_audit') %}UPDATE {{ ref('si_process_audit') }} SET end_time = CURRENT_TIMESTAMP(), status = 'SUCCESS', records_processed = (SELECT COUNT(*) FROM {{ this }}), records_successful = (SELECT COUNT(*) FROM {{ this }} WHERE record_status = 'active'), records_failed = (SELECT COUNT(*) FROM {{ this }} WHERE record_status = 'error'), processing_duration_seconds = DATEDIFF(second, start_time, CURRENT_TIMESTAMP()) WHERE execution_id = '{{ invocation_id }}_{{ this.name }}'{% endif %}"
    )
}}

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
                     load_timestamp DESC
        ) as row_num
    FROM {{ ref('bz_participants') }}
    WHERE participant_id IS NOT NULL
),

deduped_participants AS (
    SELECT *
    FROM bronze_participants
    WHERE row_num = 1
),

data_quality_checks AS (
    SELECT 
        dp.*,
        -- Null checks
        CASE WHEN dp.participant_id IS NOT NULL AND dp.meeting_id IS NOT NULL AND dp.join_time IS NOT NULL AND dp.leave_time IS NOT NULL THEN 1 ELSE 0 END as null_check,
        -- Range checks
        CASE WHEN dp.leave_time > dp.join_time THEN 1 ELSE 0 END as range_check,
        -- Referential integrity checks
        CASE WHEN EXISTS (SELECT 1 FROM {{ ref('si_meetings') }} m WHERE m.meeting_id = dp.meeting_id) THEN 1 ELSE 0 END as meeting_ref_check,
        CASE WHEN dp.user_id IS NULL OR EXISTS (SELECT 1 FROM {{ ref('si_users') }} u WHERE u.user_id = dp.user_id) THEN 1 ELSE 0 END as user_ref_check
    FROM deduped_participants dp
),

transformed_participants AS (
    SELECT 
        participant_id,
        meeting_id,
        user_id,
        join_time,
        leave_time,
        load_timestamp,
        update_timestamp,
        source_system,
        DATE(load_timestamp) as load_date,
        DATE(update_timestamp) as update_date,
        {{ calculate_data_quality_score('null_check = 1', 'range_check = 1', 'meeting_ref_check = 1 AND user_ref_check = 1') }} as data_quality_score,
        CASE 
            WHEN null_check = 1 AND range_check = 1 AND meeting_ref_check = 1 AND user_ref_check = 1 THEN 'active'
            ELSE 'error'
        END as record_status
    FROM data_quality_checks
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
FROM transformed_participants
WHERE record_status = 'active'

{% if is_incremental() %}
    AND update_timestamp > (SELECT MAX(update_timestamp) FROM {{ this }})
{% endif %}
