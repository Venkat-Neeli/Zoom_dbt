{{
    config(
        materialized='incremental',
        unique_key='meeting_id',
        on_schema_change='sync_all_columns',
        pre_hook="{% if not (this.name == 'si_process_audit') %}INSERT INTO {{ ref('si_process_audit') }} (execution_id, pipeline_name, start_time, status, source_system, target_system, process_type, load_date, update_date) VALUES ('{{ invocation_id }}_{{ this.name }}', '{{ this.name }}', CURRENT_TIMESTAMP(), 'STARTED', 'BRONZE', 'SILVER', 'ETL', CURRENT_DATE(), CURRENT_DATE()){% endif %}",
        post_hook="{% if not (this.name == 'si_process_audit') %}UPDATE {{ ref('si_process_audit') }} SET end_time = CURRENT_TIMESTAMP(), status = 'SUCCESS', records_processed = (SELECT COUNT(*) FROM {{ this }}), records_successful = (SELECT COUNT(*) FROM {{ this }} WHERE record_status = 'active'), records_failed = (SELECT COUNT(*) FROM {{ this }} WHERE record_status = 'error'), processing_duration_seconds = DATEDIFF(second, start_time, CURRENT_TIMESTAMP()) WHERE execution_id = '{{ invocation_id }}_{{ this.name }}'{% endif %}"
    )
}}

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
                     load_timestamp DESC
        ) as row_num
    FROM {{ source('bronze', 'bz_meetings') }}
    WHERE meeting_id IS NOT NULL
),

deduped_meetings AS (
    SELECT *
    FROM bronze_meetings
    WHERE row_num = 1
),

data_quality_checks AS (
    SELECT 
        *,
        -- Null checks
        CASE WHEN meeting_id IS NOT NULL AND host_id IS NOT NULL AND start_time IS NOT NULL AND end_time IS NOT NULL THEN 1 ELSE 0 END as null_check,
        -- Range checks
        CASE WHEN end_time > start_time AND duration_minutes > 0 AND duration_minutes <= 1440 THEN 1 ELSE 0 END as range_check,
        -- Referential integrity (host_id exists in users)
        CASE WHEN EXISTS (SELECT 1 FROM {{ ref('si_users') }} u WHERE u.user_id = deduped_meetings.host_id) THEN 1 ELSE 0 END as ref_check
    FROM deduped_meetings
),

transformed_meetings AS (
    SELECT 
        meeting_id,
        host_id,
        CASE WHEN TRIM(meeting_topic) = '' THEN '000' ELSE TRIM(meeting_topic) END as meeting_topic,
        start_time,
        end_time,
        duration_minutes,
        load_timestamp,
        update_timestamp,
        source_system,
        DATE(load_timestamp) as load_date,
        DATE(update_timestamp) as update_date,
        {{ calculate_data_quality_score('null_check = 1', 'range_check = 1', 'ref_check = 1') }} as data_quality_score,
        CASE 
            WHEN null_check = 1 AND range_check = 1 AND ref_check = 1 THEN 'active'
            ELSE 'error'
        END as record_status
    FROM data_quality_checks
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
FROM transformed_meetings
WHERE record_status = 'active'

{% if is_incremental() %}
    AND update_timestamp > (SELECT MAX(update_timestamp) FROM {{ this }})
{% endif %}
