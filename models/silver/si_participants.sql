{{
    config(
        materialized='incremental',
        unique_key='participant_id',
        on_schema_change='sync_all_columns',
        pre_hook="INSERT INTO {{ ref('si_process_audit') }} (execution_id, pipeline_name, start_time, status, source_system, target_system, process_type, load_date, update_date) SELECT '{{ dbt_utils.generate_surrogate_key(['current_timestamp()']) }}', 'si_participants', CURRENT_TIMESTAMP(), 'STARTED', 'Bronze', 'Silver', 'ETL', CURRENT_DATE(), CURRENT_DATE() WHERE '{{ this.name }}' != 'si_process_audit'",
        post_hook="INSERT INTO {{ ref('si_process_audit') }} (execution_id, pipeline_name, end_time, status, records_processed, source_system, target_system, process_type, load_date, update_date) SELECT '{{ dbt_utils.generate_surrogate_key(['current_timestamp()']) }}', 'si_participants', CURRENT_TIMESTAMP(), 'COMPLETED', (SELECT COUNT(*) FROM {{ this }}), 'Bronze', 'Silver', 'ETL', CURRENT_DATE(), CURRENT_DATE() WHERE '{{ this.name }}' != 'si_process_audit'"
    )
}}

-- Silver Participants transformation with data quality checks
WITH source_data AS (
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
    FROM {{ source('bronze', 'bz_participants') }}
    WHERE participant_id IS NOT NULL
      AND meeting_id IS NOT NULL
      AND join_time IS NOT NULL
      AND leave_time IS NOT NULL
      AND leave_time > join_time
),

deduped_data AS (
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
        -- Data quality score calculation
        CASE 
            WHEN participant_id IS NOT NULL 
                 AND meeting_id IS NOT NULL 
                 AND join_time IS NOT NULL 
                 AND leave_time IS NOT NULL
                 AND leave_time > join_time
            THEN 1.00
            ELSE 0.75
        END AS data_quality_score,
        'active' AS record_status
    FROM source_data
    WHERE row_num = 1
)

SELECT * FROM deduped_data

{% if is_incremental() %}
    WHERE update_timestamp > (SELECT MAX(update_timestamp) FROM {{ this }})
{% endif %}
