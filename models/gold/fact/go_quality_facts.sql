{{ config(
    materialized='incremental',
    unique_key='quality_fact_id',
    on_schema_change='sync_all_columns',
    pre_hook="INSERT INTO {{ ref('go_process_audit') }} (execution_id, pipeline_name, start_time, status, process_type, user_executed, source_system, target_system) VALUES (UUID_STRING(), 'go_quality_facts_load', CURRENT_TIMESTAMP(), 'STARTED', 'FACT_LOAD', 'DBT_USER', 'SILVER', 'GOLD') WHERE '{{ this.name }}' != 'go_process_audit'",
    post_hook="UPDATE {{ ref('go_process_audit') }} SET end_time = CURRENT_TIMESTAMP(), status = 'COMPLETED', records_processed = (SELECT COUNT(*) FROM {{ this }}) WHERE pipeline_name = 'go_quality_facts_load' AND status = 'STARTED' AND '{{ this.name }}' != 'go_process_audit'"
) }}

WITH participant_base AS (
    SELECT 
        participant_id,
        meeting_id,
        user_id,
        join_time,
        leave_time,
        load_date,
        update_date,
        source_system,
        data_quality_score,
        record_status
    FROM {{ ref('si_participants') }}
    WHERE record_status = 'ACTIVE'
        AND data_quality_score >= 0.7
        {% if is_incremental() %}
        AND update_date >= (SELECT MAX(update_date) FROM {{ this }})
        {% endif %}
),

final_facts AS (
    SELECT 
        CONCAT('QF_', meeting_id, '_', participant_id) AS quality_fact_id,
        meeting_id,
        participant_id,
        CONCAT('DC_', participant_id, '_', CURRENT_TIMESTAMP()::STRING) AS device_connection_id,
        ROUND(data_quality_score * 0.8, 2) AS audio_quality_score,
        ROUND(data_quality_score * 0.9, 2) AS video_quality_score,
        ROUND(data_quality_score, 2) AS connection_stability_rating,
        CASE 
            WHEN data_quality_score > 8 THEN 50
            WHEN data_quality_score > 6 THEN 100
            ELSE 200
        END AS latency_ms,
        CASE 
            WHEN data_quality_score > 8 THEN 0.01
            WHEN data_quality_score > 6 THEN 0.05
            ELSE 0.1
        END AS packet_loss_rate,
        DATEDIFF('minute', join_time, leave_time) * 2 AS bandwidth_utilization,
        CASE 
            WHEN data_quality_score > 8 THEN 25.0
            WHEN data_quality_score > 6 THEN 50.0
            ELSE 75.0
        END AS cpu_usage_percentage,
        DATEDIFF('minute', join_time, leave_time) * 10 AS memory_usage_mb,
        load_date,
        CURRENT_DATE() AS update_date,
        source_system
    FROM participant_base
)

SELECT * FROM final_facts
