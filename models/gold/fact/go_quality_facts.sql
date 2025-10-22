{{ config(
    materialized='incremental',
    on_schema_change='sync_all_columns',
    unique_key='quality_fact_id',
    pre_hook="INSERT INTO {{ ref('go_process_audit') }} (execution_id, pipeline_name, start_time, status, process_type, user_executed, load_date) SELECT UUID_STRING(), 'go_quality_facts', CURRENT_TIMESTAMP(), 'STARTED', 'FACT_LOAD', 'DBT_USER', CURRENT_DATE() WHERE '{{ this.name }}' != 'go_process_audit'",
    post_hook="INSERT INTO {{ ref('go_process_audit') }} (execution_id, pipeline_name, end_time, status, records_processed, process_type, user_executed, load_date) SELECT UUID_STRING(), 'go_quality_facts', CURRENT_TIMESTAMP(), 'COMPLETED', (SELECT COUNT(*) FROM {{ this }}), 'FACT_LOAD', 'DBT_USER', CURRENT_DATE() WHERE '{{ this.name }}' != 'go_process_audit'"
) }}

WITH participant_quality_base AS (
    SELECT 
        p.participant_id,
        p.meeting_id,
        p.user_id,
        p.join_time,
        p.leave_time,
        p.data_quality_score,
        p.load_date,
        p.update_date,
        p.source_system,
        p.record_status
    FROM {{ ref('si_participants') }} p
    WHERE p.record_status = 'ACTIVE'
        AND p.data_quality_score >= 0.7
        {% if is_incremental() %}
        AND p.update_date > (SELECT MAX(update_date) FROM {{ this }})
        {% endif %}
),

final_quality_facts AS (
    SELECT 
        CONCAT('QF_', pqb.meeting_id, '_', pqb.participant_id) AS quality_fact_id,
        pqb.meeting_id,
        pqb.participant_id,
        CONCAT('DC_', pqb.participant_id, '_', CURRENT_TIMESTAMP()::STRING) AS device_connection_id,
        ROUND(pqb.data_quality_score * 0.8, 2) AS audio_quality_score,
        ROUND(pqb.data_quality_score * 0.9, 2) AS video_quality_score,
        ROUND(pqb.data_quality_score, 2) AS connection_stability_rating,
        CASE 
            WHEN pqb.data_quality_score > 8 THEN 50
            WHEN pqb.data_quality_score > 6 THEN 100
            ELSE 200
        END AS latency_ms,
        CASE 
            WHEN pqb.data_quality_score > 8 THEN 0.01
            WHEN pqb.data_quality_score > 6 THEN 0.05
            ELSE 0.1
        END AS packet_loss_rate,
        DATEDIFF('minute', pqb.join_time, pqb.leave_time) * 2 AS bandwidth_utilization,
        CASE 
            WHEN pqb.data_quality_score > 8 THEN 25.0
            WHEN pqb.data_quality_score > 6 THEN 50.0
            ELSE 75.0
        END AS cpu_usage_percentage,
        DATEDIFF('minute', pqb.join_time, pqb.leave_time) * 10 AS memory_usage_mb,
        pqb.load_date,
        CURRENT_DATE() AS update_date,
        pqb.source_system
    FROM participant_quality_base pqb
)

SELECT * FROM final_quality_facts
