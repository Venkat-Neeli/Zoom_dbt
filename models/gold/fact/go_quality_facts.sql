{{ config(
    materialized='incremental',
    on_schema_change='sync_all_columns',
    unique_key='quality_fact_id',
    pre_hook="INSERT INTO {{ ref('go_process_audit') }} (execution_id, pipeline_name, start_time, status, source_system, target_system, process_type) SELECT UUID_STRING(), 'go_quality_facts', CURRENT_TIMESTAMP(), 'STARTED', 'SILVER', 'GOLD', 'FACT_LOAD' WHERE '{{ this.name }}' != 'go_process_audit'",
    post_hook="INSERT INTO {{ ref('go_process_audit') }} (execution_id, pipeline_name, end_time, status, records_processed, source_system, target_system, process_type) SELECT UUID_STRING(), 'go_quality_facts', CURRENT_TIMESTAMP(), 'COMPLETED', (SELECT COUNT(*) FROM {{ this }}), 'SILVER', 'GOLD', 'FACT_LOAD' WHERE '{{ this.name }}' != 'go_process_audit'"
) }}

WITH quality_base AS (
    SELECT 
        p.meeting_id,
        p.participant_id,
        p.join_time,
        p.leave_time,
        p.data_quality_score,
        p.load_date,
        p.update_date,
        p.source_system
    FROM {{ ref('si_participants') }} p
    WHERE p.record_status = 'ACTIVE'
        AND p.data_quality_score IS NOT NULL
    {% if is_incremental() %}
        AND p.update_date > (SELECT MAX(update_date) FROM {{ this }})
    {% endif %}
),

final_transform AS (
    SELECT 
        CONCAT('QF_', qb.meeting_id, '_', qb.participant_id) AS quality_fact_id,
        qb.meeting_id,
        qb.participant_id,
        CONCAT('DC_', qb.participant_id, '_', CURRENT_TIMESTAMP()::STRING) AS device_connection_id,
        ROUND(qb.data_quality_score * 0.8, 2) AS audio_quality_score,
        ROUND(qb.data_quality_score * 0.9, 2) AS video_quality_score,
        ROUND(qb.data_quality_score, 2) AS connection_stability_rating,
        CASE 
            WHEN qb.data_quality_score > 8 THEN 50
            WHEN qb.data_quality_score > 6 THEN 100
            ELSE 200
        END AS latency_ms,
        CASE 
            WHEN qb.data_quality_score > 8 THEN 0.01
            WHEN qb.data_quality_score > 6 THEN 0.05
            ELSE 0.1
        END AS packet_loss_rate,
        DATEDIFF('minute', qb.join_time, qb.leave_time) * 2 AS bandwidth_utilization,
        CASE 
            WHEN qb.data_quality_score > 8 THEN 25.0
            WHEN qb.data_quality_score > 6 THEN 50.0
            ELSE 75.0
        END AS cpu_usage_percentage,
        DATEDIFF('minute', qb.join_time, qb.leave_time) * 10 AS memory_usage_mb,
        qb.load_date,
        CURRENT_DATE() AS update_date,
        qb.source_system
    FROM quality_base qb
)

SELECT * FROM final_transform
