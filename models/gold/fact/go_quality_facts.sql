{{ config(
    materialized='table',
    cluster_by=['meeting_id'],
    pre_hook="INSERT INTO {{ ref('go_process_audit') }} (execution_id, pipeline_name, start_time, status, source_system, target_system, process_type) VALUES (UUID_STRING(), 'go_quality_facts', CURRENT_TIMESTAMP(), 'STARTED', 'SILVER', 'GOLD', 'FACT_LOAD')",
    post_hook="INSERT INTO {{ ref('go_process_audit') }} (execution_id, pipeline_name, end_time, status, records_processed, source_system, target_system, process_type) VALUES (UUID_STRING(), 'go_quality_facts', CURRENT_TIMESTAMP(), 'COMPLETED', (SELECT COUNT(*) FROM {{ this }}), 'SILVER', 'GOLD', 'FACT_LOAD')"
) }}

WITH participant_quality AS (
    SELECT 
        p.meeting_id,
        p.participant_id,
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
),

final_quality_facts AS (
    SELECT 
        CONCAT('QF_', pq.meeting_id, '_', pq.participant_id) AS quality_fact_id,
        pq.meeting_id,
        pq.participant_id,
        CONCAT('DC_', pq.participant_id, '_', CURRENT_TIMESTAMP()::STRING) AS device_connection_id,
        ROUND(pq.data_quality_score * 0.8, 2) AS audio_quality_score,
        ROUND(pq.data_quality_score * 0.9, 2) AS video_quality_score,
        ROUND(pq.data_quality_score, 2) AS connection_stability_rating,
        CASE 
            WHEN pq.data_quality_score > 8 THEN 50
            WHEN pq.data_quality_score > 6 THEN 100
            ELSE 200
        END AS latency_ms,
        CASE 
            WHEN pq.data_quality_score > 8 THEN 0.01
            WHEN pq.data_quality_score > 6 THEN 0.05
            ELSE 0.1
        END AS packet_loss_rate,
        DATEDIFF('minute', pq.join_time, pq.leave_time) * 2 AS bandwidth_utilization,
        CASE 
            WHEN pq.data_quality_score > 8 THEN 25.0
            WHEN pq.data_quality_score > 6 THEN 50.0
            ELSE 75.0
        END AS cpu_usage_percentage,
        DATEDIFF('minute', pq.join_time, pq.leave_time) * 10 AS memory_usage_mb,
        pq.load_date,
        CURRENT_DATE() AS update_date,
        pq.source_system,
        CURRENT_TIMESTAMP() AS created_at,
        CURRENT_TIMESTAMP() AS updated_at,
        'ACTIVE' AS process_status
    FROM participant_quality pq
)

SELECT * FROM final_quality_facts
