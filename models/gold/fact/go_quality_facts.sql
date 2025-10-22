{{ config(
    materialized='table'
) }}

SELECT 
    CONCAT('QF_', meeting_id, '_', participant_id) AS quality_fact_id,
    meeting_id,
    participant_id,
    CONCAT('DC_', participant_id) AS device_connection_id,
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
FROM {{ ref('si_participants') }}
WHERE record_status = 'ACTIVE'
    AND data_quality_score >= 0.7
