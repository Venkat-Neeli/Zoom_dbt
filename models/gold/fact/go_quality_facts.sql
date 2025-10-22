{{ config(
    materialized='table',
    cluster_by=['meeting_id']
) }}

-- Quality Facts Transformation
WITH quality_base AS (
    SELECT 
        participant_id,
        meeting_id,
        data_quality_score,
        join_time,
        leave_time,
        load_date,
        source_system,
        ROW_NUMBER() OVER (PARTITION BY participant_id, meeting_id ORDER BY update_timestamp DESC) as rn
    FROM {{ source('silver', 'si_participants') }}
    WHERE record_status = 'ACTIVE'
      AND join_time IS NOT NULL
      AND data_quality_score IS NOT NULL
)

SELECT 
    CONCAT('QF_', meeting_id, '_', participant_id)::VARCHAR(50) AS quality_fact_id,
    meeting_id::VARCHAR(50) AS meeting_id,
    participant_id::VARCHAR(50) AS participant_id,
    CONCAT('DC_', participant_id, '_', CURRENT_TIMESTAMP()::STRING)::VARCHAR(50) AS device_connection_id,
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
FROM quality_base
WHERE rn = 1
