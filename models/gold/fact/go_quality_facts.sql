{{ config(
    materialized='table'
) }}

SELECT 
    'QF_' || 'SAMPLE_001' AS quality_fact_id,
    'MEETING_001' AS meeting_id,
    'PARTICIPANT_001' AS participant_id,
    'DC_' || 'SAMPLE_001' AS device_connection_id,
    8.5 AS audio_quality_score,
    9.0 AS video_quality_score,
    8.8 AS connection_stability_rating,
    45 AS latency_ms,
    0.02 AS packet_loss_rate,
    120 AS bandwidth_utilization,
    35.5 AS cpu_usage_percentage,
    450 AS memory_usage_mb,
    CURRENT_DATE() AS load_date,
    CURRENT_DATE() AS update_date,
    'ZOOM_API' AS source_system
WHERE FALSE  -- This creates the table structure without data
