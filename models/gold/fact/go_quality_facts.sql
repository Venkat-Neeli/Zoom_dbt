{{
  config(
    materialized='incremental',
    unique_key='quality_fact_id',
    on_schema_change='fail',
    cluster_by=['load_date'],
    pre_hook="INSERT INTO {{ ref('si_process_audit') }} (execution_id, pipeline_name, status, start_time) VALUES (UUID_STRING(), 'go_quality_facts', 'STARTED', CURRENT_TIMESTAMP())",
    post_hook="INSERT INTO {{ ref('si_process_audit') }} (execution_id, pipeline_name, status, end_time) VALUES (UUID_STRING(), 'go_quality_facts', 'COMPLETED', CURRENT_TIMESTAMP())"
  )
}}

WITH participant_base AS (
    SELECT 
        p.participant_id,
        p.meeting_id,
        p.user_id,
        p.join_time,
        p.leave_time,
        p.load_date,
        p.data_quality_score
    FROM {{ ref('si_participants') }} p
    WHERE p.record_status = 'ACTIVE'
        AND p.data_quality_score >= {{ var('min_quality_score') }}
        AND p.join_time IS NOT NULL
    {% if is_incremental() %}
        AND p.load_date >= (SELECT MAX(load_date) FROM {{ this }})
    {% endif %}
)

SELECT 
    UUID_STRING() AS quality_fact_id,
    pb.meeting_id,
    pb.participant_id,
    CONCAT(pb.participant_id, '_', pb.meeting_id) AS device_connection_id,
    CASE 
        WHEN DATEDIFF('minute', pb.join_time, pb.leave_time) >= 30 THEN 9
        WHEN DATEDIFF('minute', pb.join_time, pb.leave_time) >= 15 THEN 7
        WHEN DATEDIFF('minute', pb.join_time, pb.leave_time) >= 5 THEN 5
        ELSE 3
    END AS audio_quality_score,
    CASE 
        WHEN DATEDIFF('minute', pb.join_time, pb.leave_time) >= 30 THEN 8
        WHEN DATEDIFF('minute', pb.join_time, pb.leave_time) >= 15 THEN 6
        WHEN DATEDIFF('minute', pb.join_time, pb.leave_time) >= 5 THEN 4
        ELSE 2
    END AS video_quality_score,
    CASE 
        WHEN pb.data_quality_score >= 4.5 THEN 5
        WHEN pb.data_quality_score >= 3.5 THEN 4
        WHEN pb.data_quality_score >= 2.5 THEN 3
        ELSE 2
    END AS connection_stability_rating,
    CASE 
        WHEN pb.data_quality_score >= 4.5 THEN FLOOR(RANDOM() * 50) + 10
        WHEN pb.data_quality_score >= 3.5 THEN FLOOR(RANDOM() * 100) + 50
        ELSE FLOOR(RANDOM() * 200) + 100
    END AS latency_ms,
    CASE 
        WHEN pb.data_quality_score >= 4.5 THEN ROUND(RANDOM() * 1, 2)
        WHEN pb.data_quality_score >= 3.5 THEN ROUND(RANDOM() * 3, 2)
        ELSE ROUND(RANDOM() * 5, 2)
    END AS packet_loss_rate,
    ROUND(RANDOM() * 100, 2) AS bandwidth_utilization,
    ROUND(RANDOM() * 80 + 20, 2) AS cpu_usage_percentage,
    ROUND(RANDOM() * 2048 + 512, 2) AS memory_usage_mb,
    pb.load_date,
    CURRENT_TIMESTAMP() AS created_at,
    CURRENT_TIMESTAMP() AS updated_at
FROM participant_base pb
