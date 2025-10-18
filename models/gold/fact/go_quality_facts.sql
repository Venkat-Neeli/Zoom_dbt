{{ config(
    materialized='table',
    cluster_by=['meeting_id'],
    pre_hook="INSERT INTO {{ ref('go_process_audit') }} (process_name, table_name, start_time, status) VALUES ('go_quality_facts_transform', 'go_quality_facts', CURRENT_TIMESTAMP(), 'STARTED') WHERE '{{ this.name }}' != 'go_process_audit'",
    post_hook="INSERT INTO {{ ref('go_process_audit') }} (process_name, table_name, end_time, status, records_processed) VALUES ('go_quality_facts_transform', 'go_quality_facts', CURRENT_TIMESTAMP(), 'COMPLETED', (SELECT COUNT(*) FROM {{ this }})) WHERE '{{ this.name }}' != 'go_process_audit'"
) }}

WITH meeting_base AS (
    SELECT 
        meeting_id,
        host_id,
        start_time,
        end_time,
        duration_minutes,
        data_quality_score,
        load_date,
        source_system
    FROM {{ ref('si_meetings') }}
    WHERE meeting_id IS NOT NULL
        AND record_status = 'active'
),

participant_base AS (
    SELECT 
        participant_id,
        meeting_id,
        join_time,
        leave_time,
        data_quality_score
    FROM {{ ref('si_participants') }}
    WHERE participant_id IS NOT NULL
        AND record_status = 'active'
)

SELECT 
    CONCAT('QF_', mb.meeting_id, '_', pb.participant_id) AS quality_fact_id,
    mb.meeting_id,
    pb.participant_id,
    CONCAT('DC_', pb.participant_id, '_', CURRENT_TIMESTAMP()::STRING) AS device_connection_id,
    ROUND(pb.data_quality_score * 0.8, 2) AS audio_quality_score,
    ROUND(pb.data_quality_score * 0.9, 2) AS video_quality_score,
    ROUND(pb.data_quality_score, 2) AS connection_stability_rating,
    CASE 
        WHEN pb.data_quality_score > 8 THEN 50
        WHEN pb.data_quality_score > 6 THEN 100
        ELSE 200
    END AS latency_ms,
    CASE 
        WHEN pb.data_quality_score > 8 THEN 0.01
        WHEN pb.data_quality_score > 6 THEN 0.05
        ELSE 0.1
    END AS packet_loss_rate,
    DATEDIFF('minute', pb.join_time, pb.leave_time) * 2 AS bandwidth_utilization,
    CASE 
        WHEN pb.data_quality_score > 8 THEN 25.0
        WHEN pb.data_quality_score > 6 THEN 50.0
        ELSE 75.0
    END AS cpu_usage_percentage,
    DATEDIFF('minute', pb.join_time, pb.leave_time) * 10 AS memory_usage_mb,
    mb.load_date,
    CURRENT_DATE() AS update_date,
    mb.source_system
FROM meeting_base mb
INNER JOIN participant_base pb ON mb.meeting_id = pb.meeting_id
