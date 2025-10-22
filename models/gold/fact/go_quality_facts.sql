{{ config(
    materialized='table',
    cluster_by=['meeting_id'],
    pre_hook="INSERT INTO {{ this.schema }}.go_process_audit (execution_id, pipeline_name, process_type, start_time, status, source_system, target_system, user_executed, load_date) SELECT UUID_STRING(), 'Quality Facts Load', 'DBT_MODEL', CURRENT_TIMESTAMP(), 'STARTED', 'SILVER', 'GOLD', 'DBT_CLOUD', CURRENT_DATE() WHERE '{{ this.name }}' != 'go_process_audit'",
    post_hook="INSERT INTO {{ this.schema }}.go_process_audit (execution_id, pipeline_name, process_type, start_time, end_time, status, records_processed, source_system, target_system, user_executed, processing_duration_seconds, load_date) SELECT UUID_STRING(), 'Quality Facts Load', 'DBT_MODEL', CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP(), 'COMPLETED', (SELECT COUNT(*) FROM {{ this }}), 'SILVER', 'GOLD', 'DBT_CLOUD', 0, CURRENT_DATE() WHERE '{{ this.name }}' != 'go_process_audit'"
) }}

-- Gold Quality Facts
WITH participant_quality AS (
    SELECT 
        meeting_id,
        participant_id,
        data_quality_score,
        join_time,
        leave_time,
        load_date,
        source_system
    FROM {{ source('silver', 'si_participants') }}
    WHERE record_status = 'ACTIVE'
    AND participant_id IS NOT NULL
    AND data_quality_score IS NOT NULL
),

quality_facts AS (
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
        DATEDIFF('minute', join_time, COALESCE(leave_time, CURRENT_TIMESTAMP())) * 2 AS bandwidth_utilization,
        CASE 
            WHEN data_quality_score > 8 THEN 25.0
            WHEN data_quality_score > 6 THEN 50.0
            ELSE 75.0
        END AS cpu_usage_percentage,
        DATEDIFF('minute', join_time, COALESCE(leave_time, CURRENT_TIMESTAMP())) * 10 AS memory_usage_mb,
        load_date,
        CURRENT_DATE() AS update_date,
        source_system
    FROM participant_quality
)

SELECT * FROM quality_facts
