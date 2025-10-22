{{ config(
    materialized='table',
    cluster_by=['meeting_id'],
    tags=['fact', 'gold'],
    pre_hook="INSERT INTO {{ ref('go_process_audit') }} (execution_id, pipeline_name, process_type, start_time, status, source_system, target_system, user_executed, server_name, load_date, update_date) SELECT '{{ dbt_utils.generate_surrogate_key(['invocation_id', 'this.name']) }}', 'GO_QUALITY_FACTS', 'FACT_BUILD', CURRENT_TIMESTAMP(), 'STARTED', 'SILVER', 'GOLD', CURRENT_USER(), 'DBT_CLOUD', CURRENT_DATE(), CURRENT_DATE() WHERE '{{ this.name }}' != 'go_process_audit'",
    post_hook="UPDATE {{ ref('go_process_audit') }} SET end_time = CURRENT_TIMESTAMP(), status = 'COMPLETED', records_processed = (SELECT COUNT(*) FROM {{ this }}), records_successful = (SELECT COUNT(*) FROM {{ this }}), processing_duration_seconds = DATEDIFF('second', start_time, CURRENT_TIMESTAMP()) WHERE execution_id = '{{ dbt_utils.generate_surrogate_key(['invocation_id', 'this.name']) }}' AND '{{ this.name }}' != 'go_process_audit'"
) }}

-- Gold Quality Facts Table
-- Meeting and participant quality metrics and performance indicators

WITH participant_quality AS (
    SELECT
        meeting_id,
        participant_id,
        data_quality_score,
        join_time,
        leave_time,
        source_system,
        load_date,
        update_date
    FROM {{ source('silver', 'si_participants') }}
    WHERE record_status = 'ACTIVE'
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

SELECT
    quality_fact_id::VARCHAR(50) AS quality_fact_id,
    meeting_id::VARCHAR(50) AS meeting_id,
    participant_id::VARCHAR(50) AS participant_id,
    device_connection_id::VARCHAR(50) AS device_connection_id,
    audio_quality_score,
    video_quality_score,
    connection_stability_rating,
    latency_ms,
    packet_loss_rate,
    bandwidth_utilization,
    cpu_usage_percentage,
    memory_usage_mb,
    load_date,
    update_date,
    source_system::VARCHAR(100) AS source_system
FROM quality_facts
