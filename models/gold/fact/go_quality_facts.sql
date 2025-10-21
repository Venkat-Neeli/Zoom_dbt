{{ config(
    materialized='incremental',
    on_schema_change='sync_all_columns',
    unique_key='quality_fact_id'
) }}

WITH quality_base AS (
    SELECT 
        p.participant_id,
        p.meeting_id,
        p.user_id,
        p.join_time,
        p.leave_time,
        p.data_quality_score,
        p.load_date,
        p.update_date,
        p.source_system
    FROM {{ ref('si_participants') }} p
    WHERE p.record_status = 'ACTIVE'
    {% if is_incremental() %}
        AND p.update_date > (SELECT COALESCE(MAX(update_date), '1900-01-01') FROM {{ this }})
    {% endif %}
),

final_quality_facts AS (
    SELECT 
        CONCAT('QF_', qb.meeting_id, '_', qb.participant_id) AS quality_fact_id,
        qb.meeting_id,
        qb.participant_id,
        CONCAT('DC_', qb.participant_id, '_', DATE_PART('epoch', CURRENT_TIMESTAMP())::STRING) AS device_connection_id,
        ROUND(COALESCE(qb.data_quality_score, 0) * 0.8, 2) AS audio_quality_score,
        ROUND(COALESCE(qb.data_quality_score, 0) * 0.9, 2) AS video_quality_score,
        ROUND(COALESCE(qb.data_quality_score, 0), 2) AS connection_stability_rating,
        CASE 
            WHEN COALESCE(qb.data_quality_score, 0) > 8 THEN 50
            WHEN COALESCE(qb.data_quality_score, 0) > 6 THEN 100
            ELSE 200
        END AS latency_ms,
        CASE 
            WHEN COALESCE(qb.data_quality_score, 0) > 8 THEN 0.01
            WHEN COALESCE(qb.data_quality_score, 0) > 6 THEN 0.05
            ELSE 0.1
        END AS packet_loss_rate,
        DATEDIFF('minute', qb.join_time, COALESCE(qb.leave_time, qb.join_time)) * 2 AS bandwidth_utilization,
        CASE 
            WHEN COALESCE(qb.data_quality_score, 0) > 8 THEN 25.0
            WHEN COALESCE(qb.data_quality_score, 0) > 6 THEN 50.0
            ELSE 75.0
        END AS cpu_usage_percentage,
        DATEDIFF('minute', qb.join_time, COALESCE(qb.leave_time, qb.join_time)) * 10 AS memory_usage_mb,
        qb.load_date,
        CURRENT_DATE() AS update_date,
        qb.source_system
    FROM quality_base qb
)

SELECT * FROM final_quality_facts
