{{ config(
    materialized='table',
    cluster_by=['load_date']
) }}

WITH meeting_base AS (
    SELECT 
        meeting_id,
        host_id,
        meeting_topic,
        start_time,
        end_time,
        duration_minutes,
        load_timestamp,
        update_timestamp,
        source_system,
        load_date,
        update_date,
        data_quality_score,
        record_status
    FROM {{ ref('si_meetings') }}
    WHERE record_status = 'ACTIVE'
        AND data_quality_score >= 0.8
        AND meeting_id IS NOT NULL
        AND host_id IS NOT NULL
),

participant_aggregates AS (
    SELECT 
        meeting_id,
        COUNT(DISTINCT participant_id) as total_participants,
        AVG(DATEDIFF('minute', join_time, leave_time)) as avg_participation_duration,
        SUM(DATEDIFF('minute', join_time, leave_time)) as total_participation_minutes
    FROM {{ ref('si_participants') }}
    WHERE record_status = 'ACTIVE'
        AND join_time IS NOT NULL
        AND leave_time IS NOT NULL
    GROUP BY meeting_id
),

feature_aggregates AS (
    SELECT 
        meeting_id,
        COUNT(DISTINCT feature_name) as features_used_count,
        SUM(usage_count) as total_feature_usage
    FROM {{ ref('si_feature_usage') }}
    WHERE record_status = 'ACTIVE'
    GROUP BY meeting_id
)

SELECT 
    -- Primary Keys
    mb.meeting_id,
    
    -- Meeting Details
    mb.host_id,
    mb.meeting_topic,
    mb.start_time,
    mb.end_time,
    mb.duration_minutes,
    
    -- Participant Metrics
    COALESCE(pa.total_participants, 0) as total_participants,
    COALESCE(pa.avg_participation_duration, 0) as avg_participation_duration_minutes,
    COALESCE(pa.total_participation_minutes, 0) as total_participation_minutes,
    
    -- Feature Usage Metrics
    COALESCE(fa.features_used_count, 0) as features_used_count,
    COALESCE(fa.total_feature_usage, 0) as total_feature_usage,
    
    -- Calculated Fields
    CASE 
        WHEN mb.duration_minutes > 0 THEN 
            ROUND((COALESCE(pa.total_participation_minutes, 0) / mb.duration_minutes) * 100, 2)
        ELSE 0 
    END as participation_rate_percent,
    
    DATE(mb.start_time) as meeting_date,
    EXTRACT(HOUR FROM mb.start_time) as meeting_hour,
    DAYOFWEEK(mb.start_time) as meeting_day_of_week,
    
    -- Quality and Audit Fields
    mb.data_quality_score,
    mb.source_system,
    mb.load_date,
    mb.update_date,
    CURRENT_TIMESTAMP() as created_at,
    CURRENT_TIMESTAMP() as updated_at
    
FROM meeting_base mb
LEFT JOIN participant_aggregates pa ON mb.meeting_id = pa.meeting_id
LEFT JOIN feature_aggregates fa ON mb.meeting_id = fa.meeting_id
