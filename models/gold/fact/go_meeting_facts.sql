{{ config(
    materialized='table',
    cluster_by=['meeting_date', 'account_id'],
    pre_hook="INSERT INTO {{ ref('audit_log') }} (table_name, operation, timestamp) VALUES ('go_meeting_facts', 'transform_start', CURRENT_TIMESTAMP())",
    post_hook="INSERT INTO {{ ref('audit_log') }} (table_name, operation, timestamp) VALUES ('go_meeting_facts', 'transform_complete', CURRENT_TIMESTAMP())"
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
),

participant_aggregates AS (
    SELECT 
        meeting_id,
        COUNT(*) as total_participants,
        COUNT(CASE WHEN join_time IS NOT NULL THEN 1 END) as actual_participants,
        AVG(DATEDIFF('minute', join_time, leave_time)) as avg_participant_duration,
        MAX(DATEDIFF('minute', join_time, leave_time)) as max_participant_duration,
        MIN(DATEDIFF('minute', join_time, leave_time)) as min_participant_duration
    FROM {{ ref('si_participants') }}
    WHERE record_status = 'ACTIVE'
        AND data_quality_score >= 0.8
    GROUP BY meeting_id
),

feature_aggregates AS (
    SELECT 
        meeting_id,
        SUM(CASE WHEN feature_name = 'Screen Sharing' THEN usage_count ELSE 0 END) as screen_share_count,
        SUM(CASE WHEN feature_name = 'Chat' THEN usage_count ELSE 0 END) as chat_message_count,
        SUM(CASE WHEN feature_name = 'Breakout Rooms' THEN usage_count ELSE 0 END) as breakout_room_count,
        CASE WHEN SUM(CASE WHEN feature_name = 'Recording' THEN 1 ELSE 0 END) > 0 THEN TRUE ELSE FALSE END as recording_enabled
    FROM {{ ref('si_feature_usage') }}
    WHERE record_status = 'ACTIVE'
        AND data_quality_score >= 0.8
    GROUP BY meeting_id
),

final AS (
    SELECT 
        -- Primary Keys
        CONCAT('MF_', mb.meeting_id, '_', CURRENT_TIMESTAMP()::STRING) as meeting_fact_id,
        mb.meeting_id,
        mb.host_id,
        
        -- Meeting Details
        TRIM(COALESCE(mb.meeting_topic, 'No Topic Specified')) as meeting_topic,
        CONVERT_TIMEZONE('UTC', mb.start_time) as start_time,
        CONVERT_TIMEZONE('UTC', mb.end_time) as end_time,
        
        -- Duration and Time Dimensions
        CASE 
            WHEN mb.duration_minutes > 0 THEN mb.duration_minutes 
            ELSE DATEDIFF('minute', mb.start_time, mb.end_time) 
        END as duration_minutes,
        DATE(mb.start_time) as meeting_date,
        EXTRACT(HOUR FROM mb.start_time) as meeting_hour,
        EXTRACT(DOW FROM mb.start_time) as day_of_week,
        
        -- Participant Metrics
        COALESCE(pa.total_participants, 0) as participant_count,
        COALESCE(pa.actual_participants, 0) as max_concurrent_participants,
        COALESCE(pa.total_participants * pa.avg_participant_duration, 0) as total_attendance_minutes,
        COALESCE(pa.avg_participant_duration, 0) as average_attendance_duration,
        
        -- Meeting Classification
        CASE 
            WHEN mb.duration_minutes < 15 THEN 'Quick Meeting'
            WHEN mb.duration_minutes < 60 THEN 'Standard Meeting'
            ELSE 'Extended Meeting'
        END as meeting_type,
        
        CASE 
            WHEN mb.end_time IS NOT NULL THEN 'Completed'
            WHEN mb.start_time <= CURRENT_TIMESTAMP() THEN 'In Progress'
            ELSE 'Scheduled'
        END as meeting_status,
        
        -- Feature Usage
        COALESCE(fa.recording_enabled, FALSE) as recording_enabled,
        COALESCE(fa.screen_share_count, 0) as screen_share_count,
        COALESCE(fa.chat_message_count, 0) as chat_message_count,
        COALESCE(fa.breakout_room_count, 0) as breakout_room_count,
        
        -- Quality and Engagement
        ROUND(mb.data_quality_score, 2) as quality_score_avg,
        ROUND(
            (COALESCE(fa.chat_message_count, 0) * 0.3 + 
             COALESCE(fa.screen_share_count, 0) * 0.4 + 
             COALESCE(pa.total_participants, 0) * 0.3) / 10, 2
        ) as engagement_score,
        
        -- Audit Fields
        mb.load_date,
        CURRENT_DATE() as update_date,
        mb.source_system,
        CURRENT_TIMESTAMP() as created_at,
        CURRENT_TIMESTAMP() as updated_at
        
    FROM meeting_base mb
    LEFT JOIN participant_aggregates pa ON mb.meeting_id = pa.meeting_id
    LEFT JOIN feature_aggregates fa ON mb.meeting_id = fa.meeting_id
)

SELECT * FROM final
