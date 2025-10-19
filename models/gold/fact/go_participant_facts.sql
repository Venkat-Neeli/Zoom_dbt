{{ config(
    materialized='table',
    cluster_by=['participation_date', 'meeting_id'],
    pre_hook="INSERT INTO {{ ref('audit_log') }} (table_name, operation, timestamp) VALUES ('go_participant_facts', 'transform_start', CURRENT_TIMESTAMP())",
    post_hook="INSERT INTO {{ ref('audit_log') }} (table_name, operation, timestamp) VALUES ('go_participant_facts', 'transform_complete', CURRENT_TIMESTAMP())"
) }}

WITH participant_base AS (
    SELECT 
        participant_id,
        meeting_id,
        user_id,
        join_time,
        leave_time,
        load_timestamp,
        update_timestamp,
        source_system,
        load_date,
        update_date,
        data_quality_score,
        record_status
    FROM {{ ref('si_participants') }}
    WHERE record_status = 'ACTIVE'
        AND data_quality_score >= 0.8
),

meeting_context AS (
    SELECT 
        meeting_id,
        host_id,
        meeting_topic,
        start_time as meeting_start_time,
        end_time as meeting_end_time,
        duration_minutes as meeting_duration
    FROM {{ ref('si_meetings') }}
    WHERE record_status = 'ACTIVE'
        AND data_quality_score >= 0.8
),

user_context AS (
    SELECT 
        user_id,
        user_name,
        email,
        company
    FROM {{ ref('si_users') }}
    WHERE record_status = 'ACTIVE'
        AND data_quality_score >= 0.8
),

feature_usage AS (
    SELECT 
        meeting_id,
        SUM(CASE WHEN feature_name = 'Screen Sharing' THEN usage_count ELSE 0 END) as screen_share_duration,
        SUM(CASE WHEN feature_name = 'Chat' THEN usage_count ELSE 0 END) as chat_messages_sent
    FROM {{ ref('si_feature_usage') }}
    WHERE record_status = 'ACTIVE'
        AND data_quality_score >= 0.8
    GROUP BY meeting_id
),

final AS (
    SELECT 
        -- Primary Keys
        CONCAT('PF_', pb.participant_id, '_', pb.meeting_id) as participant_fact_id,
        pb.meeting_id,
        pb.participant_id,
        COALESCE(pb.user_id, 'GUEST_USER') as user_id,
        
        -- Time Dimensions
        CONVERT_TIMEZONE('UTC', pb.join_time) as join_time,
        CONVERT_TIMEZONE('UTC', pb.leave_time) as leave_time,
        DATE(pb.join_time) as participation_date,
        EXTRACT(HOUR FROM pb.join_time) as join_hour,
        
        -- Duration Metrics
        DATEDIFF('minute', pb.join_time, pb.leave_time) as attendance_duration,
        mc.meeting_duration,
        
        -- Participant Details
        CASE 
            WHEN pb.user_id = mc.host_id THEN 'Host' 
            ELSE 'Participant' 
        END as participant_role,
        
        -- User Context
        uc.user_name,
        uc.email,
        COALESCE(uc.company, 'Unknown') as organization_id,
        
        -- Engagement Metrics
        'Computer Audio' as audio_connection_type,
        FALSE as video_enabled,
        COALESCE(fu.screen_share_duration, 0) as screen_share_duration,
        COALESCE(fu.chat_messages_sent, 0) as chat_messages_sent,
        COALESCE(fu.screen_share_duration, 0) + COALESCE(fu.chat_messages_sent, 0) as interaction_count,
        
        -- Quality and Performance
        ROUND(pb.data_quality_score, 2) as connection_quality_rating,
        'Desktop' as device_type,
        'Unknown' as geographic_location,
        
        -- Meeting Context
        mc.meeting_topic,
        
        -- Calculated Metrics
        CASE 
            WHEN mc.meeting_duration > 0 THEN 
                ROUND(DATEDIFF('minute', pb.join_time, pb.leave_time)::FLOAT / mc.meeting_duration, 4)
            ELSE 0 
        END as attendance_rate,
        
        -- Timeliness Analysis
        CASE 
            WHEN pb.join_time <= mc.meeting_start_time THEN 'ON_TIME'
            WHEN DATEDIFF('minute', mc.meeting_start_time, pb.join_time) <= 5 THEN 'SLIGHTLY_LATE'
            ELSE 'LATE'
        END as join_timeliness,
        
        -- Audit Fields
        pb.load_date,
        CURRENT_DATE() as update_date,
        pb.source_system,
        CURRENT_TIMESTAMP() as created_at,
        CURRENT_TIMESTAMP() as updated_at
        
    FROM participant_base pb
    INNER JOIN meeting_context mc ON pb.meeting_id = mc.meeting_id
    LEFT JOIN user_context uc ON pb.user_id = uc.user_id
    LEFT JOIN feature_usage fu ON pb.meeting_id = fu.meeting_id
)

SELECT * FROM final
