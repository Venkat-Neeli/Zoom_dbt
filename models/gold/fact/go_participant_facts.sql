{{ config(
    materialized='table',
    cluster_by=['load_date'],
    pre_hook="INSERT INTO {{ ref('audit_log') }} (table_name, operation, start_time, status) VALUES ('go_participant_facts', 'transform_start', CURRENT_TIMESTAMP(), 'RUNNING')",
    post_hook="INSERT INTO {{ ref('audit_log') }} (table_name, operation, end_time, status) VALUES ('go_participant_facts', 'transform_end', CURRENT_TIMESTAMP(), 'SUCCESS')"
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
        AND participant_id IS NOT NULL
        AND meeting_id IS NOT NULL
),

meeting_context AS (
    SELECT 
        meeting_id,
        host_id,
        meeting_topic,
        start_time as meeting_start_time,
        end_time as meeting_end_time,
        duration_minutes as meeting_duration_minutes
    FROM {{ ref('si_meetings') }}
    WHERE record_status = 'ACTIVE'
),

user_context AS (
    SELECT 
        user_id,
        user_name,
        email,
        company,
        plan_type
    FROM {{ ref('si_users') }}
    WHERE record_status = 'ACTIVE'
)

SELECT 
    -- Primary Keys
    pb.participant_id,
    pb.meeting_id,
    pb.user_id,
    
    -- Participation Details
    pb.join_time,
    pb.leave_time,
    
    -- Calculated Participation Metrics
    DATEDIFF('minute', pb.join_time, pb.leave_time) as participation_duration_minutes,
    DATEDIFF('second', pb.join_time, pb.leave_time) as participation_duration_seconds,
    
    -- Meeting Context
    mc.host_id,
    mc.meeting_topic,
    mc.meeting_start_time,
    mc.meeting_end_time,
    mc.meeting_duration_minutes,
    
    -- User Context
    uc.user_name,
    uc.email,
    uc.company,
    uc.plan_type,
    
    -- Participation Analysis
    CASE 
        WHEN pb.join_time <= mc.meeting_start_time THEN 'ON_TIME'
        WHEN DATEDIFF('minute', mc.meeting_start_time, pb.join_time) <= 5 THEN 'SLIGHTLY_LATE'
        ELSE 'LATE'
    END as join_timeliness,
    
    CASE 
        WHEN pb.leave_time >= mc.meeting_end_time THEN 'STAYED_FULL'
        WHEN DATEDIFF('minute', pb.leave_time, mc.meeting_end_time) <= 5 THEN 'LEFT_SLIGHTLY_EARLY'
        ELSE 'LEFT_EARLY'
    END as leave_pattern,
    
    CASE 
        WHEN mc.meeting_duration_minutes > 0 THEN 
            ROUND((DATEDIFF('minute', pb.join_time, pb.leave_time) / mc.meeting_duration_minutes) * 100, 2)
        ELSE 0 
    END as attendance_percentage,
    
    -- Time Dimensions
    DATE(pb.join_time) as participation_date,
    EXTRACT(HOUR FROM pb.join_time) as join_hour,
    DAYOFWEEK(pb.join_time) as join_day_of_week,
    
    -- Quality and Audit Fields
    pb.data_quality_score,
    pb.source_system,
    pb.load_date,
    pb.update_date,
    CURRENT_TIMESTAMP() as created_at,
    CURRENT_TIMESTAMP() as updated_at
    
FROM participant_base pb
LEFT JOIN meeting_context mc ON pb.meeting_id = mc.meeting_id
LEFT JOIN user_context uc ON pb.user_id = uc.user_id
