{{ config(
    materialized='table',
    cluster_by=['load_date'],
    pre_hook="INSERT INTO {{ ref('audit_log') }} (table_name, operation, start_time, status) VALUES ('go_quality_facts', 'transform_start', CURRENT_TIMESTAMP(), 'RUNNING')",
    post_hook="INSERT INTO {{ ref('audit_log') }} (table_name, operation, end_time, status) VALUES ('go_quality_facts', 'transform_end', CURRENT_TIMESTAMP(), 'SUCCESS')"
) }}

WITH support_base AS (
    SELECT 
        ticket_id,
        user_id,
        ticket_type,
        resolution_status,
        open_date,
        load_timestamp,
        update_timestamp,
        source_system,
        load_date,
        update_date,
        data_quality_score,
        record_status
    FROM {{ ref('si_support_tickets') }}
    WHERE record_status = 'ACTIVE'
        AND data_quality_score >= 0.8
        AND ticket_id IS NOT NULL
        AND user_id IS NOT NULL
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
),

user_meeting_quality AS (
    SELECT 
        host_id as user_id,
        AVG(data_quality_score) as avg_meeting_quality,
        COUNT(*) as total_meetings,
        SUM(CASE WHEN data_quality_score >= 0.9 THEN 1 ELSE 0 END) as high_quality_meetings
    FROM {{ ref('si_meetings') }}
    WHERE record_status = 'ACTIVE'
    GROUP BY host_id
),

user_participation_quality AS (
    SELECT 
        user_id,
        AVG(data_quality_score) as avg_participation_quality,
        COUNT(*) as total_participations,
        SUM(CASE WHEN data_quality_score >= 0.9 THEN 1 ELSE 0 END) as high_quality_participations
    FROM {{ ref('si_participants') }}
    WHERE record_status = 'ACTIVE'
    GROUP BY user_id
)

SELECT 
    -- Primary Keys
    sb.ticket_id,
    sb.user_id,
    
    -- Support Ticket Details
    sb.ticket_type,
    sb.resolution_status,
    sb.open_date,
    
    -- User Context
    uc.user_name,
    uc.email,
    uc.company,
    uc.plan_type,
    
    -- Meeting Quality Metrics
    COALESCE(umq.avg_meeting_quality, 0) as avg_meeting_quality_score,
    COALESCE(umq.total_meetings, 0) as total_meetings_hosted,
    COALESCE(umq.high_quality_meetings, 0) as high_quality_meetings_hosted,
    
    -- Participation Quality Metrics
    COALESCE(upq.avg_participation_quality, 0) as avg_participation_quality_score,
    COALESCE(upq.total_participations, 0) as total_meeting_participations,
    COALESCE(upq.high_quality_participations, 0) as high_quality_participations,
    
    -- Quality Classifications
    CASE 
        WHEN sb.ticket_type IN ('AUDIO_ISSUE', 'VIDEO_ISSUE', 'CONNECTION_ISSUE') THEN 'TECHNICAL_QUALITY'
        WHEN sb.ticket_type IN ('FEATURE_REQUEST', 'HOW_TO', 'TRAINING') THEN 'USER_EXPERIENCE'
        WHEN sb.ticket_type IN ('BILLING', 'ACCOUNT', 'LICENSE') THEN 'ACCOUNT_MANAGEMENT'
        ELSE 'OTHER'
    END as issue_category,
    
    CASE 
        WHEN sb.resolution_status = 'RESOLVED' THEN 'RESOLVED'
        WHEN sb.resolution_status = 'CLOSED' THEN 'CLOSED'
        WHEN sb.resolution_status IN ('OPEN', 'IN_PROGRESS') THEN 'ACTIVE'
        ELSE 'OTHER'
    END as resolution_category,
    
    -- Calculated Quality Metrics
    CASE 
        WHEN umq.total_meetings > 0 THEN 
            ROUND((umq.high_quality_meetings / umq.total_meetings) * 100, 2)
        ELSE 0
    END as meeting_quality_percentage,
    
    CASE 
        WHEN upq.total_participations > 0 THEN 
            ROUND((upq.high_quality_participations / upq.total_participations) * 100, 2)
        ELSE 0
    END as participation_quality_percentage,
    
    -- Overall Quality Score
    ROUND((
        COALESCE(umq.avg_meeting_quality, 0) * 0.4 + 
        COALESCE(upq.avg_participation_quality, 0) * 0.3 + 
        CASE WHEN sb.resolution_status IN ('RESOLVED', 'CLOSED') THEN 1.0 ELSE 0.5 END * 0.3
    ), 3) as overall_quality_score,
    
    -- Time Dimensions
    DATE(sb.open_date) as support_date,
    EXTRACT(MONTH FROM sb.open_date) as support_month,
    EXTRACT(YEAR FROM sb.open_date) as support_year,
    DAYOFWEEK(sb.open_date) as support_day_of_week,
    
    -- Quality and Audit Fields
    sb.data_quality_score,
    sb.source_system,
    sb.load_date,
    sb.update_date,
    CURRENT_TIMESTAMP() as created_at,
    CURRENT_TIMESTAMP() as updated_at
    
FROM support_base sb
LEFT JOIN user_context uc ON sb.user_id = uc.user_id
LEFT JOIN user_meeting_quality umq ON sb.user_id = umq.user_id
LEFT JOIN user_participation_quality upq ON sb.user_id = upq.user_id
