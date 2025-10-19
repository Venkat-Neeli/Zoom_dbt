{{ config(
    materialized='table',
    cluster_by=['quality_date', 'ticket_id'],
    pre_hook="INSERT INTO {{ ref('audit_log') }} (table_name, operation, timestamp) VALUES ('go_quality_facts', 'transform_start', CURRENT_TIMESTAMP())",
    post_hook="INSERT INTO {{ ref('audit_log') }} (table_name, operation, timestamp) VALUES ('go_quality_facts', 'transform_complete', CURRENT_TIMESTAMP())"
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
),

user_context AS (
    SELECT 
        user_id,
        user_name,
        email,
        company,
        data_quality_score as user_quality_score
    FROM {{ ref('si_users') }}
    WHERE record_status = 'ACTIVE'
        AND data_quality_score >= 0.8
),

user_meeting_quality AS (
    SELECT 
        host_id as user_id,
        AVG(data_quality_score) as avg_meeting_quality,
        COUNT(*) as total_meetings
    FROM {{ ref('si_meetings') }}
    WHERE record_status = 'ACTIVE'
        AND data_quality_score >= 0.8
    GROUP BY host_id
),

final AS (
    SELECT 
        -- Primary Keys
        CONCAT('QF_', sb.ticket_id, '_', sb.user_id) as quality_fact_id,
        sb.ticket_id,
        sb.user_id,
        
        -- Derived Keys for Compatibility
        CONCAT('MEETING_', sb.user_id, '_', sb.open_date::STRING) as meeting_id,
        CONCAT('PARTICIPANT_', sb.user_id, '_', sb.ticket_id) as participant_id,
        CONCAT('DEVICE_', sb.user_id) as device_connection_id,
        
        -- Time Dimensions
        sb.open_date as quality_date,
        EXTRACT(HOUR FROM sb.open_date) as quality_hour,
        sb.open_date as quality_timestamp,
        
        -- Support Ticket Details
        sb.ticket_type,
        sb.resolution_status,
        
        -- Quality Scores (derived from data quality and support patterns)
        CASE 
            WHEN sb.ticket_type ILIKE '%audio%' THEN 
                CASE WHEN sb.resolution_status = 'Resolved' THEN 4.0 ELSE 2.0 END
            ELSE ROUND(uc.user_quality_score * 0.8, 2)
        END as audio_quality_score,
        
        CASE 
            WHEN sb.ticket_type ILIKE '%video%' THEN 
                CASE WHEN sb.resolution_status = 'Resolved' THEN 4.0 ELSE 2.0 END
            ELSE ROUND(uc.user_quality_score * 0.9, 2)
        END as video_quality_score,
        
        ROUND(uc.user_quality_score, 2) as connection_stability_rating,
        
        -- Network Metrics (estimated based on quality patterns)
        CASE 
            WHEN uc.user_quality_score > 8 THEN 50
            WHEN uc.user_quality_score > 6 THEN 100
            ELSE 200
        END as latency_ms,
        
        CASE 
            WHEN uc.user_quality_score > 8 THEN 0.01
            WHEN uc.user_quality_score > 6 THEN 0.05
            ELSE 0.1
        END as packet_loss_rate,
        
        -- System Performance Estimates
        CASE 
            WHEN uc.user_quality_score > 8 THEN 25.0
            WHEN uc.user_quality_score > 6 THEN 50.0
            ELSE 75.0
        END as cpu_usage_percentage,
        
        100 as memory_usage_mb, -- Default estimate
        
        -- User Context
        uc.user_name,
        uc.email,
        COALESCE(uc.company, 'Unknown') as organization,
        
        -- Meeting Quality Context
        COALESCE(umq.avg_meeting_quality, uc.user_quality_score) as user_avg_meeting_quality,
        COALESCE(umq.total_meetings, 0) as user_total_meetings,
        
        -- Overall Quality Assessment
        ROUND(
            (CASE 
                WHEN sb.ticket_type ILIKE '%audio%' THEN 
                    CASE WHEN sb.resolution_status = 'Resolved' THEN 4.0 ELSE 2.0 END
                ELSE ROUND(uc.user_quality_score * 0.8, 2)
            END +
            CASE 
                WHEN sb.ticket_type ILIKE '%video%' THEN 
                    CASE WHEN sb.resolution_status = 'Resolved' THEN 4.0 ELSE 2.0 END
                ELSE ROUND(uc.user_quality_score * 0.9, 2)
            END +
            ROUND(uc.user_quality_score, 2)) / 3.0, 2
        ) as overall_quality_score,
        
        -- Issue Classification
        CASE 
            WHEN sb.ticket_type ILIKE '%audio%' THEN 'AUDIO_ISSUE'
            WHEN sb.ticket_type ILIKE '%video%' THEN 'VIDEO_ISSUE'
            WHEN sb.ticket_type ILIKE '%connection%' THEN 'CONNECTION_ISSUE'
            ELSE 'OTHER_ISSUE'
        END as issue_category,
        
        -- Resolution Performance
        CASE 
            WHEN sb.resolution_status = 'Resolved' THEN 'RESOLVED'
            WHEN sb.resolution_status = 'In Progress' THEN 'IN_PROGRESS'
            ELSE 'OPEN'
        END as resolution_category,
        
        -- Quality Impact Assessment
        CASE 
            WHEN uc.user_quality_score >= 8 AND sb.resolution_status = 'Resolved' THEN 'LOW_IMPACT'
            WHEN uc.user_quality_score >= 6 THEN 'MEDIUM_IMPACT'
            ELSE 'HIGH_IMPACT'
        END as quality_impact_level,
        
        -- Audit Fields
        sb.load_date,
        CURRENT_DATE() as update_date,
        sb.source_system,
        CURRENT_TIMESTAMP() as created_at,
        CURRENT_TIMESTAMP() as updated_at
        
    FROM support_base sb
    LEFT JOIN user_context uc ON sb.user_id = uc.user_id
    LEFT JOIN user_meeting_quality umq ON sb.user_id = umq.user_id
)

SELECT * FROM final
