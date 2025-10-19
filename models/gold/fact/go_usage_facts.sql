{{ config(
    materialized='table',
    cluster_by=['usage_date', 'meeting_id'],
    pre_hook="INSERT INTO {{ ref('audit_log') }} (table_name, operation, timestamp) VALUES ('go_usage_facts', 'transform_start', CURRENT_TIMESTAMP())",
    post_hook="INSERT INTO {{ ref('audit_log') }} (table_name, operation, timestamp) VALUES ('go_usage_facts', 'transform_complete', CURRENT_TIMESTAMP())"
) }}

WITH usage_base AS (
    SELECT 
        usage_id,
        meeting_id,
        feature_name,
        usage_count,
        usage_date,
        load_timestamp,
        update_timestamp,
        source_system,
        load_date,
        update_date,
        data_quality_score,
        record_status
    FROM {{ ref('si_feature_usage') }}
    WHERE record_status = 'ACTIVE'
        AND data_quality_score >= 0.8
),

meeting_context AS (
    SELECT 
        meeting_id,
        host_id,
        meeting_topic,
        start_time,
        duration_minutes
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

final AS (
    SELECT 
        -- Primary Keys
        CONCAT('UF_', ub.usage_id, '_', ub.usage_date::STRING) as usage_fact_id,
        ub.usage_id,
        ub.meeting_id,
        
        -- User and Organization Context
        mc.host_id as user_id,
        COALESCE(uc.company, 'INDIVIDUAL') as organization_id,
        
        -- Time Dimensions
        ub.usage_date,
        EXTRACT(YEAR FROM ub.usage_date) as usage_year,
        EXTRACT(MONTH FROM ub.usage_date) as usage_month,
        EXTRACT(QUARTER FROM ub.usage_date) as usage_quarter,
        EXTRACT(DOW FROM ub.usage_date) as day_of_week,
        EXTRACT(HOUR FROM mc.start_time) as usage_hour,
        
        -- Feature Details
        ub.feature_name,
        ub.usage_count,
        
        -- Feature Categorization
        CASE 
            WHEN ub.feature_name IN ('Screen Sharing', 'Breakout Rooms', 'Whiteboard') THEN 'COLLABORATION'
            WHEN ub.feature_name IN ('Chat', 'Reactions', 'Hand Raise') THEN 'ENGAGEMENT'
            WHEN ub.feature_name IN ('Recording', 'Transcription') THEN 'CONTENT'
            WHEN ub.feature_name IN ('Q&A', 'Polling') THEN 'INTERACTION'
            ELSE 'OTHER'
        END as feature_category,
        
        -- Usage Intensity Classification
        CASE 
            WHEN ub.usage_count >= 50 THEN 'HIGH_USAGE'
            WHEN ub.usage_count >= 10 THEN 'MEDIUM_USAGE'
            WHEN ub.usage_count > 0 THEN 'LOW_USAGE'
            ELSE 'NO_USAGE'
        END as usage_intensity,
        
        -- Meeting Context
        mc.meeting_topic,
        mc.duration_minutes as meeting_duration,
        
        -- User Context
        uc.user_name,
        uc.email,
        
        -- Calculated Metrics
        CASE 
            WHEN mc.duration_minutes > 0 THEN 
                ROUND(ub.usage_count::FLOAT / (mc.duration_minutes / 60.0), 2)
            ELSE 0 
        END as usage_per_hour,
        
        -- Meeting Classification by Usage
        CASE 
            WHEN ub.usage_count >= 20 THEN 'HIGHLY_INTERACTIVE'
            WHEN ub.usage_count >= 5 THEN 'MODERATELY_INTERACTIVE'
            WHEN ub.usage_count > 0 THEN 'LIGHTLY_INTERACTIVE'
            ELSE 'NON_INTERACTIVE'
        END as meeting_interaction_level,
        
        -- Audit Fields
        ub.load_date,
        CURRENT_DATE() as update_date,
        ub.source_system,
        CURRENT_TIMESTAMP() as created_at,
        CURRENT_TIMESTAMP() as updated_at
        
    FROM usage_base ub
    INNER JOIN meeting_context mc ON ub.meeting_id = mc.meeting_id
    LEFT JOIN user_context uc ON mc.host_id = uc.user_id
)

SELECT * FROM final
