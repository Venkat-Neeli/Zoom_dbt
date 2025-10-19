{{ config(
    materialized='table',
    cluster_by=['load_date'],
    pre_hook="INSERT INTO {{ ref('audit_log') }} (table_name, operation, start_time, status) VALUES ('go_usage_facts', 'transform_start', CURRENT_TIMESTAMP(), 'RUNNING')",
    post_hook="INSERT INTO {{ ref('audit_log') }} (table_name, operation, end_time, status) VALUES ('go_usage_facts', 'transform_end', CURRENT_TIMESTAMP(), 'SUCCESS')"
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
        AND usage_id IS NOT NULL
        AND meeting_id IS NOT NULL
        AND feature_name IS NOT NULL
),

meeting_context AS (
    SELECT 
        meeting_id,
        host_id,
        meeting_topic,
        start_time,
        end_time,
        duration_minutes
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
    ub.usage_id,
    ub.meeting_id,
    
    -- Usage Details
    ub.feature_name,
    ub.usage_count,
    ub.usage_date,
    
    -- Meeting Context
    mc.host_id,
    mc.meeting_topic,
    mc.start_time as meeting_start_time,
    mc.end_time as meeting_end_time,
    mc.duration_minutes as meeting_duration_minutes,
    
    -- Host Context
    uc.user_name as host_name,
    uc.email as host_email,
    uc.company as host_company,
    uc.plan_type as host_plan_type,
    
    -- Feature Classifications
    CASE 
        WHEN ub.feature_name IN ('SCREEN_SHARE', 'WHITEBOARD', 'ANNOTATION') THEN 'COLLABORATION'
        WHEN ub.feature_name IN ('CHAT', 'REACTIONS', 'POLLS') THEN 'ENGAGEMENT'
        WHEN ub.feature_name IN ('RECORDING', 'TRANSCRIPT', 'CLOUD_STORAGE') THEN 'CONTENT_MANAGEMENT'
        WHEN ub.feature_name IN ('BREAKOUT_ROOMS', 'WAITING_ROOM', 'SECURITY') THEN 'MEETING_MANAGEMENT'
        ELSE 'OTHER'
    END as feature_category,
    
    -- Usage Intensity
    CASE 
        WHEN ub.usage_count = 1 THEN 'SINGLE_USE'
        WHEN ub.usage_count <= 5 THEN 'LOW_USAGE'
        WHEN ub.usage_count <= 15 THEN 'MODERATE_USAGE'
        WHEN ub.usage_count <= 30 THEN 'HIGH_USAGE'
        ELSE 'INTENSIVE_USAGE'
    END as usage_intensity,
    
    -- Calculated Metrics
    CASE 
        WHEN mc.duration_minutes > 0 THEN 
            ROUND(ub.usage_count / (mc.duration_minutes / 60.0), 2)
        ELSE 0
    END as usage_per_hour,
    
    -- Time Dimensions
    DATE(ub.usage_date) as usage_date_only,
    EXTRACT(HOUR FROM ub.usage_date) as usage_hour,
    DAYOFWEEK(ub.usage_date) as usage_day_of_week,
    EXTRACT(MONTH FROM ub.usage_date) as usage_month,
    EXTRACT(YEAR FROM ub.usage_date) as usage_year,
    
    -- Quality and Audit Fields
    ub.data_quality_score,
    ub.source_system,
    ub.load_date,
    ub.update_date,
    CURRENT_TIMESTAMP() as created_at,
    CURRENT_TIMESTAMP() as updated_at
    
FROM usage_base ub
LEFT JOIN meeting_context mc ON ub.meeting_id = mc.meeting_id
LEFT JOIN user_context uc ON mc.host_id = uc.user_id
