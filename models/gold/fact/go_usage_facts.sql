{{ config(
    materialized='table'
) }}

WITH user_organizations AS (
    SELECT 
        user_id,
        COALESCE(company, 'INDIVIDUAL') as organization_id
    FROM {{ source('silver', 'si_users') }}
    WHERE record_status = 'ACTIVE'
),

user_meeting_activity AS (
    SELECT 
        host_id as user_id,
        DATE(start_time) as usage_date,
        COUNT(DISTINCT meeting_id) as meeting_count,
        SUM(duration_minutes) as total_meeting_minutes
    FROM {{ source('silver', 'si_meetings') }}
    WHERE record_status = 'ACTIVE'
    AND start_time IS NOT NULL
    GROUP BY host_id, DATE(start_time)
),

user_webinar_activity AS (
    SELECT 
        host_id as user_id,
        DATE(start_time) as usage_date,
        COUNT(DISTINCT webinar_id) as webinar_count,
        SUM(DATEDIFF('minute', start_time, end_time)) as total_webinar_minutes
    FROM {{ source('silver', 'si_webinars') }}
    WHERE record_status = 'ACTIVE'
    AND start_time IS NOT NULL
    AND end_time IS NOT NULL
    GROUP BY host_id, DATE(start_time)
),

user_feature_activity AS (
    SELECT 
        usage_date,
        SUM(usage_count) as feature_usage_count,
        SUM(CASE WHEN feature_name = 'Recording' THEN usage_count * 0.1 ELSE 0 END) as recording_storage_gb
    FROM {{ source('silver', 'si_feature_usage') }}
    WHERE record_status = 'ACTIVE'
    GROUP BY usage_date
),

user_participant_interactions AS (
    SELECT 
        m.host_id as user_id,
        DATE(p.join_time) as usage_date,
        COUNT(DISTINCT p.user_id) as unique_participants_hosted
    FROM {{ source('silver', 'si_meetings') }} m
    JOIN {{ source('silver', 'si_participants') }} p ON m.meeting_id = p.meeting_id
    WHERE m.record_status = 'ACTIVE'
    AND p.record_status = 'ACTIVE'
    GROUP BY m.host_id, DATE(p.join_time)
),

final_usage_facts AS (
    SELECT 
        CONCAT('UF_', COALESCE(uma.user_id, uwa.user_id), '_', COALESCE(uma.usage_date, uwa.usage_date)::STRING) as usage_fact_id,
        COALESCE(uma.user_id, uwa.user_id) as user_id,
        COALESCE(uo.organization_id, 'INDIVIDUAL') as organization_id,
        COALESCE(uma.usage_date, uwa.usage_date) as usage_date,
        COALESCE(uma.meeting_count, 0) as meeting_count,
        COALESCE(uma.total_meeting_minutes, 0) as total_meeting_minutes,
        COALESCE(uwa.webinar_count, 0) as webinar_count,
        COALESCE(uwa.total_webinar_minutes, 0) as total_webinar_minutes,
        COALESCE(ufa.recording_storage_gb, 0) as recording_storage_gb,
        COALESCE(ufa.feature_usage_count, 0) as feature_usage_count,
        COALESCE(upi.unique_participants_hosted, 0) as unique_participants_hosted,
        CURRENT_DATE() as load_date,
        CURRENT_DATE() as update_date,
        'Silver' as source_system
    FROM user_meeting_activity uma
    FULL OUTER JOIN user_webinar_activity uwa ON uma.user_id = uwa.user_id AND uma.usage_date = uwa.usage_date
    LEFT JOIN user_organizations uo ON COALESCE(uma.user_id, uwa.user_id) = uo.user_id
    LEFT JOIN user_feature_activity ufa ON COALESCE(uma.usage_date, uwa.usage_date) = ufa.usage_date
    LEFT JOIN user_participant_interactions upi ON COALESCE(uma.user_id, uwa.user_id) = upi.user_id AND COALESCE(uma.usage_date, uwa.usage_date) = upi.usage_date
    WHERE COALESCE(uma.user_id, uwa.user_id) IS NOT NULL
)

SELECT * FROM final_usage_facts
