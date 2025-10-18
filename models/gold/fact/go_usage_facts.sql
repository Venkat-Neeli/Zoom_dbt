{{ config(
    materialized='table',
    cluster_by=['usage_date', 'user_id'],
    pre_hook="INSERT INTO {{ ref('go_process_audit') }} (process_name, table_name, start_time, status) VALUES ('go_usage_facts_transform', 'go_usage_facts', CURRENT_TIMESTAMP(), 'STARTED') WHERE '{{ this.name }}' != 'go_process_audit'",
    post_hook="INSERT INTO {{ ref('go_process_audit') }} (process_name, table_name, end_time, status, records_processed) VALUES ('go_usage_facts_transform', 'go_usage_facts', CURRENT_TIMESTAMP(), 'COMPLETED', (SELECT COUNT(*) FROM {{ this }})) WHERE '{{ this.name }}' != 'go_process_audit'"
) }}

WITH feature_usage_base AS (
    SELECT 
        usage_id,
        meeting_id,
        feature_name,
        usage_count,
        usage_date,
        user_id,
        session_duration_minutes,
        created_at AS usage_created_at,
        updated_at AS usage_updated_at
    FROM {{ ref('si_feature_usage') }}
    WHERE usage_id IS NOT NULL
),

user_context AS (
    SELECT 
        user_id,
        user_name,
        email,
        company,
        plan_type
    FROM {{ ref('si_users') }}
    WHERE user_id IS NOT NULL
),

meeting_context AS (
    SELECT 
        meeting_id,
        host_id,
        meeting_type,
        duration_minutes AS meeting_duration_minutes
    FROM {{ ref('si_meetings') }}
    WHERE meeting_id IS NOT NULL
),

user_daily_usage AS (
    SELECT 
        user_id,
        usage_date,
        COUNT(DISTINCT feature_name) AS daily_features_used,
        SUM(usage_count) AS daily_total_usage,
        COUNT(DISTINCT meeting_id) AS daily_meetings_count,
        SUM(session_duration_minutes) AS daily_session_duration
    FROM feature_usage_base
    GROUP BY user_id, usage_date
),

feature_popularity AS (
    SELECT 
        feature_name,
        COUNT(DISTINCT user_id) AS feature_user_count,
        SUM(usage_count) AS feature_total_usage,
        AVG(usage_count) AS feature_avg_usage
    FROM feature_usage_base
    GROUP BY feature_name
)

SELECT 
    {{ dbt_utils.generate_surrogate_key(['fub.usage_id']) }} AS usage_fact_key,
    fub.usage_id,
    fub.meeting_id,
    fub.user_id,
    mc.host_id,
    uc.user_name,
    uc.email,
    uc.company,
    uc.plan_type,
    fub.feature_name,
    fub.usage_count,
    DATE(fub.usage_date) AS usage_date,
    fub.usage_date AS usage_timestamp,
    fub.session_duration_minutes,
    mc.meeting_type,
    mc.meeting_duration_minutes,
    udu.daily_features_used,
    udu.daily_total_usage,
    udu.daily_meetings_count,
    udu.daily_session_duration AS daily_total_session_duration,
    fp.feature_user_count,
    fp.feature_total_usage,
    fp.feature_avg_usage,
    ROUND(
        (fub.usage_count * 100.0) / NULLIF(udu.daily_total_usage, 0), 2
    ) AS usage_percentage_of_daily_total,
    ROUND(
        (fub.session_duration_minutes * 100.0) / NULLIF(mc.meeting_duration_minutes, 0), 2
    ) AS session_percentage_of_meeting,
    CASE 
        WHEN fub.usage_count >= 10 THEN 'Heavy'
        WHEN fub.usage_count >= 5 THEN 'Moderate'
        WHEN fub.usage_count >= 1 THEN 'Light'
        ELSE 'None'
    END AS usage_intensity,
    CASE 
        WHEN fp.feature_user_count >= 1000 THEN 'Very Popular'
        WHEN fp.feature_user_count >= 500 THEN 'Popular'
        WHEN fp.feature_user_count >= 100 THEN 'Moderate'
        ELSE 'Niche'
    END AS feature_popularity_category,
    CASE 
        WHEN fub.user_id = mc.host_id THEN 'Host'
        ELSE 'Participant'
    END AS user_role_in_meeting,
    CURRENT_TIMESTAMP() AS created_at,
    CURRENT_TIMESTAMP() AS updated_at,
    'SUCCESS' AS process_status
FROM feature_usage_base fub
LEFT JOIN user_context uc ON fub.user_id = uc.user_id
LEFT JOIN meeting_context mc ON fub.meeting_id = mc.meeting_id
LEFT JOIN user_daily_usage udu ON fub.user_id = udu.user_id AND DATE(fub.usage_date) = udu.usage_date
LEFT JOIN feature_popularity fp ON fub.feature_name = fp.feature_name
