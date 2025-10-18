{{ config(
    materialized='table',
    cluster_by=['participation_date', 'user_id'],
    pre_hook="INSERT INTO {{ ref('go_process_audit') }} (process_name, table_name, start_time, status) VALUES ('go_participant_facts_transform', 'go_participant_facts', CURRENT_TIMESTAMP(), 'STARTED') WHERE '{{ this.name }}' != 'go_process_audit'",
    post_hook="INSERT INTO {{ ref('go_process_audit') }} (process_name, table_name, end_time, status, records_processed) VALUES ('go_participant_facts_transform', 'go_participant_facts', CURRENT_TIMESTAMP(), 'COMPLETED', (SELECT COUNT(*) FROM {{ this }})) WHERE '{{ this.name }}' != 'go_process_audit'"
) }}

WITH participant_base AS (
    SELECT 
        participant_id,
        meeting_id,
        user_id,
        join_time,
        leave_time,
        participant_name,
        participant_email,
        created_at AS participant_created_at,
        updated_at AS participant_updated_at
    FROM {{ ref('si_participants') }}
    WHERE participant_id IS NOT NULL
),

meeting_context AS (
    SELECT 
        meeting_id,
        host_id,
        meeting_topic,
        start_time AS meeting_start_time,
        end_time AS meeting_end_time,
        duration_minutes AS meeting_duration_minutes,
        meeting_type
    FROM {{ ref('si_meetings') }}
    WHERE meeting_id IS NOT NULL
),

user_context AS (
    SELECT 
        user_id,
        user_name,
        company,
        plan_type
    FROM {{ ref('si_users') }}
    WHERE user_id IS NOT NULL
),

participant_feature_usage AS (
    SELECT 
        p.participant_id,
        p.meeting_id,
        COUNT(DISTINCT fu.feature_name) AS features_used,
        SUM(fu.usage_count) AS total_feature_interactions
    FROM participant_base p
    LEFT JOIN {{ ref('si_feature_usage') }} fu ON p.meeting_id = fu.meeting_id
    GROUP BY p.participant_id, p.meeting_id
)

SELECT 
    {{ dbt_utils.generate_surrogate_key(['pb.participant_id', 'pb.meeting_id']) }} AS participant_fact_key,
    pb.participant_id,
    pb.meeting_id,
    pb.user_id,
    mc.host_id,
    uc.company AS participant_company,
    uc.plan_type AS participant_plan_type,
    pb.participant_name,
    pb.participant_email,
    DATE(pb.join_time) AS participation_date,
    pb.join_time,
    pb.leave_time,
    mc.meeting_start_time,
    mc.meeting_end_time,
    mc.meeting_topic,
    mc.meeting_type,
    DATEDIFF('minute', pb.join_time, pb.leave_time) AS participation_duration_minutes,
    DATEDIFF('minute', mc.meeting_start_time, pb.join_time) AS join_delay_minutes,
    DATEDIFF('minute', pb.leave_time, mc.meeting_end_time) AS early_leave_minutes,
    CASE 
        WHEN pb.join_time <= mc.meeting_start_time THEN 1 
        ELSE 0 
    END AS joined_on_time_flag,
    CASE 
        WHEN pb.leave_time >= mc.meeting_end_time THEN 1 
        ELSE 0 
    END AS stayed_until_end_flag,
    ROUND(
        (DATEDIFF('minute', pb.join_time, pb.leave_time) * 100.0) / 
        NULLIF(mc.meeting_duration_minutes, 0), 2
    ) AS participation_percentage,
    COALESCE(pfu.features_used, 0) AS features_used_count,
    COALESCE(pfu.total_feature_interactions, 0) AS total_feature_interactions,
    CASE 
        WHEN pb.user_id = mc.host_id THEN 'Host'
        ELSE 'Participant'
    END AS participant_role,
    CASE 
        WHEN DATEDIFF('minute', pb.join_time, pb.leave_time) >= 30 THEN 'High'
        WHEN DATEDIFF('minute', pb.join_time, pb.leave_time) >= 15 THEN 'Medium'
        ELSE 'Low'
    END AS engagement_level,
    CURRENT_TIMESTAMP() AS created_at,
    CURRENT_TIMESTAMP() AS updated_at,
    'SUCCESS' AS process_status
FROM participant_base pb
LEFT JOIN meeting_context mc ON pb.meeting_id = mc.meeting_id
LEFT JOIN user_context uc ON pb.user_id = uc.user_id
LEFT JOIN participant_feature_usage pfu ON pb.participant_id = pfu.participant_id AND pb.meeting_id = pfu.meeting_id
