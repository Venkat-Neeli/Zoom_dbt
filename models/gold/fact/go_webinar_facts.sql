{{ config(
    materialized='table',
    cluster_by=['webinar_date', 'host_id'],
    pre_hook="INSERT INTO {{ ref('go_process_audit') }} (process_name, table_name, start_time, status) VALUES ('go_webinar_facts_transform', 'go_webinar_facts', CURRENT_TIMESTAMP(), 'STARTED') WHERE '{{ this.name }}' != 'go_process_audit'",
    post_hook="INSERT INTO {{ ref('go_process_audit') }} (process_name, table_name, end_time, status, records_processed) VALUES ('go_webinar_facts_transform', 'go_webinar_facts', CURRENT_TIMESTAMP(), 'COMPLETED', (SELECT COUNT(*) FROM {{ this }})) WHERE '{{ this.name }}' != 'go_process_audit'"
) }}

WITH webinar_base AS (
    SELECT 
        webinar_id,
        host_id,
        webinar_topic,
        start_time,
        end_time,
        duration_minutes,
        registrants,
        actual_attendees,
        webinar_type,
        created_at AS webinar_created_at,
        updated_at AS webinar_updated_at
    FROM {{ ref('si_webinars') }}
    WHERE webinar_id IS NOT NULL
),

host_info AS (
    SELECT 
        user_id,
        user_name,
        company,
        plan_type
    FROM {{ ref('si_users') }}
    WHERE user_id IS NOT NULL
),

webinar_engagement AS (
    SELECT 
        w.webinar_id,
        COUNT(DISTINCT p.participant_id) AS unique_participants,
        AVG(DATEDIFF('minute', p.join_time, p.leave_time)) AS avg_attendance_duration,
        COUNT(CASE WHEN DATEDIFF('minute', p.join_time, p.leave_time) >= 30 THEN 1 END) AS engaged_participants
    FROM webinar_base w
    LEFT JOIN {{ ref('si_participants') }} p ON w.webinar_id::STRING = p.meeting_id::STRING
    GROUP BY w.webinar_id
)

SELECT 
    {{ dbt_utils.generate_surrogate_key(['wb.webinar_id']) }} AS webinar_fact_key,
    wb.webinar_id,
    wb.host_id,
    hi.user_name AS host_name,
    hi.company AS host_company,
    hi.plan_type AS host_plan_type,
    wb.webinar_topic,
    DATE(wb.start_time) AS webinar_date,
    wb.start_time,
    wb.end_time,
    wb.duration_minutes,
    wb.webinar_type,
    wb.registrants,
    wb.actual_attendees,
    COALESCE(we.unique_participants, 0) AS unique_participants,
    ROUND(
        (wb.actual_attendees * 100.0) / NULLIF(wb.registrants, 0), 2
    ) AS attendance_rate_percentage,
    ROUND(
        (COALESCE(we.engaged_participants, 0) * 100.0) / NULLIF(wb.actual_attendees, 0), 2
    ) AS engagement_rate_percentage,
    COALESCE(we.avg_attendance_duration, 0) AS avg_attendance_duration_minutes,
    wb.registrants - wb.actual_attendees AS no_shows,
    CASE 
        WHEN wb.duration_minutes >= 90 THEN 'Long'
        WHEN wb.duration_minutes >= 45 THEN 'Medium'
        ELSE 'Short'
    END AS webinar_duration_category,
    CASE 
        WHEN wb.actual_attendees >= 100 THEN 'Large'
        WHEN wb.actual_attendees >= 50 THEN 'Medium'
        ELSE 'Small'
    END AS webinar_size_category,
    CASE 
        WHEN ROUND((wb.actual_attendees * 100.0) / NULLIF(wb.registrants, 0), 2) >= 80 THEN 'High'
        WHEN ROUND((wb.actual_attendees * 100.0) / NULLIF(wb.registrants, 0), 2) >= 60 THEN 'Medium'
        ELSE 'Low'
    END AS attendance_performance,
    CURRENT_TIMESTAMP() AS created_at,
    CURRENT_TIMESTAMP() AS updated_at,
    'SUCCESS' AS process_status
FROM webinar_base wb
LEFT JOIN host_info hi ON wb.host_id = hi.user_id
LEFT JOIN webinar_engagement we ON wb.webinar_id = we.webinar_id
