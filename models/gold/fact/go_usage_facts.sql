{{ config(
    materialized='table',
    cluster_by=['usage_date', 'organization_id'],
    tags=['fact', 'gold'],
    pre_hook="INSERT INTO {{ ref('go_process_audit') }} (execution_id, pipeline_name, process_type, start_time, status, source_system, target_system, user_executed, server_name, load_date, update_date) SELECT '{{ dbt_utils.generate_surrogate_key(['invocation_id', 'this.name']) }}', 'GO_USAGE_FACTS', 'FACT_BUILD', CURRENT_TIMESTAMP(), 'STARTED', 'SILVER', 'GOLD', CURRENT_USER(), 'DBT_CLOUD', CURRENT_DATE(), CURRENT_DATE() WHERE '{{ this.name }}' != 'go_process_audit'",
    post_hook="UPDATE {{ ref('go_process_audit') }} SET end_time = CURRENT_TIMESTAMP(), status = 'COMPLETED', records_processed = (SELECT COUNT(*) FROM {{ this }}), records_successful = (SELECT COUNT(*) FROM {{ this }}), processing_duration_seconds = DATEDIFF('second', start_time, CURRENT_TIMESTAMP()) WHERE execution_id = '{{ dbt_utils.generate_surrogate_key(['invocation_id', 'this.name']) }}' AND '{{ this.name }}' != 'go_process_audit'"
) }}

-- Gold Usage Facts Table
-- Daily user activity and platform utilization metrics

WITH user_organizations AS (
    SELECT
        user_id,
        COALESCE(company, 'INDIVIDUAL') AS organization_id
    FROM {{ source('silver', 'si_users') }}
    WHERE record_status = 'ACTIVE'
),

daily_meeting_usage AS (
    SELECT
        host_id AS user_id,
        DATE(start_time) AS usage_date,
        COUNT(DISTINCT meeting_id) AS meeting_count,
        SUM(duration_minutes) AS total_meeting_minutes
    FROM {{ source('silver', 'si_meetings') }}
    WHERE record_status = 'ACTIVE'
        AND start_time IS NOT NULL
    GROUP BY host_id, DATE(start_time)
),

daily_webinar_usage AS (
    SELECT
        host_id AS user_id,
        DATE(start_time) AS usage_date,
        COUNT(DISTINCT webinar_id) AS webinar_count,
        SUM(DATEDIFF('minute', start_time, COALESCE(end_time, CURRENT_TIMESTAMP()))) AS total_webinar_minutes
    FROM {{ source('silver', 'si_webinars') }}
    WHERE record_status = 'ACTIVE'
        AND start_time IS NOT NULL
    GROUP BY host_id, DATE(start_time)
),

daily_feature_usage AS (
    SELECT
        fu.usage_date,
        m.host_id AS user_id,
        SUM(fu.usage_count) AS feature_usage_count,
        SUM(CASE WHEN UPPER(fu.feature_name) LIKE '%RECORDING%' THEN fu.usage_count * 0.1 ELSE 0 END) AS recording_storage_gb
    FROM {{ source('silver', 'si_feature_usage') }} fu
    INNER JOIN {{ source('silver', 'si_meetings') }} m ON fu.meeting_id = m.meeting_id
    WHERE fu.record_status = 'ACTIVE'
        AND m.record_status = 'ACTIVE'
    GROUP BY fu.usage_date, m.host_id
),

unique_participants_hosted AS (
    SELECT
        m.host_id AS user_id,
        DATE(m.start_time) AS usage_date,
        COUNT(DISTINCT p.user_id) AS unique_participants_hosted
    FROM {{ source('silver', 'si_meetings') }} m
    INNER JOIN {{ source('silver', 'si_participants') }} p ON m.meeting_id = p.meeting_id
    WHERE m.record_status = 'ACTIVE'
        AND p.record_status = 'ACTIVE'
        AND p.user_id != m.host_id
    GROUP BY m.host_id, DATE(m.start_time)
),

all_usage_dates AS (
    SELECT user_id, usage_date FROM daily_meeting_usage
    UNION
    SELECT user_id, usage_date FROM daily_webinar_usage
    UNION
    SELECT user_id, usage_date FROM daily_feature_usage
),

usage_facts AS (
    SELECT
        CONCAT('UF_', aud.user_id, '_', aud.usage_date::STRING) AS usage_fact_id,
        aud.user_id,
        COALESCE(uo.organization_id, 'INDIVIDUAL') AS organization_id,
        aud.usage_date,
        COALESCE(dmu.meeting_count, 0) AS meeting_count,
        COALESCE(dmu.total_meeting_minutes, 0) AS total_meeting_minutes,
        COALESCE(dwu.webinar_count, 0) AS webinar_count,
        COALESCE(dwu.total_webinar_minutes, 0) AS total_webinar_minutes,
        COALESCE(dfu.recording_storage_gb, 0) AS recording_storage_gb,
        COALESCE(dfu.feature_usage_count, 0) AS feature_usage_count,
        COALESCE(uph.unique_participants_hosted, 0) AS unique_participants_hosted,
        CURRENT_DATE() AS load_date,
        CURRENT_DATE() AS update_date,
        'SILVER' AS source_system
    FROM all_usage_dates aud
    LEFT JOIN user_organizations uo ON aud.user_id = uo.user_id
    LEFT JOIN daily_meeting_usage dmu ON aud.user_id = dmu.user_id AND aud.usage_date = dmu.usage_date
    LEFT JOIN daily_webinar_usage dwu ON aud.user_id = dwu.user_id AND aud.usage_date = dwu.usage_date
    LEFT JOIN daily_feature_usage dfu ON aud.user_id = dfu.user_id AND aud.usage_date = dfu.usage_date
    LEFT JOIN unique_participants_hosted uph ON aud.user_id = uph.user_id AND aud.usage_date = uph.usage_date
)

SELECT
    usage_fact_id::VARCHAR(50) AS usage_fact_id,
    user_id::VARCHAR(50) AS user_id,
    organization_id::VARCHAR(50) AS organization_id,
    usage_date,
    meeting_count,
    total_meeting_minutes,
    webinar_count,
    total_webinar_minutes,
    recording_storage_gb,
    feature_usage_count,
    unique_participants_hosted,
    load_date,
    update_date,
    source_system::VARCHAR(100) AS source_system
FROM usage_facts
