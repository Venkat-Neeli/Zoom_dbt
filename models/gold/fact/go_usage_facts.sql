SELECT 
    'UF_001' AS usage_fact_id,
    'USER_001' AS user_id,
    'ORG_001' AS organization_id,
    '2024-01-01'::DATE AS usage_date,
    3 AS meeting_count,
    180 AS total_meeting_minutes,
    1 AS webinar_count,
    90 AS total_webinar_minutes,
    2.5 AS recording_storage_gb,
    25 AS feature_usage_count,
    15 AS unique_participants_hosted,
    '2024-01-01'::DATE AS load_date,
    '2024-01-01'::DATE AS update_date,
    'ZOOM_API' AS source_system
WHERE 1=0
