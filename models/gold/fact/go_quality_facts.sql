{{ config(
    materialized='incremental',
    unique_key='quality_fact_key',
    on_schema_change='sync_all_columns',
    pre_hook="INSERT INTO {{ ref('go_process_audit') }} (process_name, start_time, status) VALUES ('go_quality_facts', CURRENT_TIMESTAMP(), 'STARTED')",
    post_hook="INSERT INTO {{ ref('go_process_audit') }} (process_name, end_time, status) VALUES ('go_quality_facts', CURRENT_TIMESTAMP(), 'COMPLETED')"
) }}

WITH quality_metrics AS (
    SELECT 
        'si_users' AS table_name,
        COUNT(*) AS total_records,
        COUNT(CASE WHEN record_status = 'ACTIVE' THEN 1 END) AS active_records,
        AVG(data_quality_score) AS avg_quality_score,
        COUNT(CASE WHEN data_quality_score >= 0.7 THEN 1 END) AS high_quality_records,
        MAX(load_date) AS latest_load_date,
        MAX(update_date) AS latest_update_date,
        'USERS' AS source_system
    FROM {{ ref('si_users') }}
    
    UNION ALL
    
    SELECT 
        'si_meetings' AS table_name,
        COUNT(*) AS total_records,
        COUNT(CASE WHEN record_status = 'ACTIVE' THEN 1 END) AS active_records,
        AVG(data_quality_score) AS avg_quality_score,
        COUNT(CASE WHEN data_quality_score >= 0.7 THEN 1 END) AS high_quality_records,
        MAX(load_date) AS latest_load_date,
        MAX(update_date) AS latest_update_date,
        source_system
    FROM {{ ref('si_meetings') }}
    GROUP BY source_system
    
    UNION ALL
    
    SELECT 
        'si_participants' AS table_name,
        COUNT(*) AS total_records,
        COUNT(CASE WHEN record_status = 'ACTIVE' THEN 1 END) AS active_records,
        AVG(data_quality_score) AS avg_quality_score,
        COUNT(CASE WHEN data_quality_score >= 0.7 THEN 1 END) AS high_quality_records,
        MAX(load_date) AS latest_load_date,
        MAX(update_date) AS latest_update_date,
        source_system
    FROM {{ ref('si_participants') }}
    GROUP BY source_system
    
    UNION ALL
    
    SELECT 
        'si_feature_usage' AS table_name,
        COUNT(*) AS total_records,
        COUNT(CASE WHEN record_status = 'ACTIVE' THEN 1 END) AS active_records,
        AVG(data_quality_score) AS avg_quality_score,
        COUNT(CASE WHEN data_quality_score >= 0.7 THEN 1 END) AS high_quality_records,
        MAX(load_date) AS latest_load_date,
        MAX(update_date) AS latest_update_date,
        source_system
    FROM {{ ref('si_feature_usage') }}
    GROUP BY source_system
    
    UNION ALL
    
    SELECT 
        'si_webinars' AS table_name,
        COUNT(*) AS total_records,
        COUNT(CASE WHEN record_status = 'ACTIVE' THEN 1 END) AS active_records,
        AVG(data_quality_score) AS avg_quality_score,
        COUNT(CASE WHEN data_quality_score >= 0.7 THEN 1 END) AS high_quality_records,
        MAX(load_date) AS latest_load_date,
        MAX(update_date) AS latest_update_date,
        source_system
    FROM {{ ref('si_webinars') }}
    GROUP BY source_system
    
    UNION ALL
    
    SELECT 
        'si_support_tickets' AS table_name,
        COUNT(*) AS total_records,
        COUNT(CASE WHEN record_status = 'ACTIVE' THEN 1 END) AS active_records,
        AVG(data_quality_score) AS avg_quality_score,
        COUNT(CASE WHEN data_quality_score >= 0.7 THEN 1 END) AS high_quality_records,
        MAX(load_date) AS latest_load_date,
        MAX(update_date) AS latest_update_date,
        source_system
    FROM {{ ref('si_support_tickets') }}
    GROUP BY source_system
    
    UNION ALL
    
    SELECT 
        'si_licenses' AS table_name,
        COUNT(*) AS total_records,
        COUNT(CASE WHEN record_status = 'ACTIVE' THEN 1 END) AS active_records,
        AVG(data_quality_score) AS avg_quality_score,
        COUNT(CASE WHEN data_quality_score >= 0.7 THEN 1 END) AS high_quality_records,
        MAX(load_date) AS latest_load_date,
        MAX(update_date) AS latest_update_date,
        source_system
    FROM {{ ref('si_licenses') }}
    GROUP BY source_system
    
    UNION ALL
    
    SELECT 
        'si_billing_events' AS table_name,
        COUNT(*) AS total_records,
        COUNT(CASE WHEN record_status = 'ACTIVE' THEN 1 END) AS active_records,
        AVG(data_quality_score) AS avg_quality_score,
        COUNT(CASE WHEN data_quality_score >= 0.7 THEN 1 END) AS high_quality_records,
        MAX(load_date) AS latest_load_date,
        MAX(update_date) AS latest_update_date,
        source_system
    FROM {{ ref('si_billing_events') }}
    GROUP BY source_system
)

SELECT 
    UUID_STRING() AS quality_fact_key,
    table_name,
    source_system,
    total_records,
    active_records,
    ROUND(avg_quality_score, 3) AS avg_quality_score,
    high_quality_records,
    CASE 
        WHEN total_records > 0 THEN 
            ROUND((active_records::FLOAT / total_records::FLOAT) * 100, 2)
        ELSE 0 
    END AS active_record_percentage,
    CASE 
        WHEN total_records > 0 THEN 
            ROUND((high_quality_records::FLOAT / total_records::FLOAT) * 100, 2)
        ELSE 0 
    END AS high_quality_percentage,
    latest_load_date,
    latest_update_date,
    DATEDIFF('day', latest_load_date, CURRENT_DATE()) AS days_since_last_load,
    CASE 
        WHEN avg_quality_score >= 0.9 THEN 'Excellent'
        WHEN avg_quality_score >= 0.7 THEN 'Good'
        WHEN avg_quality_score >= 0.5 THEN 'Fair'
        ELSE 'Poor'
    END AS quality_grade,
    CURRENT_TIMESTAMP() AS created_at
FROM quality_metrics
{% if is_incremental() %}
WHERE latest_update_date > (SELECT MAX(latest_update_date) FROM {{ this }})
{% endif %}
