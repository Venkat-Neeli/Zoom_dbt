{{
  config(
    materialized='table',
    tags=['gold', 'fact_table', 'quality']
  )
}}

WITH quality_metrics AS (
  SELECT 
    'si_users' as table_name,
    COUNT(*) as total_records,
    COUNT(CASE WHEN record_status = 'ACTIVE' THEN 1 END) as active_records,
    AVG(data_quality_score) as avg_quality_score,
    MIN(data_quality_score) as min_quality_score,
    MAX(data_quality_score) as max_quality_score,
    COUNT(CASE WHEN data_quality_score < 0.8 THEN 1 END) as low_quality_records,
    MAX(update_timestamp) as last_update
  FROM {{ ref('si_users') }}
  
  UNION ALL
  
  SELECT 
    'si_meetings' as table_name,
    COUNT(*) as total_records,
    COUNT(CASE WHEN record_status = 'ACTIVE' THEN 1 END) as active_records,
    AVG(data_quality_score) as avg_quality_score,
    MIN(data_quality_score) as min_quality_score,
    MAX(data_quality_score) as max_quality_score,
    COUNT(CASE WHEN data_quality_score < 0.8 THEN 1 END) as low_quality_records,
    MAX(update_timestamp) as last_update
  FROM {{ ref('si_meetings') }}
  
  UNION ALL
  
  SELECT 
    'si_participants' as table_name,
    COUNT(*) as total_records,
    COUNT(CASE WHEN record_status = 'ACTIVE' THEN 1 END) as active_records,
    AVG(data_quality_score) as avg_quality_score,
    MIN(data_quality_score) as min_quality_score,
    MAX(data_quality_score) as max_quality_score,
    COUNT(CASE WHEN data_quality_score < 0.8 THEN 1 END) as low_quality_records,
    MAX(update_timestamp) as last_update
  FROM {{ ref('si_participants') }}
  
  UNION ALL
  
  SELECT 
    'si_feature_usage' as table_name,
    COUNT(*) as total_records,
    COUNT(CASE WHEN record_status = 'ACTIVE' THEN 1 END) as active_records,
    AVG(data_quality_score) as avg_quality_score,
    MIN(data_quality_score) as min_quality_score,
    MAX(data_quality_score) as max_quality_score,
    COUNT(CASE WHEN data_quality_score < 0.8 THEN 1 END) as low_quality_records,
    MAX(update_timestamp) as last_update
  FROM {{ ref('si_feature_usage') }}
  
  UNION ALL
  
  SELECT 
    'si_webinars' as table_name,
    COUNT(*) as total_records,
    COUNT(CASE WHEN record_status = 'ACTIVE' THEN 1 END) as active_records,
    AVG(data_quality_score) as avg_quality_score,
    MIN(data_quality_score) as min_quality_score,
    MAX(data_quality_score) as max_quality_score,
    COUNT(CASE WHEN data_quality_score < 0.8 THEN 1 END) as low_quality_records,
    MAX(update_timestamp) as last_update
  FROM {{ ref('si_webinars') }}
  
  UNION ALL
  
  SELECT 
    'si_billing_events' as table_name,
    COUNT(*) as total_records,
    COUNT(CASE WHEN record_status = 'ACTIVE' THEN 1 END) as active_records,
    AVG(data_quality_score) as avg_quality_score,
    MIN(data_quality_score) as min_quality_score,
    MAX(data_quality_score) as max_quality_score,
    COUNT(CASE WHEN data_quality_score < 0.8 THEN 1 END) as low_quality_records,
    MAX(update_timestamp) as last_update
  FROM {{ ref('si_billing_events') }}
  
  UNION ALL
  
  SELECT 
    'si_licenses' as table_name,
    COUNT(*) as total_records,
    COUNT(CASE WHEN record_status = 'ACTIVE' THEN 1 END) as active_records,
    AVG(data_quality_score) as avg_quality_score,
    MIN(data_quality_score) as min_quality_score,
    MAX(data_quality_score) as max_quality_score,
    COUNT(CASE WHEN data_quality_score < 0.8 THEN 1 END) as low_quality_records,
    MAX(update_timestamp) as last_update
  FROM {{ ref('si_licenses') }}
)

SELECT 
  {{ dbt_utils.generate_surrogate_key(['table_name', 'CURRENT_DATE()']) }} as quality_fact_key,
  table_name,
  total_records,
  active_records,
  ROUND(avg_quality_score, 4) as avg_quality_score,
  ROUND(min_quality_score, 4) as min_quality_score,
  ROUND(max_quality_score, 4) as max_quality_score,
  low_quality_records,
  CASE 
    WHEN (active_records::FLOAT / NULLIF(total_records, 0)) >= 0.95 THEN 'Excellent'
    WHEN (active_records::FLOAT / NULLIF(total_records, 0)) >= 0.85 THEN 'Good'
    WHEN (active_records::FLOAT / NULLIF(total_records, 0)) >= 0.70 THEN 'Fair'
    ELSE 'Poor'
  END as record_status_quality,
  CASE 
    WHEN avg_quality_score >= 0.95 THEN 'Excellent'
    WHEN avg_quality_score >= 0.85 THEN 'Good'
    WHEN avg_quality_score >= 0.70 THEN 'Fair'
    ELSE 'Poor'
  END as data_quality_rating,
  ROUND((low_quality_records::FLOAT / NULLIF(total_records, 0)) * 100, 2) as low_quality_percentage,
  last_update,
  CURRENT_TIMESTAMP() as created_at,
  CURRENT_DATE() as quality_check_date
FROM quality_metrics
