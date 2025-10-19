-- =====================================================
-- Custom dbt Test Macros for Zoom Gold Fact Tables
-- =====================================================
-- File: custom_test_macros.sql
-- Purpose: Custom test macros for advanced validation scenarios
-- =====================================================

-- =====================================================
-- Macro: test_not_null_proportion
-- Purpose: Test that a minimum proportion of records are not null
-- =====================================================
{% macro test_not_null_proportion(model, column_name, at_least=0.95) %}

  SELECT 
    '{{ column_name }}' as column_name,
    COUNT(*) as total_records,
    COUNT({{ column_name }}) as non_null_records,
    COUNT({{ column_name }}) * 1.0 / COUNT(*) as non_null_proportion,
    {{ at_least }} as required_proportion,
    CASE 
      WHEN COUNT({{ column_name }}) * 1.0 / COUNT(*) >= {{ at_least }} THEN 'PASS'
      ELSE 'FAIL'
    END as test_result
  FROM {{ model }}
  HAVING COUNT({{ column_name }}) * 1.0 / COUNT(*) < {{ at_least }}

{% endmacro %}

-- =====================================================
-- Macro: test_meeting_participant_consistency
-- Purpose: Validate meeting participant counts match actual participants
-- =====================================================
{% macro test_meeting_participant_consistency() %}

  SELECT 
    m.meeting_uuid,
    m.participant_count as declared_count,
    COUNT(p.participant_id) as actual_count,
    ABS(m.participant_count - COUNT(p.participant_id)) as difference,
    CASE 
      WHEN ABS(m.participant_count - COUNT(p.participant_id)) <= 2 THEN 'PASS'
      ELSE 'FAIL'
    END as test_result
  FROM {{ ref('go_meeting_facts') }} m
  LEFT JOIN {{ ref('go_participant_facts') }} p ON m.meeting_uuid = p.meeting_uuid
  GROUP BY m.meeting_uuid, m.participant_count
  HAVING ABS(m.participant_count - COUNT(p.participant_id)) > 2

{% endmacro %}

-- =====================================================
-- Macro: test_quality_score_rating_alignment
-- Purpose: Ensure quality scores align with quality ratings
-- =====================================================
{% macro test_quality_score_rating_alignment() %}

  SELECT 
    meeting_uuid,
    participant_id,
    quality_metric,
    quality_score,
    quality_rating,
    CASE 
      WHEN quality_score >= 80 AND quality_rating IN ('GOOD', 'EXCELLENT') THEN 'ALIGNED'
      WHEN quality_score BETWEEN 60 AND 79 AND quality_rating = 'FAIR' THEN 'ALIGNED'
      WHEN quality_score < 60 AND quality_rating = 'POOR' THEN 'ALIGNED'
      ELSE 'MISALIGNED'
    END as alignment_status
  FROM {{ ref('go_quality_facts') }}
  WHERE quality_score IS NOT NULL AND quality_rating IS NOT NULL
  HAVING alignment_status = 'MISALIGNED'

{% endmacro %}

-- =====================================================
-- Macro: test_billing_amount_consistency
-- Purpose: Validate billing amounts are reasonable for account types
-- =====================================================
{% macro test_billing_amount_consistency() %}

  SELECT 
    account_id,
    billing_period,
    billing_type,
    amount,
    CASE 
      WHEN billing_type = 'FREE' AND amount > 0 THEN 'INCONSISTENT'
      WHEN billing_type = 'TRIAL' AND amount > 100 THEN 'INCONSISTENT'
      WHEN billing_type = 'SUBSCRIPTION' AND amount <= 0 THEN 'INCONSISTENT'
      WHEN billing_type = 'USAGE' AND amount < 0 THEN 'INCONSISTENT'
      ELSE 'CONSISTENT'
    END as consistency_status
  FROM {{ ref('go_billing_facts') }}
  WHERE consistency_status = 'INCONSISTENT'

{% endmacro %}

-- =====================================================
-- Macro: test_data_freshness_all_tables
-- Purpose: Check that all tables have recent data
-- =====================================================
{% macro test_data_freshness_all_tables(max_days_old=7) %}

  WITH freshness_check AS (
    SELECT 'go_meeting_facts' as table_name, 
           MAX(start_time::date) as latest_date,
           DATEDIFF(day, MAX(start_time::date), CURRENT_DATE()) as days_old
    FROM {{ ref('go_meeting_facts') }}
    
    UNION ALL
    
    SELECT 'go_participant_facts' as table_name,
           MAX(join_time::date) as latest_date,
           DATEDIFF(day, MAX(join_time::date), CURRENT_DATE()) as days_old
    FROM {{ ref('go_participant_facts') }}
    
    UNION ALL
    
    SELECT 'go_webinar_facts' as table_name,
           MAX(start_time::date) as latest_date,
           DATEDIFF(day, MAX(start_time::date), CURRENT_DATE()) as days_old
    FROM {{ ref('go_webinar_facts') }}
    
    UNION ALL
    
    SELECT 'go_billing_facts' as table_name,
           MAX(TO_DATE(billing_period || '-01', 'YYYY-MM-DD')) as latest_date,
           DATEDIFF(day, MAX(TO_DATE(billing_period || '-01', 'YYYY-MM-DD')), CURRENT_DATE()) as days_old
    FROM {{ ref('go_billing_facts') }}
    
    UNION ALL
    
    SELECT 'go_usage_facts' as table_name,
           MAX(usage_date) as latest_date,
           DATEDIFF(day, MAX(usage_date), CURRENT_DATE()) as days_old
    FROM {{ ref('go_usage_facts') }}
    
    UNION ALL
    
    SELECT 'go_quality_facts' as table_name,
           MAX(created_date::date) as latest_date,
           DATEDIFF(day, MAX(created_date::date), CURRENT_DATE()) as days_old
    FROM {{ ref('go_quality_facts') }}
  )
  
  SELECT 
    table_name,
    latest_date,
    days_old,
    {{ max_days_old }} as max_allowed_days,
    CASE 
      WHEN days_old <= {{ max_days_old }} THEN 'FRESH'
      ELSE 'STALE'
    END as freshness_status
  FROM freshness_check
  WHERE days_old > {{ max_days_old }}

{% endmacro %}

-- =====================================================
-- Macro: test_cross_table_referential_integrity
-- Purpose: Comprehensive referential integrity testing
-- =====================================================
{% macro test_cross_table_referential_integrity() %}

  -- Test 1: Participants must have valid meetings
  SELECT 
    'participant_meeting_integrity' as test_name,
    COUNT(*) as violation_count
  FROM {{ ref('go_participant_facts') }} p
  LEFT JOIN {{ ref('go_meeting_facts') }} m ON p.meeting_uuid = m.meeting_uuid
  WHERE m.meeting_uuid IS NULL
  HAVING COUNT(*) > 0
  
  UNION ALL
  
  -- Test 2: Quality facts must have valid meetings
  SELECT 
    'quality_meeting_integrity' as test_name,
    COUNT(*) as violation_count
  FROM {{ ref('go_quality_facts') }} q
  LEFT JOIN {{ ref('go_meeting_facts') }} m ON q.meeting_uuid = m.meeting_uuid
  WHERE m.meeting_uuid IS NULL
  HAVING COUNT(*) > 0
  
  UNION ALL
  
  -- Test 3: Quality facts should have valid participants
  SELECT 
    'quality_participant_integrity' as test_name,
    COUNT(*) as violation_count
  FROM {{ ref('go_quality_facts') }} q
  LEFT JOIN {{ ref('go_participant_facts') }} p 
    ON q.meeting_uuid = p.meeting_uuid 
    AND q.participant_id = p.participant_id
  WHERE p.participant_id IS NULL
  HAVING COUNT(*) > 0

{% endmacro %}

-- =====================================================
-- Macro: test_business_rule_validation
-- Purpose: Validate complex business rules across tables
-- =====================================================
{% macro test_business_rule_validation() %}

  -- Rule 1: Meeting duration should roughly match sum of participant durations
  SELECT 
    'meeting_duration_consistency' as rule_name,
    m.meeting_uuid,
    m.duration as meeting_duration,
    AVG(p.duration) as avg_participant_duration,
    COUNT(p.participant_id) as participant_count,
    ABS(m.duration - AVG(p.duration)) as duration_difference
  FROM {{ ref('go_meeting_facts') }} m
  JOIN {{ ref('go_participant_facts') }} p ON m.meeting_uuid = p.meeting_uuid
  GROUP BY m.meeting_uuid, m.duration
  HAVING ABS(m.duration - AVG(p.duration)) > 30  -- Allow 30 minute variance
  
  UNION ALL
  
  -- Rule 2: Webinar actual participants should not exceed max significantly
  SELECT 
    'webinar_capacity_validation' as rule_name,
    webinar_uuid,
    max_participants,
    actual_participants,
    (actual_participants - max_participants) as overflow
  FROM {{ ref('go_webinar_facts') }}
  WHERE actual_participants > max_participants * 1.1  -- Allow 10% overflow
  
  UNION ALL
  
  -- Rule 3: Usage should correlate with billing for usage-based accounts
  SELECT 
    'usage_billing_correlation' as rule_name,
    b.account_id,
    b.billing_period,
    b.amount as billing_amount,
    SUM(u.usage_count) as total_usage
  FROM {{ ref('go_billing_facts') }} b
  JOIN {{ ref('go_usage_facts') }} u 
    ON b.account_id = u.account_id 
    AND DATE_TRUNC('month', u.usage_date) = TO_DATE(b.billing_period || '-01', 'YYYY-MM-DD')
  WHERE b.billing_type = 'USAGE'
  GROUP BY b.account_id, b.billing_period, b.amount
  HAVING (b.amount > 0 AND SUM(u.usage_count) = 0) 
      OR (b.amount = 0 AND SUM(u.usage_count) > 100)

{% endmacro %}

-- =====================================================
-- Macro: test_performance_metrics
-- Purpose: Monitor performance-related metrics
-- =====================================================
{% macro test_performance_metrics() %}

  SELECT 
    'table_size_monitoring' as metric_name,
    table_name,
    record_count,
    CASE 
      WHEN record_count = 0 THEN 'EMPTY_TABLE'
      WHEN record_count > 10000000 THEN 'VERY_LARGE'
      WHEN record_count > 1000000 THEN 'LARGE'
      ELSE 'NORMAL'
    END as size_category
  FROM (
    SELECT 'go_meeting_facts' as table_name, COUNT(*) as record_count FROM {{ ref('go_meeting_facts') }}
    UNION ALL
    SELECT 'go_participant_facts' as table_name, COUNT(*) as record_count FROM {{ ref('go_participant_facts') }}
    UNION ALL
    SELECT 'go_webinar_facts' as table_name, COUNT(*) as record_count FROM {{ ref('go_webinar_facts') }}
    UNION ALL
    SELECT 'go_billing_facts' as table_name, COUNT(*) as record_count FROM {{ ref('go_billing_facts') }}
    UNION ALL
    SELECT 'go_usage_facts' as table_name, COUNT(*) as record_count FROM {{ ref('go_usage_facts') }}
    UNION ALL
    SELECT 'go_quality_facts' as table_name, COUNT(*) as record_count FROM {{ ref('go_quality_facts') }}
  ) table_sizes
  WHERE size_category IN ('EMPTY_TABLE', 'VERY_LARGE')

{% endmacro %}

-- =====================================================
-- Macro: test_data_distribution
-- Purpose: Check for data distribution anomalies
-- =====================================================
{% macro test_data_distribution() %}

  -- Check for date distribution anomalies
  WITH date_distribution AS (
    SELECT 
      DATE_TRUNC('month', start_time) as month_year,
      COUNT(*) as meeting_count
    FROM {{ ref('go_meeting_facts') }}
    WHERE start_time >= DATEADD('month', -12, CURRENT_DATE())
    GROUP BY DATE_TRUNC('month', start_time)
  ),
  avg_monthly_count AS (
    SELECT AVG(meeting_count) as avg_count,
           STDDEV(meeting_count) as stddev_count
    FROM date_distribution
  )
  
  SELECT 
    'monthly_meeting_distribution' as test_name,
    d.month_year,
    d.meeting_count,
    a.avg_count,
    ABS(d.meeting_count - a.avg_count) / NULLIF(a.stddev_count, 0) as z_score
  FROM date_distribution d
  CROSS JOIN avg_monthly_count a
  WHERE ABS(d.meeting_count - a.avg_count) / NULLIF(a.stddev_count, 0) > 2  -- More than 2 standard deviations

{% endmacro %}