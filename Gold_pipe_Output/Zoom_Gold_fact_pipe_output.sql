-- =====================================================
-- Snowflake dbt Unit Test Cases for Zoom Gold Fact Tables
-- =====================================================
-- File: Zoom_Gold_fact_pipe_output.sql
-- Purpose: Comprehensive unit testing for all Zoom fact tables
-- Tables Covered: go_meeting_facts, go_participant_facts, go_webinar_facts, 
--                go_billing_facts, go_usage_facts, go_quality_facts
-- Last Updated: $(date)
-- =====================================================

-- Test Configuration
{{ config(
    materialized='test',
    tags=['unit_test', 'gold_layer', 'fact_tables']
) }}

-- =====================================================
-- 1. GO_MEETING_FACTS Unit Tests
-- =====================================================

-- Test 1.1: Meeting Facts - Data Quality Tests
SELECT 'go_meeting_facts_null_check' as test_name,
       COUNT(*) as failed_records
FROM {{ ref('go_meeting_facts') }}
WHERE meeting_id IS NULL 
   OR meeting_uuid IS NULL
   OR start_time IS NULL
HAVING COUNT(*) > 0;

-- Test 1.2: Meeting Facts - Business Rule Validation
SELECT 'go_meeting_facts_duration_check' as test_name,
       COUNT(*) as failed_records
FROM {{ ref('go_meeting_facts') }}
WHERE duration < 0 
   OR (end_time IS NOT NULL AND start_time IS NOT NULL AND end_time < start_time)
HAVING COUNT(*) > 0;

-- Test 1.3: Meeting Facts - Uniqueness Test
SELECT 'go_meeting_facts_uniqueness' as test_name,
       COUNT(*) - COUNT(DISTINCT meeting_uuid) as failed_records
FROM {{ ref('go_meeting_facts') }}
HAVING COUNT(*) - COUNT(DISTINCT meeting_uuid) > 0;

-- =====================================================
-- 2. GO_PARTICIPANT_FACTS Unit Tests
-- =====================================================

-- Test 2.1: Participant Facts - Data Quality Tests
SELECT 'go_participant_facts_null_check' as test_name,
       COUNT(*) as failed_records
FROM {{ ref('go_participant_facts') }}
WHERE participant_id IS NULL 
   OR meeting_uuid IS NULL
   OR join_time IS NULL
HAVING COUNT(*) > 0;

-- Test 2.2: Participant Facts - Business Rule Validation
SELECT 'go_participant_facts_time_validation' as test_name,
       COUNT(*) as failed_records
FROM {{ ref('go_participant_facts') }}
WHERE (leave_time IS NOT NULL AND join_time IS NOT NULL AND leave_time < join_time)
   OR duration < 0
HAVING COUNT(*) > 0;

-- Test 2.3: Participant Facts - Referential Integrity
SELECT 'go_participant_facts_meeting_reference' as test_name,
       COUNT(*) as failed_records
FROM {{ ref('go_participant_facts') }} p
LEFT JOIN {{ ref('go_meeting_facts') }} m ON p.meeting_uuid = m.meeting_uuid
WHERE m.meeting_uuid IS NULL
HAVING COUNT(*) > 0;

-- =====================================================
-- 3. GO_WEBINAR_FACTS Unit Tests
-- =====================================================

-- Test 3.1: Webinar Facts - Data Quality Tests
SELECT 'go_webinar_facts_null_check' as test_name,
       COUNT(*) as failed_records
FROM {{ ref('go_webinar_facts') }}
WHERE webinar_id IS NULL 
   OR webinar_uuid IS NULL
   OR start_time IS NULL
HAVING COUNT(*) > 0;

-- Test 3.2: Webinar Facts - Business Rule Validation
SELECT 'go_webinar_facts_capacity_check' as test_name,
       COUNT(*) as failed_records
FROM {{ ref('go_webinar_facts') }}
WHERE max_participants < 0
   OR (actual_participants > max_participants AND max_participants > 0)
HAVING COUNT(*) > 0;

-- Test 3.3: Webinar Facts - Duration Validation
SELECT 'go_webinar_facts_duration_check' as test_name,
       COUNT(*) as failed_records
FROM {{ ref('go_webinar_facts') }}
WHERE duration < 0
   OR (end_time IS NOT NULL AND start_time IS NOT NULL AND end_time < start_time)
HAVING COUNT(*) > 0;

-- =====================================================
-- 4. GO_BILLING_FACTS Unit Tests
-- =====================================================

-- Test 4.1: Billing Facts - Data Quality Tests
SELECT 'go_billing_facts_null_check' as test_name,
       COUNT(*) as failed_records
FROM {{ ref('go_billing_facts') }}
WHERE account_id IS NULL 
   OR billing_period IS NULL
   OR amount IS NULL
HAVING COUNT(*) > 0;

-- Test 4.2: Billing Facts - Amount Validation
SELECT 'go_billing_facts_amount_check' as test_name,
       COUNT(*) as failed_records
FROM {{ ref('go_billing_facts') }}
WHERE amount < 0
   OR (amount = 0 AND billing_type NOT IN ('FREE', 'TRIAL', 'PROMOTIONAL'))
HAVING COUNT(*) > 0;

-- Test 4.3: Billing Facts - Period Validation
SELECT 'go_billing_facts_period_check' as test_name,
       COUNT(*) as failed_records
FROM {{ ref('go_billing_facts') }}
WHERE billing_period NOT LIKE '____-__'
   OR LENGTH(billing_period) != 7
HAVING COUNT(*) > 0;

-- =====================================================
-- 5. GO_USAGE_FACTS Unit Tests
-- =====================================================

-- Test 5.1: Usage Facts - Data Quality Tests
SELECT 'go_usage_facts_null_check' as test_name,
       COUNT(*) as failed_records
FROM {{ ref('go_usage_facts') }}
WHERE account_id IS NULL 
   OR usage_date IS NULL
   OR feature_name IS NULL
HAVING COUNT(*) > 0;

-- Test 5.2: Usage Facts - Metric Validation
SELECT 'go_usage_facts_metrics_check' as test_name,
       COUNT(*) as failed_records
FROM {{ ref('go_usage_facts') }}
WHERE usage_count < 0
   OR usage_duration < 0
   OR (usage_count = 0 AND usage_duration > 0)
HAVING COUNT(*) > 0;

-- Test 5.3: Usage Facts - Date Validation
SELECT 'go_usage_facts_date_check' as test_name,
       COUNT(*) as failed_records
FROM {{ ref('go_usage_facts') }}
WHERE usage_date > CURRENT_DATE()
   OR usage_date < '2020-01-01'
HAVING COUNT(*) > 0;

-- =====================================================
-- 6. GO_QUALITY_FACTS Unit Tests
-- =====================================================

-- Test 6.1: Quality Facts - Data Quality Tests
SELECT 'go_quality_facts_null_check' as test_name,
       COUNT(*) as failed_records
FROM {{ ref('go_quality_facts') }}
WHERE meeting_uuid IS NULL 
   OR participant_id IS NULL
   OR quality_metric IS NULL
HAVING COUNT(*) > 0;

-- Test 6.2: Quality Facts - Score Validation
SELECT 'go_quality_facts_score_range' as test_name,
       COUNT(*) as failed_records
FROM {{ ref('go_quality_facts') }}
WHERE quality_score < 0 
   OR quality_score > 100
   OR (quality_rating NOT IN ('POOR', 'FAIR', 'GOOD', 'EXCELLENT') AND quality_rating IS NOT NULL)
HAVING COUNT(*) > 0;

-- Test 6.3: Quality Facts - Metric Consistency
SELECT 'go_quality_facts_metric_consistency' as test_name,
       COUNT(*) as failed_records
FROM {{ ref('go_quality_facts') }}
WHERE (quality_score >= 80 AND quality_rating = 'POOR')
   OR (quality_score <= 20 AND quality_rating = 'EXCELLENT')
HAVING COUNT(*) > 0;

-- =====================================================
-- Cross-Table Integration Tests
-- =====================================================

-- Test 7.1: Meeting-Participant Relationship
SELECT 'meeting_participant_relationship' as test_name,
       COUNT(*) as failed_records
FROM {{ ref('go_meeting_facts') }} m
LEFT JOIN {{ ref('go_participant_facts') }} p ON m.meeting_uuid = p.meeting_uuid
WHERE m.participant_count > 0 AND p.meeting_uuid IS NULL
HAVING COUNT(*) > 0;

-- Test 7.2: Quality-Meeting Relationship
SELECT 'quality_meeting_relationship' as test_name,
       COUNT(*) as failed_records
FROM {{ ref('go_quality_facts') }} q
LEFT JOIN {{ ref('go_meeting_facts') }} m ON q.meeting_uuid = m.meeting_uuid
WHERE m.meeting_uuid IS NULL
HAVING COUNT(*) > 0;

-- =====================================================
-- Performance and Volume Tests
-- =====================================================

-- Test 8.1: Record Count Validation
SELECT 'record_count_validation' as test_name,
       'go_meeting_facts' as table_name,
       COUNT(*) as record_count,
       CASE WHEN COUNT(*) = 0 THEN 'FAIL' ELSE 'PASS' END as test_result
FROM {{ ref('go_meeting_facts') }}

UNION ALL

SELECT 'record_count_validation' as test_name,
       'go_participant_facts' as table_name,
       COUNT(*) as record_count,
       CASE WHEN COUNT(*) = 0 THEN 'FAIL' ELSE 'PASS' END as test_result
FROM {{ ref('go_participant_facts') }}

UNION ALL

SELECT 'record_count_validation' as test_name,
       'go_webinar_facts' as table_name,
       COUNT(*) as record_count,
       CASE WHEN COUNT(*) = 0 THEN 'FAIL' ELSE 'PASS' END as test_result
FROM {{ ref('go_webinar_facts') }}

UNION ALL

SELECT 'record_count_validation' as test_name,
       'go_billing_facts' as table_name,
       COUNT(*) as record_count,
       CASE WHEN COUNT(*) = 0 THEN 'FAIL' ELSE 'PASS' END as test_result
FROM {{ ref('go_billing_facts') }}

UNION ALL

SELECT 'record_count_validation' as test_name,
       'go_usage_facts' as table_name,
       COUNT(*) as record_count,
       CASE WHEN COUNT(*) = 0 THEN 'FAIL' ELSE 'PASS' END as test_result
FROM {{ ref('go_usage_facts') }}

UNION ALL

SELECT 'record_count_validation' as test_name,
       'go_quality_facts' as table_name,
       COUNT(*) as record_count,
       CASE WHEN COUNT(*) = 0 THEN 'FAIL' ELSE 'PASS' END as test_result
FROM {{ ref('go_quality_facts') }};

-- =====================================================
-- Edge Case Tests
-- =====================================================

-- Test 9.1: Timezone Handling
SELECT 'timezone_consistency' as test_name,
       COUNT(*) as failed_records
FROM {{ ref('go_meeting_facts') }}
WHERE start_time::string LIKE '%+%' 
   OR start_time::string LIKE '%-__:__'
HAVING COUNT(*) > 0;

-- Test 9.2: Large Meeting Validation
SELECT 'large_meeting_validation' as test_name,
       COUNT(*) as failed_records
FROM {{ ref('go_meeting_facts') }} m
JOIN (SELECT meeting_uuid, COUNT(*) as actual_participants 
      FROM {{ ref('go_participant_facts') }} 
      GROUP BY meeting_uuid) p ON m.meeting_uuid = p.meeting_uuid
WHERE ABS(m.participant_count - p.actual_participants) > 5
HAVING COUNT(*) > 0;

-- =====================================================
-- Data Freshness Tests
-- =====================================================

-- Test 10.1: Data Freshness Check
SELECT 'data_freshness_check' as test_name,
       table_name,
       max_date,
       DATEDIFF(day, max_date, CURRENT_DATE()) as days_old,
       CASE WHEN DATEDIFF(day, max_date, CURRENT_DATE()) > 7 THEN 'FAIL' ELSE 'PASS' END as test_result
FROM (
    SELECT 'go_meeting_facts' as table_name, MAX(start_time::date) as max_date FROM {{ ref('go_meeting_facts') }}
    UNION ALL
    SELECT 'go_participant_facts' as table_name, MAX(join_time::date) as max_date FROM {{ ref('go_participant_facts') }}
    UNION ALL
    SELECT 'go_webinar_facts' as table_name, MAX(start_time::date) as max_date FROM {{ ref('go_webinar_facts') }}
    UNION ALL
    SELECT 'go_billing_facts' as table_name, MAX(TO_DATE(billing_period || '-01', 'YYYY-MM-DD')) as max_date FROM {{ ref('go_billing_facts') }}
    UNION ALL
    SELECT 'go_usage_facts' as table_name, MAX(usage_date) as max_date FROM {{ ref('go_usage_facts') }}
    UNION ALL
    SELECT 'go_quality_facts' as table_name, MAX(created_date::date) as max_date FROM {{ ref('go_quality_facts') }}
);

-- =====================================================
-- Summary Test Results
-- =====================================================

-- This query provides a summary of all test results
-- Run this after executing all individual tests
/*
SELECT 
    test_name,
    SUM(failed_records) as total_failures,
    CASE WHEN SUM(failed_records) = 0 THEN 'PASS' ELSE 'FAIL' END as overall_result
FROM (
    -- Include all test results here
    -- This would be populated by the test execution framework
) test_results
GROUP BY test_name
ORDER BY total_failures DESC;
*/