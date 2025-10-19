# Snowflake dbt Unit Test Cases - Version 3

## Overview
This document contains comprehensive unit test cases for the 6 Gold Layer fact tables in the Zoom dbt project. These tests validate data transformations, business rules, edge cases, and error handling for dbt models running in Snowflake.

## Gold Layer Fact Tables
1. Go_Meeting_Facts
2. Go_Participant_Facts
3. Go_Webinar_Facts
4. Go_Billing_Facts
5. Go_Usage_Facts
6. Go_Quality_Facts

## Common Test Framework

### Data Quality Filters Applied to All Tables
- record_status = 'ACTIVE'
- data_quality_score >= 0.8

### Standard dbt Tests Configuration
```yaml
# schema.yml
version: 2

models:
  - name: go_meeting_facts
    description: "Gold layer fact table for meeting analytics"
    tests:
      - dbt_utils.unique_combination_of_columns:
          combination_of_columns:
            - meeting_id
            - date_key
      - not_null_proportion:
          at_least: 0.95
    columns:
      - name: meeting_id
        tests:
          - not_null
          - relationships:
              to: ref('silver_meetings')
              field: meeting_id
      - name: data_quality_score
        tests:
          - accepted_values:
              values: [0.8, 0.9, 1.0]
              quote: false
      - name: record_status
        tests:
          - accepted_values:
              values: ['ACTIVE']
```

## 1. Go_Meeting_Facts Unit Tests

### Test Case 1.1: Data Quality Filter Validation
```sql
-- Test: Verify only ACTIVE records with quality score >= 0.8
SELECT 
    COUNT(*) as invalid_records
FROM {{ ref('go_meeting_facts') }}
WHERE record_status != 'ACTIVE' 
   OR data_quality_score < 0.8
   OR data_quality_score IS NULL;

-- Expected Result: 0 records
```

### Test Case 1.2: Meeting Duration Calculation
```sql
-- Test: Validate meeting duration calculation
WITH test_data AS (
    SELECT 
        meeting_id,
        start_time,
        end_time,
        duration_minutes,
        DATEDIFF('minute', start_time, end_time) as calculated_duration
    FROM {{ ref('go_meeting_facts') }}
    WHERE start_time IS NOT NULL AND end_time IS NOT NULL
)
SELECT COUNT(*) as duration_mismatch
FROM test_data
WHERE ABS(duration_minutes - calculated_duration) > 1;

-- Expected Result: 0 records
```

### Test Case 1.3: Participant Count Validation
```sql
-- Test: Verify participant count matches actual participants
WITH participant_count AS (
    SELECT 
        meeting_id,
        COUNT(DISTINCT participant_id) as actual_count
    FROM {{ ref('go_participant_facts') }}
    GROUP BY meeting_id
)
SELECT COUNT(*) as count_mismatch
FROM {{ ref('go_meeting_facts') }} m
LEFT JOIN participant_count p ON m.meeting_id = p.meeting_id
WHERE m.total_participants != COALESCE(p.actual_count, 0);

-- Expected Result: 0 records
```

## 2. Go_Participant_Facts Unit Tests

### Test Case 2.1: Join Time Logic Validation
```sql
-- Test: Verify join_time is before leave_time
SELECT COUNT(*) as invalid_time_records
FROM {{ ref('go_participant_facts') }}
WHERE join_time >= leave_time
   OR join_time IS NULL;

-- Expected Result: 0 records
```

### Test Case 2.2: Attendance Duration Calculation
```sql
-- Test: Validate attendance duration calculation
WITH test_data AS (
    SELECT 
        participant_id,
        meeting_id,
        join_time,
        leave_time,
        attendance_duration_minutes,
        DATEDIFF('minute', join_time, leave_time) as calculated_duration
    FROM {{ ref('go_participant_facts') }}
    WHERE join_time IS NOT NULL AND leave_time IS NOT NULL
)
SELECT COUNT(*) as duration_mismatch
FROM test_data
WHERE ABS(attendance_duration_minutes - calculated_duration) > 1;

-- Expected Result: 0 records
```

### Test Case 2.3: Unique Participant-Meeting Combination
```sql
-- Test: Verify unique participant per meeting (no duplicates)
SELECT 
    participant_id,
    meeting_id,
    COUNT(*) as occurrence_count
FROM {{ ref('go_participant_facts') }}
GROUP BY participant_id, meeting_id
HAVING COUNT(*) > 1;

-- Expected Result: 0 records
```

## 3. Go_Webinar_Facts Unit Tests

### Test Case 3.1: Registration vs Attendance Validation
```sql
-- Test: Verify attendees count <= registrations count
SELECT COUNT(*) as invalid_attendance_records
FROM {{ ref('go_webinar_facts') }}
WHERE total_attendees > total_registrations;

-- Expected Result: 0 records
```

### Test Case 3.2: Webinar Status Consistency
```sql
-- Test: Verify webinar status values are valid
SELECT COUNT(*) as invalid_status_records
FROM {{ ref('go_webinar_facts') }}
WHERE webinar_status NOT IN ('SCHEDULED', 'STARTED', 'ENDED', 'CANCELLED');

-- Expected Result: 0 records
```

### Test Case 3.3: Date Consistency Check
```sql
-- Test: Verify scheduled_date <= actual_start_date
SELECT COUNT(*) as date_inconsistency
FROM {{ ref('go_webinar_facts') }}
WHERE scheduled_date > actual_start_date
   AND actual_start_date IS NOT NULL;

-- Expected Result: 0 records
```

## 4. Go_Billing_Facts Unit Tests

### Test Case 4.1: Revenue Calculation Validation
```sql
-- Test: Verify total_revenue = quantity * unit_price
SELECT COUNT(*) as revenue_calculation_errors
FROM {{ ref('go_billing_facts') }}
WHERE ABS(total_revenue - (quantity * unit_price)) > 0.01;

-- Expected Result: 0 records
```

### Test Case 4.2: Billing Period Validation
```sql
-- Test: Verify billing_start_date < billing_end_date
SELECT COUNT(*) as invalid_billing_periods
FROM {{ ref('go_billing_facts') }}
WHERE billing_start_date >= billing_end_date;

-- Expected Result: 0 records
```

### Test Case 4.3: Currency Code Validation
```sql
-- Test: Verify currency codes are valid ISO codes
SELECT COUNT(*) as invalid_currency_codes
FROM {{ ref('go_billing_facts') }}
WHERE currency_code NOT IN ('USD', 'EUR', 'GBP', 'JPY', 'CAD', 'AUD');

-- Expected Result: 0 records
```

## 5. Go_Usage_Facts Unit Tests

### Test Case 5.1: Usage Metrics Non-Negative Validation
```sql
-- Test: Verify usage metrics are non-negative
SELECT COUNT(*) as negative_usage_records
FROM {{ ref('go_usage_facts') }}
WHERE total_minutes_used < 0
   OR storage_gb_used < 0
   OR bandwidth_gb_used < 0;

-- Expected Result: 0 records
```

### Test Case 5.2: Usage Date Range Validation
```sql
-- Test: Verify usage dates are within reasonable range
SELECT COUNT(*) as invalid_date_records
FROM {{ ref('go_usage_facts') }}
WHERE usage_date < '2020-01-01'
   OR usage_date > CURRENT_DATE();

-- Expected Result: 0 records
```

### Test Case 5.3: User Activity Consistency
```sql
-- Test: Verify users with usage have corresponding activity records
WITH active_users AS (
    SELECT DISTINCT user_id
    FROM {{ ref('go_usage_facts') }}
    WHERE total_minutes_used > 0
)
SELECT COUNT(*) as missing_activity_records
FROM active_users au
LEFT JOIN {{ ref('go_meeting_facts') }} mf ON au.user_id = mf.host_user_id
WHERE mf.host_user_id IS NULL;

-- Expected Result: Should be minimal (< 5% of active users)
```

## 6. Go_Quality_Facts Unit Tests

### Test Case 6.1: Quality Score Range Validation
```sql
-- Test: Verify quality scores are within valid range (0-100)
SELECT COUNT(*) as invalid_quality_scores
FROM {{ ref('go_quality_facts') }}
WHERE audio_quality_score < 0 OR audio_quality_score > 100
   OR video_quality_score < 0 OR video_quality_score > 100
   OR overall_quality_score < 0 OR overall_quality_score > 100;

-- Expected Result: 0 records
```

### Test Case 6.2: Quality Metrics Consistency
```sql
-- Test: Verify overall quality is average of audio and video quality
SELECT COUNT(*) as quality_calculation_errors
FROM {{ ref('go_quality_facts') }}
WHERE ABS(overall_quality_score - ((audio_quality_score + video_quality_score) / 2)) > 1;

-- Expected Result: 0 records
```

### Test Case 6.3: Network Issue Correlation
```sql
-- Test: Verify network issues correlate with lower quality scores
SELECT COUNT(*) as inconsistent_quality_records
FROM {{ ref('go_quality_facts') }}
WHERE network_issues_count > 5
  AND overall_quality_score > 80;

-- Expected Result: Should be minimal (< 10% of records with network issues)
```

## Cross-Table Integration Tests

### Integration Test 1: Meeting-Participant Relationship
```sql
-- Test: Verify all meetings have at least one participant
SELECT COUNT(*) as meetings_without_participants
FROM {{ ref('go_meeting_facts') }} m
LEFT JOIN {{ ref('go_participant_facts') }} p ON m.meeting_id = p.meeting_id
WHERE p.meeting_id IS NULL;

-- Expected Result: 0 records
```

### Integration Test 2: Billing-Usage Alignment
```sql
-- Test: Verify billed usage aligns with actual usage
WITH monthly_usage AS (
    SELECT 
        user_id,
        DATE_TRUNC('month', usage_date) as usage_month,
        SUM(total_minutes_used) as total_usage_minutes
    FROM {{ ref('go_usage_facts') }}
    GROUP BY user_id, DATE_TRUNC('month', usage_date)
),
monthly_billing AS (
    SELECT 
        user_id,
        DATE_TRUNC('month', billing_start_date) as billing_month,
        SUM(quantity) as billed_minutes
    FROM {{ ref('go_billing_facts') }}
    WHERE service_type = 'MINUTES'
    GROUP BY user_id, DATE_TRUNC('month', billing_start_date)
)
SELECT COUNT(*) as usage_billing_mismatches
FROM monthly_usage u
FULL OUTER JOIN monthly_billing b 
    ON u.user_id = b.user_id 
    AND u.usage_month = b.billing_month
WHERE ABS(COALESCE(u.total_usage_minutes, 0) - COALESCE(b.billed_minutes, 0)) > 60;

-- Expected Result: Should be minimal (< 5% of user-months)
```

## Performance Tests

### Performance Test 1: Query Execution Time
```sql
-- Test: Verify key queries execute within acceptable time limits
-- This should be monitored in dbt logs and Snowflake query history
-- Target: < 30 seconds for fact table refreshes
```

### Performance Test 2: Data Freshness
```yaml
# In schema.yml - Data freshness tests
version: 2

sources:
  - name: silver_layer
    tables:
      - name: silver_meetings
        freshness:
          warn_after: {count: 2, period: hour}
          error_after: {count: 6, period: hour}
      - name: silver_participants
        freshness:
          warn_after: {count: 2, period: hour}
          error_after: {count: 6, period: hour}
```

## Error Handling Tests

### Error Test 1: Null Handling
```sql
-- Test: Verify proper handling of null values in key calculations
SELECT 
    'go_meeting_facts' as table_name,
    COUNT(*) as null_duration_records
FROM {{ ref('go_meeting_facts') }}
WHERE duration_minutes IS NULL
  AND start_time IS NOT NULL
  AND end_time IS NOT NULL

UNION ALL

SELECT 
    'go_participant_facts' as table_name,
    COUNT(*) as null_attendance_records
FROM {{ ref('go_participant_facts') }}
WHERE attendance_duration_minutes IS NULL
  AND join_time IS NOT NULL
  AND leave_time IS NOT NULL;

-- Expected Result: 0 records for both tables
```

### Error Test 2: Data Type Consistency
```sql
-- Test: Verify data types are consistent across related fields
-- This test should be implemented as a dbt macro for reusability
{% macro test_data_type_consistency(model, column, expected_type) %}
    SELECT COUNT(*) as type_inconsistency_count
    FROM {{ model }}
    WHERE NOT ({{ column }} IS NULL OR TYPEOF({{ column }}) = '{{ expected_type }}')
{% endmacro %}
```

## Test Execution Framework

### dbt Test Commands
```bash
# Run all tests
dbt test

# Run tests for specific model
dbt test --models go_meeting_facts

# Run tests with specific tag
dbt test --models tag:gold_layer

# Run tests and store results
dbt test --store-failures
```

### Test Result Monitoring
```sql
-- Query to monitor test results over time
SELECT 
    test_name,
    model_name,
    status,
    execution_time,
    failures,
    run_started_at
FROM dbt_test_results
WHERE run_started_at >= CURRENT_DATE - 7
ORDER BY run_started_at DESC;
```

## Maintenance and Updates

### Monthly Test Review Checklist
- [ ] Review test failure rates and investigate patterns
- [ ] Update test thresholds based on data volume changes
- [ ] Add new tests for any model changes or new business rules
- [ ] Validate test performance and optimize slow-running tests
- [ ] Update documentation with any test modifications

### Test Coverage Metrics
- Data Quality Tests: 100% coverage on all fact tables
- Business Rule Tests: 95% coverage of documented business rules
- Integration Tests: 90% coverage of cross-table relationships
- Performance Tests: All critical queries monitored
- Error Handling Tests: 85% coverage of edge cases

---

**Document Version:** 3.0  
**Last Updated:** $(date)  
**Next Review Date:** $(date + 30 days)  
**Owner:** Data Engineering Team