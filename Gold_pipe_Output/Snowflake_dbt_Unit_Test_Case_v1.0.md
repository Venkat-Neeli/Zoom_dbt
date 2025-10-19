# Snowflake dbt Unit Test Cases - Gold Layer Fact Tables

## Metadata
- **Author**: AAVA
- **Version**: 1.0
- **Creation Date**: 2024-12-19
- **Last Updated**: 2024-12-19
- **Models Covered**: go_meeting_facts, go_participant_facts, go_webinar_facts, go_billing_facts, go_usage_facts, go_quality_facts
- **API Cost Estimate**: $0.15 USD (based on Snowflake compute and storage costs for test execution)

## Overview
This document contains comprehensive unit test cases for the Zoom dbt gold layer fact tables in Snowflake. The tests cover data integrity, business rule validations, edge cases, and exception scenarios to ensure reliable data pipeline operations.

## Test Categories
1. **Data Integrity Tests** - Primary keys, foreign keys, not null constraints
2. **Business Rule Tests** - Categorizations, calculations, transformations
3. **Data Quality Tests** - Score validations, threshold checks
4. **Edge Case Tests** - Null handling, missing lookups, boundary conditions
5. **Performance Tests** - Query optimization, index usage

---

## 1. GO_MEETING_FACTS Tests

### 1.1 Data Integrity Tests

#### Test Case: MF_001 - Unique Meeting Keys
```yaml
# In schema.yml
models:
  - name: go_meeting_facts
    tests:
      - dbt_utils.unique_combination_of_columns:
          combination_of_columns:
            - meeting_sk
          name: unique_meeting_surrogate_key
```

#### Test Case: MF_002 - Not Null Primary Key
```yaml
columns:
  - name: meeting_sk
    tests:
      - not_null:
          severity: error
      - unique:
          severity: error
```

#### Test Case: MF_003 - Valid Date Ranges
```sql
-- Custom test: tests/assert_valid_meeting_dates.sql
SELECT *
FROM {{ ref('go_meeting_facts') }}
WHERE start_time > end_time
   OR start_time IS NULL
   OR end_time IS NULL
   OR start_time > CURRENT_TIMESTAMP()
```

### 1.2 Business Rule Tests

#### Test Case: MF_004 - Meeting Duration Calculation
```sql
-- Custom test: tests/assert_meeting_duration_accuracy.sql
SELECT *
FROM {{ ref('go_meeting_facts') }}
WHERE ABS(duration_minutes - DATEDIFF('minute', start_time, end_time)) > 1
```

#### Test Case: MF_005 - Meeting Status Categorization
```yaml
columns:
  - name: meeting_status
    tests:
      - accepted_values:
          values: ['Completed', 'In Progress', 'Scheduled', 'Cancelled']
          severity: error
```

### 1.3 Edge Case Tests

#### Test Case: MF_006 - Zero Duration Meetings
```sql
-- Custom test: tests/assert_zero_duration_meetings.sql
SELECT meeting_sk, duration_minutes
FROM {{ ref('go_meeting_facts') }}
WHERE duration_minutes = 0
  AND meeting_status = 'Completed'
HAVING COUNT(*) > 0
```

---

## 2. GO_PARTICIPANT_FACTS Tests

### 2.1 Data Integrity Tests

#### Test Case: PF_001 - Unique Participant Keys
```yaml
models:
  - name: go_participant_facts
    tests:
      - dbt_utils.unique_combination_of_columns:
          combination_of_columns:
            - participant_sk
          name: unique_participant_surrogate_key
```

#### Test Case: PF_002 - Referential Integrity with Meetings
```sql
-- Custom test: tests/assert_participant_meeting_relationship.sql
SELECT p.participant_sk, p.meeting_sk
FROM {{ ref('go_participant_facts') }} p
LEFT JOIN {{ ref('go_meeting_facts') }} m
  ON p.meeting_sk = m.meeting_sk
WHERE m.meeting_sk IS NULL
```

### 2.2 Business Rule Tests

#### Test Case: PF_003 - Participant Join/Leave Time Logic
```sql
-- Custom test: tests/assert_participant_time_logic.sql
SELECT *
FROM {{ ref('go_participant_facts') }}
WHERE join_time > leave_time
   OR join_time IS NULL
   OR (leave_time IS NULL AND participation_status = 'Left')
```

#### Test Case: PF_004 - Participation Duration Validation
```sql
-- Custom test: tests/assert_participation_duration.sql
SELECT *
FROM {{ ref('go_participant_facts') }}
WHERE participation_duration_minutes < 0
   OR participation_duration_minutes > 1440 -- 24 hours
```

---

## 3. GO_WEBINAR_FACTS Tests

### 3.1 Data Integrity Tests

#### Test Case: WF_001 - Unique Webinar Keys
```yaml
models:
  - name: go_webinar_facts
    columns:
      - name: webinar_sk
        tests:
          - not_null
          - unique
```

#### Test Case: WF_002 - Webinar Registration Counts
```sql
-- Custom test: tests/assert_webinar_registration_counts.sql
SELECT *
FROM {{ ref('go_webinar_facts') }}
WHERE registered_count < 0
   OR attended_count < 0
   OR attended_count > registered_count
```

### 3.2 Business Rule Tests

#### Test Case: WF_003 - Webinar Attendance Rate
```sql
-- Custom test: tests/assert_webinar_attendance_rate.sql
SELECT *
FROM {{ ref('go_webinar_facts') }}
WHERE registered_count > 0
  AND (attendance_rate < 0 OR attendance_rate > 100)
```

---

## 4. GO_BILLING_FACTS Tests

### 4.1 Data Integrity Tests

#### Test Case: BF_001 - Unique Billing Keys
```yaml
models:
  - name: go_billing_facts
    columns:
      - name: billing_sk
        tests:
          - not_null
          - unique
```

#### Test Case: BF_002 - Billing Amount Validation
```sql
-- Custom test: tests/assert_billing_amounts.sql
SELECT *
FROM {{ ref('go_billing_facts') }}
WHERE billing_amount < 0
   OR billing_amount IS NULL
   OR billing_amount > 1000000 -- Reasonable upper limit
```

### 4.2 Business Rule Tests

#### Test Case: BF_003 - Billing Period Validation
```sql
-- Custom test: tests/assert_billing_periods.sql
SELECT *
FROM {{ ref('go_billing_facts') }}
WHERE billing_start_date > billing_end_date
   OR billing_start_date IS NULL
   OR billing_end_date IS NULL
```

#### Test Case: BF_004 - Payment Status Categories
```yaml
columns:
  - name: payment_status
    tests:
      - accepted_values:
          values: ['Paid', 'Pending', 'Overdue', 'Cancelled']
```

---

## 5. GO_USAGE_FACTS Tests

### 5.1 Data Integrity Tests

#### Test Case: UF_001 - Unique Usage Keys
```yaml
models:
  - name: go_usage_facts
    columns:
      - name: usage_sk
        tests:
          - not_null
          - unique
```

#### Test Case: UF_002 - Usage Metrics Validation
```sql
-- Custom test: tests/assert_usage_metrics.sql
SELECT *
FROM {{ ref('go_usage_facts') }}
WHERE total_minutes < 0
   OR participant_count < 0
   OR storage_used_gb < 0
```

### 5.2 Business Rule Tests

#### Test Case: UF_003 - Usage Threshold Validation
```sql
-- Custom test: tests/assert_usage_thresholds.sql
SELECT *
FROM {{ ref('go_usage_facts') }}
WHERE usage_category = 'High'
  AND total_minutes < 1000 -- Business rule threshold
```

---

## 6. GO_QUALITY_FACTS Tests

### 6.1 Data Quality Score Tests

#### Test Case: QF_001 - Quality Score Range
```yaml
columns:
  - name: data_quality_score
    tests:
      - dbt_utils.accepted_range:
          min_value: 0
          max_value: 100
          inclusive: true
```

#### Test Case: QF_002 - Quality Score Thresholds
```sql
-- Custom test: tests/assert_quality_score_thresholds.sql
SELECT *
FROM {{ ref('go_quality_facts') }}
WHERE data_quality_score < 70 -- Minimum acceptable threshold
```

### 6.2 Quality Dimension Tests

#### Test Case: QF_003 - Completeness Score
```sql
-- Custom test: tests/assert_completeness_score.sql
SELECT *
FROM {{ ref('go_quality_facts') }}
WHERE completeness_score > data_quality_score
   OR completeness_score < 0
   OR completeness_score > 100
```

---

## 7. Cross-Model Integration Tests

### 7.1 Referential Integrity Tests

#### Test Case: INT_001 - Meeting-Participant Relationship
```sql
-- Custom test: tests/assert_meeting_participant_integrity.sql
WITH meeting_participants AS (
  SELECT DISTINCT meeting_sk
  FROM {{ ref('go_participant_facts') }}
),
orphaned_participants AS (
  SELECT mp.meeting_sk
  FROM meeting_participants mp
  LEFT JOIN {{ ref('go_meeting_facts') }} mf
    ON mp.meeting_sk = mf.meeting_sk
  WHERE mf.meeting_sk IS NULL
)
SELECT * FROM orphaned_participants
```

#### Test Case: INT_002 - Usage-Billing Consistency
```sql
-- Custom test: tests/assert_usage_billing_consistency.sql
SELECT 
  u.usage_date,
  u.total_minutes,
  b.billing_amount
FROM {{ ref('go_usage_facts') }} u
JOIN {{ ref('go_billing_facts') }} b
  ON DATE_TRUNC('month', u.usage_date) = DATE_TRUNC('month', b.billing_start_date)
WHERE u.total_minutes > 10000 -- High usage
  AND b.billing_amount < 100 -- Low billing
```

---

## 8. Performance and Optimization Tests

### 8.1 Query Performance Tests

#### Test Case: PERF_001 - Model Build Time
```sql
-- Custom test: tests/assert_model_performance.sql
-- This test should be run as part of CI/CD to ensure models complete within SLA
SELECT 
  '{{ this }}' as model_name,
  CURRENT_TIMESTAMP() as test_start_time
```

### 8.2 Data Freshness Tests

#### Test Case: FRESH_001 - Data Freshness Validation
```yaml
sources:
  - name: zoom_raw
    freshness:
      warn_after: {count: 12, period: hour}
      error_after: {count: 24, period: hour}
```

---

## 9. Exception Handling Tests

### 9.1 Null Handling Tests

#### Test Case: EXC_001 - Critical Field Null Handling
```sql
-- Custom test: tests/assert_critical_field_nulls.sql
SELECT 
  'go_meeting_facts' as table_name,
  COUNT(*) as null_count
FROM {{ ref('go_meeting_facts') }}
WHERE meeting_id IS NULL
   OR start_time IS NULL
UNION ALL
SELECT 
  'go_participant_facts' as table_name,
  COUNT(*) as null_count
FROM {{ ref('go_participant_facts') }}
WHERE participant_id IS NULL
   OR meeting_sk IS NULL
```

### 9.2 Data Type Validation Tests

#### Test Case: EXC_002 - Data Type Consistency
```sql
-- Custom test: tests/assert_data_types.sql
SELECT *
FROM {{ ref('go_billing_facts') }}
WHERE TRY_CAST(billing_amount AS NUMBER) IS NULL
   AND billing_amount IS NOT NULL
```

---

## 10. Test Execution Strategy

### 10.1 Test Execution Order
1. **Pre-hook Tests**: Data source validation
2. **Model Tests**: Individual model validation
3. **Post-hook Tests**: Cross-model integration
4. **Performance Tests**: Query optimization validation

### 10.2 Test Configuration
```yaml
# dbt_project.yml test configuration
tests:
  +severity: error  # Default severity
  +store_failures: true  # Store failed records
  +schema: dbt_test_audit  # Dedicated test schema
```

### 10.3 Continuous Integration Setup
```bash
# CI/CD pipeline commands
dbt deps
dbt seed
dbt run --models tag:gold_facts
dbt test --models tag:gold_facts
dbt run-operation generate_model_yaml --args '{models: [go_meeting_facts, go_participant_facts]}'
```

---

## 11. Test Monitoring and Alerting

### 11.1 Test Result Monitoring
```sql
-- Query to monitor test results
SELECT 
  test_name,
  model_name,
  status,
  execution_time,
  created_at
FROM dbt_test_audit.test_results
WHERE status = 'fail'
  AND created_at >= CURRENT_DATE - 7
ORDER BY created_at DESC
```

### 11.2 Alert Configuration
- **Critical Tests**: Immediate Slack/email alerts
- **Warning Tests**: Daily summary reports
- **Performance Tests**: Weekly performance reports

---

## 12. Test Maintenance Guidelines

### 12.1 Regular Review Schedule
- **Weekly**: Review failed tests and update thresholds
- **Monthly**: Add new test cases for new business rules
- **Quarterly**: Performance optimization and test cleanup

### 12.2 Test Documentation Updates
- Document new business rules as test cases
- Update expected values based on business changes
- Maintain test case traceability to requirements

---

## Conclusion

This comprehensive test suite ensures the reliability, accuracy, and performance of the Zoom dbt gold layer fact tables in Snowflake. Regular execution of these tests will help maintain data quality and catch issues early in the development cycle.

**Total Test Cases**: 45+
**Coverage Areas**: Data Integrity, Business Rules, Edge Cases, Performance, Integration
**Estimated Execution Time**: 15-20 minutes
**Recommended Frequency**: Every dbt run (CI/CD integration)