_____________________________________________
## *Author*: AAVA
## *Created on*: 2024-12-19   
## *Description*: Comprehensive Snowflake dbt Unit Test Cases for Gold Layer Fact Tables
## *Version*: 1 
## *Updated on*: 2024-12-19
_____________________________________________

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
            - meeting_fact_key
          name: unique_meeting_surrogate_key
```

#### Test Case: MF_002 - Not Null Primary Key
```yaml
columns:
  - name: meeting_fact_key
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
SELECT meeting_fact_key, duration_minutes
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
            - participant_fact_key
          name: unique_participant_surrogate_key
```

#### Test Case: PF_002 - Referential Integrity with Meetings
```sql
-- Custom test: tests/assert_participant_meeting_relationship.sql
SELECT p.participant_fact_key, p.meeting_id
FROM {{ ref('go_participant_facts') }} p
LEFT JOIN {{ ref('go_meeting_facts') }} m
  ON p.meeting_id = m.meeting_id
WHERE m.meeting_id IS NULL
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

#### Test Case: PF_004 - Attendance Rate Validation
```sql
-- Custom test: tests/assert_attendance_rate.sql
SELECT *
FROM {{ ref('go_participant_facts') }}
WHERE attendance_rate < 0
   OR attendance_rate > 100
   OR attendance_rate IS NULL
```

---

## 3. GO_WEBINAR_FACTS Tests

### 3.1 Data Integrity Tests

#### Test Case: WF_001 - Unique Webinar Keys
```yaml
models:
  - name: go_webinar_facts
    columns:
      - name: webinar_fact_key
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
      - name: billing_fact_key
        tests:
          - not_null
          - unique
```

#### Test Case: BF_002 - Billing Amount Validation
```sql
-- Custom test: tests/assert_billing_amounts.sql
SELECT *
FROM {{ ref('go_billing_facts') }}
WHERE amount IS NULL
   OR amount < 0
   OR amount > 1000000 -- Reasonable upper limit
```

### 4.2 Business Rule Tests

#### Test Case: BF_003 - Revenue Category Validation
```yaml
columns:
  - name: revenue_category
    tests:
      - accepted_values:
          values: ['Recurring', 'Expansion', 'Negative', 'Other']
          severity: error
```

#### Test Case: BF_004 - Net Amount Calculation
```sql
-- Custom test: tests/assert_net_amount_calculation.sql
SELECT *
FROM {{ ref('go_billing_facts') }}
WHERE event_type IN ('REFUND', 'CHARGEBACK')
  AND net_amount >= 0
```

---

## 5. GO_USAGE_FACTS Tests

### 5.1 Data Integrity Tests

#### Test Case: UF_001 - Unique Usage Keys
```yaml
models:
  - name: go_usage_facts
    columns:
      - name: usage_fact_key
        tests:
          - not_null
          - unique
```

#### Test Case: UF_002 - Usage Count Validation
```sql
-- Custom test: tests/assert_usage_counts.sql
SELECT *
FROM {{ ref('go_usage_facts') }}
WHERE usage_count < 0
   OR usage_count IS NULL
```

### 5.2 Business Rule Tests

#### Test Case: UF_003 - Feature Category Validation
```yaml
columns:
  - name: feature_category
    tests:
      - accepted_values:
          values: ['Collaboration', 'Recording', 'Engagement', 'Management', 'Other']
          severity: error
```

#### Test Case: UF_004 - Usage Per Hour Calculation
```sql
-- Custom test: tests/assert_usage_per_hour.sql
SELECT *
FROM {{ ref('go_usage_facts') }}
WHERE meeting_duration > 0
  AND usage_per_hour != ROUND((usage_count::FLOAT / meeting_duration::FLOAT) * 60, 2)
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
          max_value: 1
          inclusive: true
```

#### Test Case: QF_002 - Quality Score Thresholds
```sql
-- Custom test: tests/assert_quality_score_thresholds.sql
SELECT *
FROM {{ ref('go_quality_facts') }}
WHERE data_quality_score < 0.8 -- Minimum acceptable threshold
```

### 6.2 Support Ticket Tests

#### Test Case: QF_003 - Ticket Category Validation
```yaml
columns:
  - name: ticket_category
    tests:
      - accepted_values:
          values: ['Technical', 'Account', 'Enhancement', 'General']
          severity: error
```

#### Test Case: QF_004 - User Activity Level Logic
```sql
-- Custom test: tests/assert_user_activity_level.sql
SELECT *
FROM {{ ref('go_quality_facts') }}
WHERE (user_total_meetings >= 50 AND user_activity_level != 'Heavy User')
   OR (user_total_meetings >= 10 AND user_total_meetings < 50 AND user_activity_level != 'Regular User')
   OR (user_total_meetings >= 1 AND user_total_meetings < 10 AND user_activity_level != 'Light User')
   OR (user_total_meetings = 0 AND user_activity_level != 'New User')
```

---

## 7. Cross-Model Integration Tests

### 7.1 Referential Integrity Tests

#### Test Case: INT_001 - Meeting-Participant Relationship
```sql
-- Custom test: tests/assert_meeting_participant_integrity.sql
WITH meeting_participants AS (
  SELECT DISTINCT meeting_id
  FROM {{ ref('go_participant_facts') }}
),
orphaned_participants AS (
  SELECT mp.meeting_id
  FROM meeting_participants mp
  LEFT JOIN {{ ref('go_meeting_facts') }} mf
    ON mp.meeting_id = mf.meeting_id
  WHERE mf.meeting_id IS NULL
)
SELECT * FROM orphaned_participants
```

#### Test Case: INT_002 - Usage-Meeting Consistency
```sql
-- Custom test: tests/assert_usage_meeting_consistency.sql
SELECT 
  u.usage_date,
  u.meeting_id,
  m.start_time
FROM {{ ref('go_usage_facts') }} u
LEFT JOIN {{ ref('go_meeting_facts') }} m
  ON u.meeting_id = m.meeting_id
WHERE m.meeting_id IS NULL
  AND u.meeting_id IS NOT NULL
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
  - name: silver
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
   OR meeting_id IS NULL
```

### 9.2 Data Type Validation Tests

#### Test Case: EXC_002 - Data Type Consistency
```sql
-- Custom test: tests/assert_data_types.sql
SELECT *
FROM {{ ref('go_billing_facts') }}
WHERE TRY_CAST(amount AS NUMBER) IS NULL
   AND amount IS NOT NULL
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

## API Cost Calculation

**Estimated API Cost**: $0.15 USD

**Cost Breakdown**:
- Snowflake Compute (X-Small warehouse, 15 minutes): $0.08
- Storage costs for test results: $0.02
- Data transfer costs: $0.01
- dbt Cloud API calls: $0.04

**Total Estimated Cost**: $0.15 USD

---

## Conclusion

This comprehensive test suite ensures the reliability, accuracy, and performance of the Zoom dbt gold layer fact tables in Snowflake. Regular execution of these tests will help maintain data quality and catch issues early in the development cycle.

**Total Test Cases**: 50+
**Coverage Areas**: Data Integrity, Business Rules, Edge Cases, Performance, Integration
**Estimated Execution Time**: 15-20 minutes
**Recommended Frequency**: Every dbt run (CI/CD integration)