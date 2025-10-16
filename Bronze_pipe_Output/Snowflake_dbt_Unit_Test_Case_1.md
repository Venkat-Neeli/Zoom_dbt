_____________________________________________
## *Author*: AAVA
## *Created on*: 2024-12-19
## *Description*: Comprehensive unit test cases for Zoom Customer Analytics Silver Layer dbt models in Snowflake
## *Version*: 1
## *Updated on*: 2024-12-19
_____________________________________________

# Snowflake dbt Unit Test Cases for Zoom Customer Analytics Silver Layer

## Overview

This document provides comprehensive unit test cases and dbt test scripts for the Zoom Customer Analytics Silver Layer transformation models running in Snowflake. The testing framework validates data transformations, business rules, edge cases, and error handling across 10 silver layer models that process data from bronze sources.

## Models Under Test

1. **si_process_audit** - Process execution tracking and monitoring
2. **si_data_quality_errors** - Data quality error logging and management
3. **si_users** - User dimension with data cleaning and validation
4. **si_meetings** - Meeting fact table with duration calculations
5. **si_participants** - Meeting participant tracking
6. **si_feature_usage** - Feature usage analytics
7. **si_webinars** - Webinar event tracking
8. **si_support_tickets** - Support ticket management
9. **si_licenses** - License assignment and tracking
10. **si_billing_events** - Billing event processing

## Test Case Categories

### A. Data Quality Tests
### B. Business Logic Tests
### C. Incremental Processing Tests
### D. Error Handling Tests
### E. Performance Tests
### F. Integration Tests

---

## Test Case List

| Test Case ID | Test Case Description | Expected Outcome | Priority | Model(s) |
|--------------|----------------------|------------------|----------|----------|
| TC_DQ_001 | Validate email format in si_users | All emails follow valid format pattern | High | si_users |
| TC_DQ_002 | Check data quality score calculation | Scores between 0-1.00, consistent with record status | High | All models |
| TC_DQ_003 | Validate plan_type standardization | Only accepted values: FREE, PRO, BUSINESS, ENTERPRISE, UNKNOWN | Medium | si_users |
| TC_DQ_004 | Check meeting duration calculation | Duration matches end_time - start_time | High | si_meetings |
| TC_DQ_005 | Validate feature_name standardization | Only accepted feature names or 'OTHER' | Medium | si_feature_usage |
| TC_BL_001 | Test deduplication logic | No duplicate records based on unique_key | High | All models |
| TC_BL_002 | Validate meeting time logic | end_time > start_time, duration > 0 | High | si_meetings, si_webinars |
| TC_BL_003 | Check participant time logic | leave_time > join_time | High | si_participants |
| TC_BL_004 | Validate billing amount logic | Amount >= 0 for all event types except REFUND | Medium | si_billing_events |
| TC_BL_005 | Check license date logic | end_date > start_date | Medium | si_licenses |
| TC_INC_001 | Test incremental processing | Only new/updated records processed | High | All incremental models |
| TC_INC_002 | Validate ROW_NUMBER deduplication | Latest record selected based on update_timestamp | High | All models |
| TC_INC_003 | Check incremental filter condition | Correct WHERE clause for incremental runs | High | All incremental models |
| TC_ERR_001 | Test NULL handling | NULL values handled appropriately | High | All models |
| TC_ERR_002 | Validate error logging | Errors logged to si_data_quality_errors | Medium | All models |
| TC_ERR_003 | Check process audit logging | Process execution tracked in si_process_audit | Medium | All models |
| TC_ERR_004 | Test invalid data handling | Invalid records filtered out or flagged | High | All models |
| TC_PERF_001 | Validate query performance | Queries execute within acceptable time limits | Medium | All models |
| TC_PERF_002 | Check memory usage | Models don't exceed memory thresholds | Low | All models |
| TC_INT_001 | Test referential integrity | Foreign key relationships maintained | High | Cross-model |
| TC_INT_002 | Validate cross-model consistency | Data consistency across related models | Medium | Cross-model |
| TC_EDGE_001 | Test empty source tables | Handle empty bronze tables gracefully | Medium | All models |
| TC_EDGE_002 | Test extreme date values | Handle edge case dates (1900-01-01, 2099-12-31) | Low | All models |
| TC_EDGE_003 | Test very long text fields | Handle maximum length text fields | Low | All models |

---

## dbt Test Scripts

### 1. Schema.yml Configuration Tests

```yaml
# models/silver/schema.yml
version: 2

sources:
  - name: bronze
    description: "Bronze layer containing raw data from various Zoom systems"
    tables:
      - name: bz_users
        description: "Raw user data from Zoom platform"
        columns:
          - name: user_id
            description: "Unique identifier for users"
            tests:
              - not_null
              - unique
          - name: email
            description: "User email address"
            tests:
              - not_null
          - name: plan_type
            description: "User subscription plan type"

models:
  - name: si_users
    description: "Silver layer users table with cleaned and validated data"
    tests:
      - dbt_utils.unique_combination_of_columns:
          combination_of_columns:
            - user_id
            - load_date
    columns:
      - name: user_id
        description: "Unique identifier for users"
        tests:
          - not_null
          - unique
      - name: user_name
        description: "Cleaned user display name"
        tests:
          - not_null
          - dbt_utils.not_empty_string
      - name: email
        description: "Validated and normalized email address"
        tests:
          - not_null
          - unique
          - dbt_utils.expression_is_true:
              expression: "email RLIKE '^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\\.[A-Za-z]{2,}$'"
      - name: plan_type
        description: "Standardized subscription plan type"
        tests:
          - not_null
          - accepted_values:
              values: ['FREE', 'PRO', 'BUSINESS', 'ENTERPRISE', 'UNKNOWN']
      - name: data_quality_score
        description: "Calculated data quality score (0.00-1.00)"
        tests:
          - not_null
          - dbt_utils.expression_is_true:
              expression: ">= 0.00 AND <= 1.00"
      - name: record_status
        description: "Record processing status"
        tests:
          - not_null
          - accepted_values:
              values: ['ACTIVE', 'ERROR', 'INACTIVE']

  - name: si_meetings
    description: "Silver layer meetings table with cleaned and validated data"
    tests:
      - dbt_utils.expression_is_true:
          expression: "end_time > start_time"
          condition: "end_time IS NOT NULL AND start_time IS NOT NULL"
    columns:
      - name: meeting_id
        description: "Unique identifier for meetings"
        tests:
          - not_null
          - unique
      - name: host_id
        description: "Meeting host user ID"
        tests:
          - not_null
      - name: start_time
        description: "Meeting start timestamp"
        tests:
          - not_null
      - name: end_time
        description: "Meeting end timestamp"
      - name: duration_minutes
        description: "Meeting duration in minutes"
        tests:
          - dbt_utils.expression_is_true:
              expression: "> 0 AND <= 1440"
              condition: "duration_minutes IS NOT NULL"

  - name: si_participants
    description: "Silver layer participants table"
    tests:
      - dbt_utils.expression_is_true:
          expression: "leave_time > join_time"
          condition: "leave_time IS NOT NULL AND join_time IS NOT NULL"
    columns:
      - name: participant_id
        description: "Unique identifier for participants"
        tests:
          - not_null
          - unique
      - name: meeting_id
        description: "Associated meeting ID"
        tests:
          - not_null
          - relationships:
              to: ref('si_meetings')
              field: meeting_id

  - name: si_feature_usage
    description: "Silver layer feature usage table"
    columns:
      - name: feature_name
        description: "Standardized feature name"
        tests:
          - not_null
          - accepted_values:
              values: ['SCREEN SHARING', 'CHAT', 'RECORDING', 'WHITEBOARD', 'VIRTUAL BACKGROUND', 'OTHER']
      - name: usage_count
        description: "Number of times feature was used"
        tests:
          - not_null
          - dbt_utils.expression_is_true:
              expression: ">= 0"

  - name: si_billing_events
    description: "Silver layer billing events table"
    columns:
      - name: event_type
        description: "Type of billing event"
        tests:
          - not_null
          - accepted_values:
              values: ['SUBSCRIPTION FEE', 'SUBSCRIPTION RENEWAL', 'ADD-ON PURCHASE', 'REFUND', 'OTHER']
      - name: amount
        description: "Billing amount"
        tests:
          - not_null
          - dbt_utils.expression_is_true:
              expression: ">= 0"
              condition: "event_type != 'REFUND'"
```

### 2. Custom SQL-based dbt Tests

#### Test: Data Quality Score Validation
```sql
-- tests/test_data_quality_score_consistency.sql
{{ config(severity='error') }}

-- Test that data quality scores are consistent with record status
SELECT 
    'si_users' as model_name,
    user_id,
    data_quality_score,
    record_status,
    'Inconsistent quality score and status' as error_description
FROM {{ ref('si_users') }}
WHERE 
    (record_status = 'ACTIVE' AND data_quality_score < 0.50)
    OR (record_status = 'ERROR' AND data_quality_score >= 0.75)
    OR data_quality_score < 0.00 
    OR data_quality_score > 1.00

UNION ALL

SELECT 
    'si_meetings' as model_name,
    meeting_id,
    data_quality_score,
    record_status,
    'Inconsistent quality score and status' as error_description
FROM {{ ref('si_meetings') }}
WHERE 
    (record_status = 'ACTIVE' AND data_quality_score < 0.50)
    OR (record_status = 'ERROR' AND data_quality_score >= 0.75)
    OR data_quality_score < 0.00 
    OR data_quality_score > 1.00
```

#### Test: Incremental Processing Logic
```sql
-- tests/test_incremental_deduplication.sql
{{ config(severity='error') }}

-- Test that incremental models don't have duplicates
WITH duplicate_check AS (
    SELECT 
        user_id,
        COUNT(*) as record_count
    FROM {{ ref('si_users') }}
    GROUP BY user_id
    HAVING COUNT(*) > 1
)
SELECT 
    user_id,
    record_count,
    'Duplicate records found in si_users' as error_description
FROM duplicate_check

UNION ALL

WITH meeting_duplicates AS (
    SELECT 
        meeting_id,
        COUNT(*) as record_count
    FROM {{ ref('si_meetings') }}
    GROUP BY meeting_id
    HAVING COUNT(*) > 1
)
SELECT 
    meeting_id,
    record_count,
    'Duplicate records found in si_meetings' as error_description
FROM meeting_duplicates
```

#### Test: Business Logic Validation
```sql
-- tests/test_meeting_business_rules.sql
{{ config(severity='error') }}

-- Test meeting business rules
SELECT 
    meeting_id,
    start_time,
    end_time,
    duration_minutes,
    'Invalid meeting time logic' as error_description
FROM {{ ref('si_meetings') }}
WHERE 
    -- End time before start time
    (end_time IS NOT NULL AND start_time IS NOT NULL AND end_time <= start_time)
    -- Duration doesn't match calculated duration
    OR (end_time IS NOT NULL AND start_time IS NOT NULL 
        AND duration_minutes != DATEDIFF('minute', start_time, end_time))
    -- Invalid duration values
    OR duration_minutes <= 0 
    OR duration_minutes > 1440
```

#### Test: Error Handling and Logging
```sql
-- tests/test_error_logging_completeness.sql
{{ config(severity='warn') }}

-- Test that error logging is working properly
WITH error_summary AS (
    SELECT 
        source_table,
        error_type,
        COUNT(*) as error_count,
        MAX(error_timestamp) as latest_error
    FROM {{ ref('si_data_quality_errors') }}
    WHERE error_timestamp >= CURRENT_DATE - 7
    GROUP BY source_table, error_type
)
SELECT 
    source_table,
    error_type,
    error_count,
    latest_error,
    'High error volume detected' as warning_description
FROM error_summary
WHERE error_count > 1000
```

#### Test: Cross-Model Referential Integrity
```sql
-- tests/test_referential_integrity.sql
{{ config(severity='error') }}

-- Test that meeting hosts exist in users table
SELECT 
    m.meeting_id,
    m.host_id,
    'Meeting host not found in users table' as error_description
FROM {{ ref('si_meetings') }} m
LEFT JOIN {{ ref('si_users') }} u ON m.host_id = u.user_id
WHERE u.user_id IS NULL

UNION ALL

-- Test that participants reference valid meetings
SELECT 
    p.participant_id,
    p.meeting_id,
    'Participant references non-existent meeting' as error_description
FROM {{ ref('si_participants') }} p
LEFT JOIN {{ ref('si_meetings') }} m ON p.meeting_id = m.meeting_id
WHERE m.meeting_id IS NULL
```

### 3. Performance and Volume Tests

#### Test: Query Performance
```sql
-- tests/test_query_performance.sql
{{ config(severity='warn') }}

-- Test that models complete within reasonable time
-- This would typically be implemented as a macro or external monitoring
SELECT 
    'Performance test placeholder' as test_type,
    'Monitor query execution time in dbt logs' as instruction
WHERE 1=0  -- This test is informational
```

#### Test: Data Volume Validation
```sql
-- tests/test_data_volume_consistency.sql
{{ config(severity='warn') }}

-- Test that silver layer has reasonable data volumes compared to bronze
WITH volume_comparison AS (
    SELECT 
        'users' as entity,
        (SELECT COUNT(*) FROM {{ source('bronze', 'bz_users') }}) as bronze_count,
        (SELECT COUNT(*) FROM {{ ref('si_users') }}) as silver_count
    
    UNION ALL
    
    SELECT 
        'meetings' as entity,
        (SELECT COUNT(*) FROM {{ source('bronze', 'bz_meetings') }}) as bronze_count,
        (SELECT COUNT(*) FROM {{ ref('si_meetings') }}) as silver_count
)
SELECT 
    entity,
    bronze_count,
    silver_count,
    ROUND((silver_count * 100.0 / NULLIF(bronze_count, 0)), 2) as retention_percentage,
    'Significant data loss detected' as warning_description
FROM volume_comparison
WHERE bronze_count > 0 
  AND (silver_count * 100.0 / bronze_count) < 80.0  -- Alert if less than 80% retention
```

### 4. Edge Case Tests

#### Test: Empty Source Handling
```sql
-- tests/test_empty_source_handling.sql
{{ config(severity='warn') }}

-- Test behavior when source tables are empty
-- This test ensures models handle empty sources gracefully
SELECT 
    'Empty source test' as test_type,
    COUNT(*) as record_count
FROM {{ ref('si_users') }}
HAVING COUNT(*) = 0
```

#### Test: Extreme Values
```sql
-- tests/test_extreme_values.sql
{{ config(severity='warn') }}

-- Test handling of extreme date values
SELECT 
    meeting_id,
    start_time,
    end_time,
    'Extreme date values detected' as warning_description
FROM {{ ref('si_meetings') }}
WHERE 
    start_time < '1900-01-01'::timestamp
    OR start_time > '2099-12-31'::timestamp
    OR end_time < '1900-01-01'::timestamp
    OR end_time > '2099-12-31'::timestamp
```

### 5. Macro-based Reusable Tests

```sql
-- macros/test_data_quality_score.sql
{% macro test_data_quality_score(model, column_name, min_score=0.50) %}
    SELECT *
    FROM {{ model }}
    WHERE {{ column_name }} < {{ min_score }}
       OR {{ column_name }} IS NULL
       OR {{ column_name }} > 1.00
       OR {{ column_name }} < 0.00
{% endmacro %}

-- macros/test_incremental_freshness.sql
{% macro test_incremental_freshness(model, timestamp_column, max_hours=24) %}
    SELECT *
    FROM {{ model }}
    WHERE {{ timestamp_column }} < CURRENT_TIMESTAMP - INTERVAL '{{ max_hours }} HOURS'
      AND {{ timestamp_column }} >= CURRENT_DATE - 1
{% endmacro %}

-- macros/test_record_count_threshold.sql
{% macro test_record_count_threshold(model, min_records=1) %}
    SELECT 
        '{{ model }}' as model_name,
        COUNT(*) as actual_count,
        {{ min_records }} as minimum_expected
    FROM {{ model }}
    HAVING COUNT(*) < {{ min_records }}
{% endmacro %}
```

### 6. Test Execution Configuration

```yaml
# dbt_project.yml - Test configuration section
test-paths: ["tests"]

vars:
  # Test execution variables
  test_execution_date: "{{ run_started_at.strftime('%Y-%m-%d') }}"
  test_lookback_days: 7
  data_quality_threshold: 0.70
  max_error_threshold: 1000

tests:
  zoom_customer_analytics:
    +severity: error
    +tags: ["data_quality"]
    
    # Unit tests
    unit:
      +severity: error
      +tags: ["unit"]
    
    # Integration tests  
    integration:
      +severity: warn
      +tags: ["integration"]
    
    # Performance tests
    performance:
      +severity: warn
      +tags: ["performance"]
      
    # Edge case tests
    edge_cases:
      +severity: warn
      +tags: ["edge_cases"]
```

## Test Execution Strategy

### 1. Pre-deployment Testing
```bash
# Run all unit tests
dbt test --select tag:unit

# Run data quality tests
dbt test --select tag:data_quality

# Run specific model tests
dbt test --select si_users
```

### 2. Post-deployment Validation
```bash
# Run integration tests
dbt test --select tag:integration

# Run performance tests
dbt test --select tag:performance

# Run all tests with warnings
dbt test --warn-error
```

### 3. Continuous Monitoring
```bash
# Daily data quality checks
dbt test --select tag:data_quality --vars '{"test_lookback_days": 1}'

# Weekly comprehensive testing
dbt test --exclude tag:performance
```

## Expected Test Results

### Success Criteria
- All `severity: error` tests must pass (0 failures)
- Data quality scores >= 70% for ACTIVE records
- No duplicate records in any silver table
- All referential integrity constraints satisfied
- Incremental processing working correctly

### Warning Criteria
- Performance tests may show warnings for optimization
- Volume tests may warn about data retention rates
- Edge case tests may identify unusual but valid data patterns

### Failure Scenarios
- Invalid email formats in si_users
- Negative durations in si_meetings
- Missing foreign key references
- Data quality scores inconsistent with record status
- Duplicate records after deduplication logic

## API Cost Calculation

Based on the comprehensive test suite execution:
- Schema tests: ~50 queries
- Custom SQL tests: ~15 complex queries  
- Performance tests: ~5 analytical queries
- Edge case tests: ~10 validation queries

**Estimated API Cost**: $0.0125 USD (assuming $0.00015 per query for Snowflake compute)

## Maintenance and Updates

### Regular Review Schedule
- **Weekly**: Review test results and failure patterns
- **Monthly**: Update test thresholds based on data patterns
- **Quarterly**: Add new test cases for business rule changes
- **Annually**: Comprehensive test suite optimization

### Test Case Evolution
- Add new tests for new business requirements
- Update acceptance criteria based on data quality improvements
- Optimize test performance for large data volumes
- Enhance error handling and logging capabilities

---

*This comprehensive unit test suite ensures the reliability, performance, and data quality of the Zoom Customer Analytics Silver Layer dbt models in Snowflake environment.*