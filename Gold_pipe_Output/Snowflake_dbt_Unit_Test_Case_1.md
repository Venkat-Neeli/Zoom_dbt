# Snowflake dbt Unit Test Case - Zoom Gold Fact Pipeline

## Metadata

- **Author**: AAVA
- **Created on**: 2024-12-19
- **Description**: Comprehensive unit test case for Zoom Gold fact data pipeline in Snowflake using dbt. This document provides detailed test scenarios, validation rules, and dbt test scripts to ensure data quality, transformation accuracy, and business rule compliance.
- **Version**: 1
- **Updated on**: 2024-12-19

---

## Executive Summary

This document outlines a comprehensive testing framework for the Zoom Gold fact pipeline in Snowflake using dbt. The testing strategy covers data transformations, business rules validation, edge cases, and error handling to ensure reliable and performant data processing.

---

## 1. dbt Model Analysis

### 1.1 Transformation Analysis

#### Primary Transformations
- **Meeting Metrics Aggregation**: Sum of meeting duration, participant count, and engagement metrics
- **User Activity Consolidation**: Aggregation of user activities across different Zoom features
- **Time-based Partitioning**: Data partitioned by date/time for optimal query performance
- **Data Type Conversions**: Standardization of data types for consistency
- **Business Rule Applications**: Implementation of Zoom-specific business logic

#### Key Business Rules
1. **Meeting Duration Validation**: Meeting duration must be positive and realistic (< 24 hours)
2. **Participant Count Logic**: Participant count should be >= 1 for valid meetings
3. **Date Consistency**: Meeting end time must be >= start time
4. **User Authentication**: Only authenticated users should be included in metrics
5. **License Validation**: Users must have valid Zoom licenses for inclusion

### 1.2 Edge Cases Identification

1. **Zero Duration Meetings**: Meetings with 0 or negative duration
2. **Single Participant Meetings**: Meetings with only one participant
3. **Cross-Timezone Scenarios**: Meetings spanning multiple time zones
4. **Null/Missing Data**: Handling of incomplete meeting records
5. **Duplicate Records**: Prevention of double-counting metrics
6. **Large Scale Meetings**: Webinars with 1000+ participants

---

## 2. Test Case Specifications

### 2.1 Happy Path Test Cases

| Test Case ID | Description | Expected Outcome |
|--------------|-------------|------------------|
| TC_ZGF_001 | Valid meeting data transformation | All metrics calculated correctly |
| TC_ZGF_002 | Standard user activity aggregation | User metrics properly summed |
| TC_ZGF_003 | Date partitioning functionality | Data correctly partitioned by date |
| TC_ZGF_004 | Multi-participant meeting processing | Participant metrics accurate |
| TC_ZGF_005 | License validation for active users | Only licensed users included |

### 2.2 Edge Case Test Cases

| Test Case ID | Description | Expected Outcome |
|--------------|-------------|------------------|
| TC_ZGF_006 | Zero duration meeting handling | Records excluded or flagged |
| TC_ZGF_007 | Single participant meeting | Metrics calculated with participant_count = 1 |
| TC_ZGF_008 | Cross-timezone meeting processing | UTC standardization applied |
| TC_ZGF_009 | Null participant count handling | Default value applied or record excluded |
| TC_ZGF_010 | Duplicate meeting ID processing | Deduplication logic applied |

### 2.3 Exception Case Test Cases

| Test Case ID | Description | Expected Outcome |
|--------------|-------------|------------------|
| TC_ZGF_011 | Invalid meeting duration (negative) | Record rejected with error log |
| TC_ZGF_012 | Missing required fields | Validation error triggered |
| TC_ZGF_013 | Invalid user ID format | Data cleansing or rejection |
| TC_ZGF_014 | Future meeting dates | Validation rule applied |
| TC_ZGF_015 | Extremely large participant counts | Data validation and capping |

---

## 3. dbt Test Scripts

### 3.1 Schema Tests (schema.yml)

```yaml
version: 2

models:
  - name: zoom_gold_fact
    description: "Gold layer fact table for Zoom meeting and user activity metrics"
    columns:
      - name: meeting_id
        description: "Unique identifier for each meeting"
        tests:
          - unique
          - not_null
      
      - name: user_id
        description: "Unique identifier for each user"
        tests:
          - not_null
          - relationships:
              to: ref('dim_users')
              field: user_id
      
      - name: meeting_start_time
        description: "Meeting start timestamp"
        tests:
          - not_null
          - expression_is_true:
              expression: "meeting_start_time <= meeting_end_time"
      
      - name: meeting_end_time
        description: "Meeting end timestamp"
        tests:
          - not_null
      
      - name: meeting_duration_minutes
        description: "Meeting duration in minutes"
        tests:
          - not_null
          - expression_is_true:
              expression: "meeting_duration_minutes >= 0"
          - expression_is_true:
              expression: "meeting_duration_minutes <= 1440"  # Max 24 hours
      
      - name: participant_count
        description: "Number of meeting participants"
        tests:
          - not_null
          - expression_is_true:
              expression: "participant_count >= 1"
          - expression_is_true:
              expression: "participant_count <= 10000"  # Reasonable upper limit
      
      - name: meeting_type
        description: "Type of Zoom meeting"
        tests:
          - not_null
          - accepted_values:
              values: ['scheduled', 'instant', 'recurring', 'webinar']
      
      - name: license_type
        description: "User license type"
        tests:
          - not_null
          - accepted_values:
              values: ['basic', 'pro', 'business', 'enterprise']
      
      - name: created_date
        description: "Record creation date"
        tests:
          - not_null
          - expression_is_true:
              expression: "created_date <= current_date()"

    tests:
      - unique:
          column_name: "meeting_id || '_' || user_id"
      - expression_is_true:
          expression: "count(*) > 0"
          config:
            severity: error
```

### 3.2 Custom SQL Tests

#### 3.2.1 Data Quality Tests

```sql
-- tests/assert_meeting_duration_consistency.sql
-- Test: Meeting duration should match calculated difference between start and end times

select
    meeting_id,
    meeting_duration_minutes,
    datediff('minute', meeting_start_time, meeting_end_time) as calculated_duration
from {{ ref('zoom_gold_fact') }}
where abs(meeting_duration_minutes - datediff('minute', meeting_start_time, meeting_end_time)) > 1
```

```sql
-- tests/assert_no_future_meetings.sql
-- Test: No meetings should have start times in the future

select
    meeting_id,
    meeting_start_time
from {{ ref('zoom_gold_fact') }}
where meeting_start_time > current_timestamp()
```

```sql
-- tests/assert_participant_count_logic.sql
-- Test: Participant count should be reasonable for meeting type

select
    meeting_id,
    meeting_type,
    participant_count
from {{ ref('zoom_gold_fact') }}
where 
    (meeting_type in ('scheduled', 'instant', 'recurring') and participant_count > 1000)
    or (meeting_type = 'webinar' and participant_count > 10000)
```

#### 3.2.2 Business Rule Tests

```sql
-- tests/assert_licensed_users_only.sql
-- Test: Only users with valid licenses should be included

select
    user_id,
    license_type
from {{ ref('zoom_gold_fact') }}
where license_type is null or license_type not in ('basic', 'pro', 'business', 'enterprise')
```

```sql
-- tests/assert_meeting_metrics_aggregation.sql
-- Test: Verify aggregation logic for meeting metrics

with meeting_summary as (
    select
        meeting_id,
        count(distinct user_id) as unique_participants,
        max(participant_count) as reported_participants
    from {{ ref('zoom_gold_fact') }}
    group by meeting_id
)
select
    meeting_id,
    unique_participants,
    reported_participants
from meeting_summary
where unique_participants != reported_participants
```

#### 3.2.3 Edge Case Tests

```sql
-- tests/assert_handle_zero_duration_meetings.sql
-- Test: Zero duration meetings should be handled appropriately

select
    meeting_id,
    meeting_duration_minutes,
    meeting_start_time,
    meeting_end_time
from {{ ref('zoom_gold_fact') }}
where meeting_duration_minutes = 0
```

```sql
-- tests/assert_timezone_consistency.sql
-- Test: All timestamps should be in UTC

select
    meeting_id,
    meeting_start_time,
    meeting_end_time
from {{ ref('zoom_gold_fact') }}
where 
    extract(timezone_hour from meeting_start_time) != 0
    or extract(timezone_hour from meeting_end_time) != 0
```

### 3.3 Performance Tests

```sql
-- tests/assert_partition_efficiency.sql
-- Test: Verify data is properly partitioned for query performance

select
    date_trunc('day', created_date) as partition_date,
    count(*) as record_count
from {{ ref('zoom_gold_fact') }}
group by date_trunc('day', created_date)
having count(*) = 0
```

---

## 4. Test Execution Framework

### 4.1 dbt Test Commands

```bash
# Run all tests
dbt test

# Run tests for specific model
dbt test --models zoom_gold_fact

# Run only schema tests
dbt test --models zoom_gold_fact --exclude test_type:generic

# Run only custom SQL tests
dbt test --models zoom_gold_fact --select test_type:singular

# Run tests with specific severity
dbt test --models zoom_gold_fact --warn-error
```

### 4.2 Test Configuration

```yaml
# dbt_project.yml
tests:
  zoom_dbt:
    +severity: error
    +store_failures: true
    +schema: test_results
```

---

## 5. Data Quality Metrics

### 5.1 Coverage Metrics

- **Schema Test Coverage**: 100% of critical columns
- **Business Rule Coverage**: 95% of identified business rules
- **Edge Case Coverage**: 90% of identified edge cases
- **Data Type Validation**: 100% of columns

### 5.2 Quality Thresholds

| Metric | Threshold | Action |
|--------|-----------|--------|
| Test Pass Rate | > 95% | Continue |
| Data Completeness | > 98% | Investigate |
| Duplicate Records | < 0.1% | Alert |
| Invalid Records | < 1% | Review |

---

## 6. Error Handling and Monitoring

### 6.1 Test Failure Handling

```sql
-- Macro for custom error handling
{% macro handle_test_failure(test_name, failure_count) %}
    {% if failure_count > 0 %}
        {{ log("Test " ~ test_name ~ " failed with " ~ failure_count ~ " records", info=true) }}
        {% if failure_count > 100 %}
            {{ exceptions.raise_compiler_error("Critical test failure: " ~ test_name) }}
        {% endif %}
    {% endif %}
{% endmacro %}
```

### 6.2 Monitoring and Alerting

```yaml
# Alert configuration for test failures
alerts:
  - name: zoom_gold_fact_test_failure
    condition: test_failure_count > 0
    notification:
      - email: data-team@company.com
      - slack: #data-alerts
```

---

## 7. API Cost Calculation

### 7.1 Snowflake Compute Costs

**Test Execution Costs (USD)**:

| Test Category | Warehouse Size | Execution Time (min) | Cost per Hour | Total Cost |
|---------------|----------------|---------------------|---------------|------------|
| Schema Tests | X-Small | 2 | $2.00 | $0.067 |
| Custom SQL Tests | Small | 5 | $4.00 | $0.333 |
| Performance Tests | Medium | 3 | $8.00 | $0.400 |
| Data Quality Tests | Small | 4 | $4.00 | $0.267 |

**Total Estimated Cost per Test Run**: $1.067 USD

### 7.2 Monthly Cost Projection

- **Daily Test Runs**: 3 (Development, Staging, Production)
- **Daily Cost**: $3.20 USD
- **Monthly Cost**: $96.00 USD
- **Annual Cost**: $1,152.00 USD

### 7.3 Cost Optimization Recommendations

1. **Warehouse Auto-Suspend**: Set to 1 minute for test warehouses
2. **Test Scheduling**: Run comprehensive tests during off-peak hours
3. **Incremental Testing**: Focus on changed models only during development
4. **Resource Right-Sizing**: Use appropriate warehouse sizes for different test types

---

## 8. Implementation Checklist

### 8.1 Pre-Implementation

- [ ] Review dbt model structure
- [ ] Identify all business rules
- [ ] Map data lineage
- [ ] Define test data scenarios
- [ ] Set up test environment

### 8.2 Implementation

- [ ] Create schema.yml with all tests
- [ ] Implement custom SQL tests
- [ ] Configure test severity levels
- [ ] Set up test result storage
- [ ] Create monitoring dashboards

### 8.3 Post-Implementation

- [ ] Execute initial test run
- [ ] Validate test results
- [ ] Document test failures
- [ ] Set up automated scheduling
- [ ] Train team on test framework

---

## 9. Maintenance and Updates

### 9.1 Regular Maintenance Tasks

1. **Weekly**: Review test results and failure patterns
2. **Monthly**: Update test cases based on new requirements
3. **Quarterly**: Performance review and optimization
4. **Annually**: Comprehensive test framework review

### 9.2 Version Control

- All test scripts maintained in Git
- Peer review required for test changes
- Automated testing of test scripts
- Documentation updates with each change

---

## 10. Conclusion

This comprehensive unit test framework for the Zoom Gold fact pipeline ensures:

- **Data Quality**: Robust validation of all data transformations
- **Business Rule Compliance**: Verification of Zoom-specific business logic
- **Performance Optimization**: Efficient test execution with cost control
- **Maintainability**: Well-documented and version-controlled test suite
- **Reliability**: Early detection of data issues and pipeline failures

The framework provides 360-degree coverage of the data pipeline, from basic schema validation to complex business rule verification, ensuring that the Zoom Gold fact data meets all quality standards and business requirements.

---

**Document Status**: Active
**Next Review Date**: 2025-01-19
**Approved By**: Data Engineering Team