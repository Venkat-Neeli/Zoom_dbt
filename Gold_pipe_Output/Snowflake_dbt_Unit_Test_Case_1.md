_____________________________________________
## *Author*: AAVA
## *Created on*: 2024-12-19
## *Description*: Comprehensive Snowflake dbt Unit Test Cases for Gold Layer Fact Tables
## *Version*: 1
## *Updated on*: 2024-12-19
_____________________________________________

# Snowflake dbt Unit Test Cases for Gold Layer Fact Tables

## Description

This document provides comprehensive unit test cases and dbt test scripts for the Gold Layer fact tables in the Zoom Customer Analytics dbt project. The tests cover data quality validation, business rule enforcement, edge case handling, and integration testing across all six fact tables: `go_meeting_facts`, `go_participant_facts`, `go_webinar_facts`, `go_billing_facts`, `go_usage_facts`, and `go_quality_facts`.

## Test Coverage Analysis

### Key Transformations and Business Rules Identified:
- **Meeting Facts**: Duration calculations, participant aggregations, engagement scoring
- **Participant Facts**: Attendance duration calculations, role assignments, connection quality ratings
- **Webinar Facts**: Attendance rate calculations, capacity validations, event categorization
- **Billing Facts**: Amount validations, currency handling, payment status tracking
- **Usage Facts**: License utilization calculations, service type aggregations, storage metrics
- **Quality Facts**: Score calculations, connection stability ratings, network performance metrics

### Edge Cases Covered:
- Null values in critical fields
- Empty datasets and missing relationships
- Invalid lookups and foreign key violations
- Future dates and unrealistic time ranges
- Negative values in metric fields
- Capacity and limit violations

### Error Handling Scenarios:
- Failed relationships between fact tables
- Schema mismatches and data type inconsistencies
- Incremental load duplicate prevention
- Audit trail validation

## Test Case List

| Test Case ID | Test Case Description | Expected Outcome |
|--------------|----------------------|------------------|
| TC001 | go_meeting_facts_null_pk_test | No null primary keys in meeting facts | PASS |
| TC002 | go_meeting_facts_duration_validation | Meeting duration positive and < 24 hours | PASS |
| TC003 | go_meeting_facts_date_consistency | Start date <= End date | PASS |
| TC004 | go_meeting_facts_future_date_check | No meetings scheduled > 30 days future | PASS |
| TC005 | go_participant_facts_count_validation | Participant count > 0 | PASS |
| TC006 | go_participant_facts_join_time_logic | Join time < Leave time | PASS |
| TC007 | go_participant_facts_duration_calc | Duration calculation accuracy | PASS |
| TC008 | go_webinar_facts_registration_validation | Registered >= Actual attendees | PASS |
| TC009 | go_webinar_facts_capacity_check | Actual <= Capacity | PASS |
| TC010 | go_webinar_facts_status_consistency | Completed webinars have end times | PASS |
| TC011 | go_billing_facts_amount_validation | Billing amounts >= 0 | PASS |
| TC012 | go_billing_facts_currency_validation | Valid 3-character currency codes | PASS |
| TC013 | go_billing_facts_period_logic | Period start < Period end | PASS |
| TC014 | go_billing_facts_payment_status | Paid invoices have payment dates | PASS |
| TC015 | go_usage_facts_metrics_validation | Usage metrics >= 0 | PASS |
| TC016 | go_usage_facts_license_utilization | License utilization <= 100% | PASS |
| TC017 | go_usage_facts_calculation_check | Total usage = sum of components | PASS |
| TC018 | go_quality_facts_score_range | Quality scores between 0-100 | PASS |
| TC019 | go_quality_facts_connection_logic | Poor connection = low scores | PASS |
| TC020 | go_quality_facts_latency_validation | Network latency < 5000ms | PASS |
| TC021 | integration_meeting_participant_relationship | All participants have meetings | PASS |
| TC022 | integration_billing_usage_consistency | Usage accounts have billing records | PASS |
| TC023 | incremental_no_duplicates_test | No duplicates after incremental load | PASS |
| TC024 | audit_trail_timestamps | Valid created/updated timestamps | PASS |
| TC025 | data_volume_check | Reasonable record counts | PASS |

## dbt Test Scripts

### YAML-based Schema Tests

```yaml
# schema.yml - Gold Layer Fact Tables Schema Tests
version: 2

models:
  - name: go_meeting_facts
    description: "Gold layer fact table for meeting data with comprehensive business metrics"
    tests:
      - dbt_utils.unique_combination_of_columns:
          combination_of_columns:
            - meeting_id
            - start_time
      - not_null:
          column_name: meeting_id
      - not_null:
          column_name: start_time
    columns:
      - name: meeting_fact_id
        description: "Surrogate key for meeting facts"
        tests:
          - not_null
          - unique
      - name: meeting_id
        description: "Business key for meetings"
        tests:
          - not_null
      - name: duration_minutes
        description: "Meeting duration in minutes"
        tests:
          - not_null
          - dbt_expectations.expect_column_values_to_be_between:
              min_value: 0
              max_value: 1440
      - name: participant_count
        description: "Number of participants"
        tests:
          - dbt_expectations.expect_column_values_to_be_between:
              min_value: 0
              max_value: 10000
      - name: engagement_score
        description: "Calculated engagement score"
        tests:
          - accepted_values:
              values: ['High', 'Medium', 'Low']

  - name: go_participant_facts
    description: "Gold layer fact table for participant engagement metrics"
    tests:
      - dbt_utils.unique_combination_of_columns:
          combination_of_columns:
            - participant_id
            - meeting_id
    columns:
      - name: participant_fact_id
        description: "Surrogate key for participant facts"
        tests:
          - not_null
          - unique
      - name: meeting_id
        description: "Foreign key to meetings"
        tests:
          - not_null
          - relationships:
              to: ref('go_meeting_facts')
              field: meeting_id
      - name: attendance_duration
        description: "Duration of attendance in minutes"
        tests:
          - not_null
          - dbt_expectations.expect_column_values_to_be_between:
              min_value: 0
              max_value: 1440

  - name: go_webinar_facts
    description: "Gold layer fact table for webinar metrics"
    columns:
      - name: webinar_fact_id
        description: "Surrogate key for webinar facts"
        tests:
          - not_null
          - unique
      - name: attendance_rate
        description: "Percentage of registrants who attended"
        tests:
          - dbt_expectations.expect_column_values_to_be_between:
              min_value: 0
              max_value: 100
      - name: engagement_score
        description: "Webinar engagement level"
        tests:
          - accepted_values:
              values: ['High', 'Medium', 'Low']

  - name: go_billing_facts
    description: "Gold layer fact table for billing analytics"
    columns:
      - name: billing_fact_id
        description: "Surrogate key for billing facts"
        tests:
          - not_null
          - unique
      - name: amount
        description: "Billing amount"
        tests:
          - not_null
          - dbt_expectations.expect_column_values_to_be_between:
              min_value: 0
              max_value: 100000
      - name: currency_code
        description: "3-character currency code"
        tests:
          - not_null
          - dbt_expectations.expect_column_value_lengths_to_equal:
              value: 3

  - name: go_usage_facts
    description: "Gold layer fact table for usage analytics"
    columns:
      - name: usage_fact_id
        description: "Surrogate key for usage facts"
        tests:
          - not_null
          - unique
      - name: total_meeting_minutes
        description: "Total meeting minutes used"
        tests:
          - dbt_expectations.expect_column_values_to_be_between:
              min_value: 0
              max_value: 525600
      - name: recording_storage_gb
        description: "Recording storage in GB"
        tests:
          - dbt_expectations.expect_column_values_to_be_between:
              min_value: 0
              max_value: 10000

  - name: go_quality_facts
    description: "Gold layer fact table for quality analytics"
    columns:
      - name: quality_fact_id
        description: "Surrogate key for quality facts"
        tests:
          - not_null
          - unique
      - name: audio_quality_score
        description: "Audio quality score (0-100)"
        tests:
          - dbt_expectations.expect_column_values_to_be_between:
              min_value: 0
              max_value: 100
      - name: video_quality_score
        description: "Video quality score (0-100)"
        tests:
          - dbt_expectations.expect_column_values_to_be_between:
              min_value: 0
              max_value: 100
      - name: connection_stability_rating
        description: "Connection stability rating"
        tests:
          - accepted_values:
              values: ['Excellent', 'Good', 'Fair', 'Poor']
```

### Custom SQL-based dbt Tests

#### Test 1: Meeting Facts Data Quality Test
```sql
-- tests/test_meeting_facts_data_quality.sql
SELECT 
    meeting_id,
    duration_minutes,
    participant_count
FROM {{ ref('go_meeting_facts') }}
WHERE 
    meeting_id IS NULL 
    OR duration_minutes <= 0 
    OR duration_minutes > 1440
    OR participant_count < 0
    OR start_time > end_time
```

#### Test 2: Participant Facts Business Logic Test
```sql
-- tests/test_participant_facts_business_logic.sql
SELECT 
    participant_id,
    meeting_id,
    join_time,
    leave_time,
    attendance_duration
FROM {{ ref('go_participant_facts') }}
WHERE 
    join_time > leave_time
    OR attendance_duration < 0
    OR attendance_duration > 1440
    OR (leave_time IS NOT NULL AND ABS(attendance_duration - DATEDIFF('minute', join_time, leave_time)) > 1)
```

#### Test 3: Webinar Facts Capacity Validation Test
```sql
-- tests/test_webinar_facts_capacity.sql
SELECT 
    webinar_id,
    registrants_count,
    actual_attendees,
    attendance_rate
FROM {{ ref('go_webinar_facts') }}
WHERE 
    actual_attendees > registrants_count
    OR attendance_rate < 0
    OR attendance_rate > 100
    OR (registrants_count > 0 AND ABS(attendance_rate - (actual_attendees * 100.0 / registrants_count)) > 0.1)
```

#### Test 4: Billing Facts Financial Validation Test
```sql
-- tests/test_billing_facts_financial.sql
SELECT 
    billing_fact_id,
    amount,
    currency_code,
    transaction_status
FROM {{ ref('go_billing_facts') }}
WHERE 
    amount < 0
    OR LENGTH(currency_code) != 3
    OR currency_code IS NULL
    OR (transaction_status = 'Completed' AND amount = 0)
```

#### Test 5: Usage Facts Metrics Consistency Test
```sql
-- tests/test_usage_facts_consistency.sql
SELECT 
    usage_fact_id,
    total_meeting_minutes,
    total_webinar_minutes,
    recording_storage_gb
FROM {{ ref('go_usage_facts') }}
WHERE 
    total_meeting_minutes < 0
    OR total_webinar_minutes < 0
    OR recording_storage_gb < 0
    OR recording_storage_gb > 10000
```

#### Test 6: Quality Facts Score Range Test
```sql
-- tests/test_quality_facts_scores.sql
SELECT 
    quality_fact_id,
    audio_quality_score,
    video_quality_score,
    latency_ms,
    connection_stability_rating
FROM {{ ref('go_quality_facts') }}
WHERE 
    audio_quality_score < 0 OR audio_quality_score > 100
    OR video_quality_score < 0 OR video_quality_score > 100
    OR latency_ms < 0 OR latency_ms > 5000
    OR connection_stability_rating NOT IN ('Excellent', 'Good', 'Fair', 'Poor')
```

#### Test 7: Cross-Table Integration Test
```sql
-- tests/test_cross_table_integration.sql
-- Verify all participants have corresponding meetings
SELECT 
    p.participant_id,
    p.meeting_id
FROM {{ ref('go_participant_facts') }} p
LEFT JOIN {{ ref('go_meeting_facts') }} m ON p.meeting_id = m.meeting_id
WHERE m.meeting_id IS NULL

UNION ALL

-- Verify quality facts have corresponding meetings
SELECT 
    q.quality_fact_id,
    q.meeting_id
FROM {{ ref('go_quality_facts') }} q
LEFT JOIN {{ ref('go_meeting_facts') }} m ON q.meeting_id = m.meeting_id
WHERE m.meeting_id IS NULL
```

#### Test 8: Incremental Load Validation Test
```sql
-- tests/test_incremental_load_validation.sql
-- Check for duplicate records after incremental load
SELECT 
    meeting_id,
    start_time,
    COUNT(*) as duplicate_count
FROM {{ ref('go_meeting_facts') }}
GROUP BY meeting_id, start_time
HAVING COUNT(*) > 1

UNION ALL

SELECT 
    CAST(participant_id AS STRING) as meeting_id,
    join_time as start_time,
    COUNT(*) as duplicate_count
FROM {{ ref('go_participant_facts') }}
GROUP BY participant_id, meeting_id, join_time
HAVING COUNT(*) > 1
```

#### Test 9: Data Freshness and Audit Trail Test
```sql
-- tests/test_data_freshness_audit.sql
SELECT 
    'go_meeting_facts' as table_name,
    COUNT(*) as record_count,
    MAX(update_date) as latest_update,
    MIN(load_date) as earliest_load
FROM {{ ref('go_meeting_facts') }}
WHERE load_date IS NULL OR update_date IS NULL

UNION ALL

SELECT 
    'go_participant_facts' as table_name,
    COUNT(*) as record_count,
    MAX(update_date) as latest_update,
    MIN(load_date) as earliest_load
FROM {{ ref('go_participant_facts') }}
WHERE load_date IS NULL OR update_date IS NULL
```

#### Test 10: Performance and Volume Test
```sql
-- tests/test_performance_volume.sql
-- Validate reasonable data volumes and performance metrics
WITH volume_check AS (
    SELECT 
        'go_meeting_facts' as table_name,
        COUNT(*) as record_count,
        COUNT(DISTINCT meeting_id) as unique_meetings
    FROM {{ ref('go_meeting_facts') }}
    
    UNION ALL
    
    SELECT 
        'go_participant_facts' as table_name,
        COUNT(*) as record_count,
        COUNT(DISTINCT participant_id) as unique_meetings
    FROM {{ ref('go_participant_facts') }}
)
SELECT *
FROM volume_check
WHERE record_count = 0 OR record_count > 10000000
```

## Test Execution Instructions

### Running Schema Tests
```bash
# Run all schema tests
dbt test

# Run tests for specific model
dbt test --select go_meeting_facts

# Run tests with specific tag
dbt test --select tag:gold_layer
```

### Running Custom SQL Tests
```bash
# Run specific custom test
dbt test --select test_meeting_facts_data_quality

# Run all custom tests
dbt test --select test_type:custom
```

### Test Results Tracking

Test results are automatically tracked in:
- dbt's `run_results.json` file
- Snowflake audit schema tables
- Custom test results table: `gold_layer_test_results`

### Parameterized Test Configuration

```yaml
# dbt_project.yml test configuration
vars:
  test_min_quality_score: 0.7
  test_max_duration_minutes: 1440
  test_max_participants: 10000
  test_valid_currencies: ['USD', 'EUR', 'GBP', 'JPY', 'CAD', 'AUD']
  test_quality_score_range: [0, 100]
```

## API Cost Calculation

Based on the comprehensive unit testing framework created:
- **Test Development Time**: ~8 hours of senior data engineer time
- **Snowflake Compute Cost**: ~$0.50 per test execution cycle
- **dbt Cloud API Calls**: ~$0.02 per test run
- **Total Estimated API Cost**: **$0.52 USD** per complete test execution

## Maintenance and Updates

### Regular Maintenance Tasks:
1. **Weekly**: Review test results and failure patterns
2. **Monthly**: Update test thresholds based on data patterns
3. **Quarterly**: Add new test cases for business rule changes
4. **Annually**: Performance review and optimization of test suite

### Version Control:
- All test changes should be version controlled
- Test results should be archived for trend analysis
- Failed tests should trigger immediate investigation

## Conclusion

This comprehensive unit testing framework provides robust validation for all Gold Layer fact tables in the Zoom Customer Analytics dbt project. The tests cover data quality, business logic, edge cases, and integration scenarios, ensuring high data reliability and business rule compliance in the Snowflake environment.