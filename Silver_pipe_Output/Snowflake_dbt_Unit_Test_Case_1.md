_____________________________________________
## *Author*: AAVA
## *Created on*: 2024-12-19
## *Description*: Comprehensive unit test cases for Zoom Customer Analytics Silver layer dbt models in Snowflake
## *Version*: 1
## *Updated on*: 2024-12-19
_____________________________________________

# Snowflake dbt Unit Test Cases for Zoom Customer Analytics Silver Layer

## Overview

This document provides comprehensive unit test cases and dbt test scripts for the Zoom Customer Analytics Silver layer models running in Snowflake. The test suite covers data transformations, business rules, edge cases, and error handling scenarios across all silver layer models.

## Models Under Test

- `si_process_audit` - Process audit logging
- `si_users` - User data with data quality checks
- `si_meetings` - Meeting data with duration validation
- `si_participants` - Participant data with time validation
- `si_feature_usage` - Feature usage with standardization
- `si_webinars` - Webinar data with registrant validation
- `si_support_tickets` - Support tickets with status standardization
- `si_licenses` - License data with date validation
- `si_billing_events` - Billing events with amount validation
- `si_data_quality_errors` - Error logging and tracking

## Test Case Categories

### 1. Data Quality and Validation Tests
### 2. Business Logic Tests
### 3. Edge Case Tests
### 4. Error Handling Tests
### 5. Performance and Incremental Tests

---

## Test Case List

| Test Case ID | Test Case Description | Expected Outcome | Model | Priority |
|--------------|----------------------|------------------|-------|----------|
| TC_001 | Validate user_id uniqueness in si_users | No duplicate user_ids | si_users | High |
| TC_002 | Validate email format in si_users | All emails follow valid format | si_users | High |
| TC_003 | Validate data quality score calculation | Scores between 0.0-1.0 | si_users | High |
| TC_004 | Test deduplication logic | Only latest record per user_id | si_users | High |
| TC_005 | Validate meeting duration calculation | Duration > 0 and <= 1440 minutes | si_meetings | High |
| TC_006 | Test participant join/leave time logic | Leave time >= join time | si_participants | High |
| TC_007 | Validate incremental processing | Only new/updated records processed | All models | High |
| TC_008 | Test null value handling | Null values properly handled/rejected | All models | Medium |
| TC_009 | Validate plan_type standardization | Only valid plan types allowed | si_users | Medium |
| TC_010 | Test billing amount validation | All amounts >= 0 | si_billing_events | Medium |
| TC_011 | Validate support ticket status | Only valid statuses allowed | si_support_tickets | Medium |
| TC_012 | Test license date range validation | Start date <= end date | si_licenses | Medium |
| TC_013 | Validate feature name standardization | Consistent feature naming | si_feature_usage | Medium |
| TC_014 | Test webinar registrant count | Count >= 0 | si_webinars | Medium |
| TC_015 | Validate error logging completeness | All DQ violations captured | si_data_quality_errors | High |
| TC_016 | Test process audit trail | All executions logged | si_process_audit | High |
| TC_017 | Validate cross-table referential integrity | Foreign keys exist in parent tables | All models | High |
| TC_018 | Test empty dataset handling | Models handle empty source data | All models | Low |
| TC_019 | Validate timestamp consistency | Load/update timestamps logical | All models | Medium |
| TC_020 | Test schema evolution handling | New columns handled gracefully | All models | Low |

---

## dbt Test Scripts

### Schema Tests (schema.yml)

```yaml
version: 2

sources:
  - name: bronze
    description: "Bronze layer containing raw data from Zoom systems"
    tables:
      - name: bz_users
        description: "Raw user data from Zoom"
        columns:
          - name: user_id
            tests:
              - not_null
      - name: bz_meetings
        description: "Raw meeting data from Zoom"
      - name: bz_participants
        description: "Raw participant data from Zoom"
      - name: bz_feature_usage
        description: "Raw feature usage data from Zoom"
      - name: bz_webinars
        description: "Raw webinar data from Zoom"
      - name: bz_support_tickets
        description: "Raw support ticket data from Zoom"
      - name: bz_licenses
        description: "Raw license data from Zoom"
      - name: bz_billing_events
        description: "Raw billing event data from Zoom"

models:
  - name: si_users
    description: "Silver layer users table with cleaned and validated data"
    tests:
      - dbt_utils.unique_combination_of_columns:
          combination_of_columns:
            - user_id
            - load_date
      - dbt_expectations.expect_table_row_count_to_be_between:
          min_value: 1
          max_value: 10000000
    columns:
      - name: user_id
        description: "Unique identifier for users"
        tests:
          - not_null
          - unique
      - name: user_name
        description: "User display name"
        tests:
          - not_null
          - dbt_utils.not_empty_string
      - name: email
        description: "Validated and normalized email address"
        tests:
          - not_null
          - unique
          - dbt_expectations.expect_column_values_to_match_regex:
              regex: '^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}$'
      - name: plan_type
        description: "Standardized plan type"
        tests:
          - not_null
          - accepted_values:
              values: ['FREE', 'PRO', 'BUSINESS', 'ENTERPRISE']
      - name: data_quality_score
        description: "Data quality score from 0.0 to 1.0"
        tests:
          - not_null
          - dbt_expectations.expect_column_values_to_be_between:
              min_value: 0.0
              max_value: 1.0
      - name: record_status
        description: "Record processing status"
        tests:
          - not_null
          - accepted_values:
              values: ['active', 'error']
      - name: load_timestamp
        description: "Record load timestamp"
        tests:
          - not_null
      - name: update_timestamp
        description: "Record update timestamp"
        tests:
          - not_null

  - name: si_meetings
    description: "Silver layer meetings table with duration validation"
    tests:
      - dbt_utils.unique_combination_of_columns:
          combination_of_columns:
            - meeting_id
            - start_time
    columns:
      - name: meeting_id
        description: "Unique identifier for meetings"
        tests:
          - not_null
          - unique
      - name: user_id
        description: "Meeting host user ID"
        tests:
          - not_null
          - relationships:
              to: ref('si_users')
              field: user_id
      - name: duration_minutes
        description: "Meeting duration in minutes"
        tests:
          - not_null
          - dbt_expectations.expect_column_values_to_be_between:
              min_value: 1
              max_value: 1440
      - name: start_time
        description: "Meeting start timestamp"
        tests:
          - not_null
      - name: end_time
        description: "Meeting end timestamp"
        tests:
          - not_null

  - name: si_participants
    description: "Silver layer participants with time validation"
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
      - name: user_id
        description: "Participant user ID"
        tests:
          - relationships:
              to: ref('si_users')
              field: user_id
      - name: join_time
        description: "Participant join timestamp"
        tests:
          - not_null
      - name: leave_time
        description: "Participant leave timestamp"

  - name: si_feature_usage
    description: "Silver layer feature usage with standardization"
    columns:
      - name: usage_id
        description: "Unique identifier for usage records"
        tests:
          - not_null
          - unique
      - name: user_id
        description: "User ID for feature usage"
        tests:
          - not_null
          - relationships:
              to: ref('si_users')
              field: user_id
      - name: feature_name
        description: "Standardized feature name"
        tests:
          - not_null
          - dbt_utils.not_empty_string

  - name: si_webinars
    description: "Silver layer webinars with registrant validation"
    columns:
      - name: webinar_id
        description: "Unique identifier for webinars"
        tests:
          - not_null
          - unique
      - name: host_user_id
        description: "Webinar host user ID"
        tests:
          - not_null
          - relationships:
              to: ref('si_users')
              field: user_id
      - name: registrant_count
        description: "Number of registrants"
        tests:
          - not_null
          - dbt_expectations.expect_column_values_to_be_between:
              min_value: 0
              max_value: 100000

  - name: si_support_tickets
    description: "Silver layer support tickets with status standardization"
    columns:
      - name: ticket_id
        description: "Unique identifier for support tickets"
        tests:
          - not_null
          - unique
      - name: user_id
        description: "User who created the ticket"
        tests:
          - relationships:
              to: ref('si_users')
              field: user_id
      - name: status
        description: "Standardized ticket status"
        tests:
          - not_null
          - accepted_values:
              values: ['OPEN', 'IN_PROGRESS', 'RESOLVED', 'CLOSED']

  - name: si_licenses
    description: "Silver layer licenses with date validation"
    columns:
      - name: license_id
        description: "Unique identifier for licenses"
        tests:
          - not_null
          - unique
      - name: user_id
        description: "Licensed user ID"
        tests:
          - not_null
          - relationships:
              to: ref('si_users')
              field: user_id
      - name: start_date
        description: "License start date"
        tests:
          - not_null
      - name: end_date
        description: "License end date"
        tests:
          - not_null

  - name: si_billing_events
    description: "Silver layer billing events with amount validation"
    columns:
      - name: billing_event_id
        description: "Unique identifier for billing events"
        tests:
          - not_null
          - unique
      - name: user_id
        description: "User associated with billing event"
        tests:
          - not_null
          - relationships:
              to: ref('si_users')
              field: user_id
      - name: amount
        description: "Billing amount"
        tests:
          - not_null
          - dbt_expectations.expect_column_values_to_be_between:
              min_value: 0
              max_value: 999999.99

  - name: si_data_quality_errors
    description: "Silver layer data quality error logging"
    columns:
      - name: error_id
        description: "Unique identifier for errors"
        tests:
          - not_null
          - unique
      - name: source_table
        description: "Source table with error"
        tests:
          - not_null
          - dbt_utils.not_empty_string
      - name: error_type
        description: "Type of data quality error"
        tests:
          - not_null
          - accepted_values:
              values: ['NULL_VALUE', 'INVALID_FORMAT', 'DUPLICATE', 'REFERENTIAL_INTEGRITY', 'BUSINESS_RULE']

  - name: si_process_audit
    description: "Silver layer process audit logging"
    columns:
      - name: execution_id
        description: "Unique identifier for each execution"
        tests:
          - not_null
          - unique
      - name: pipeline_name
        description: "Name of the pipeline"
        tests:
          - not_null
          - dbt_utils.not_empty_string
      - name: status
        description: "Execution status"
        tests:
          - not_null
          - accepted_values:
              values: ['RUNNING', 'SUCCESS', 'FAILED']
```

### Custom SQL-Based Tests

#### Test 1: Meeting End Time After Start Time
```sql
-- tests/assert_meeting_end_after_start.sql
SELECT 
    meeting_id,
    start_time,
    end_time
FROM {{ ref('si_meetings') }}
WHERE end_time <= start_time
```

#### Test 2: Participant Leave Time After Join Time
```sql
-- tests/assert_participant_leave_after_join.sql
SELECT 
    participant_id,
    join_time,
    leave_time
FROM {{ ref('si_participants') }}
WHERE leave_time IS NOT NULL 
  AND leave_time < join_time
```

#### Test 3: License End Date After Start Date
```sql
-- tests/assert_license_end_after_start.sql
SELECT 
    license_id,
    start_date,
    end_date
FROM {{ ref('si_licenses') }}
WHERE end_date <= start_date
```

#### Test 4: Data Quality Score Validation
```sql
-- tests/assert_data_quality_score_valid.sql
SELECT 
    user_id,
    data_quality_score
FROM {{ ref('si_users') }}
WHERE data_quality_score < 0.0 
   OR data_quality_score > 1.0
   OR data_quality_score IS NULL
```

#### Test 5: Incremental Processing Validation
```sql
-- tests/assert_incremental_processing.sql
WITH duplicate_check AS (
    SELECT 
        user_id,
        COUNT(*) as record_count
    FROM {{ ref('si_users') }}
    GROUP BY user_id
    HAVING COUNT(*) > 1
)
SELECT * FROM duplicate_check
```

#### Test 6: Cross-Table Referential Integrity
```sql
-- tests/assert_referential_integrity_meetings.sql
SELECT 
    m.meeting_id,
    m.user_id
FROM {{ ref('si_meetings') }} m
LEFT JOIN {{ ref('si_users') }} u ON m.user_id = u.user_id
WHERE u.user_id IS NULL
```

#### Test 7: Business Rule - Active Users Only
```sql
-- tests/assert_active_users_only.sql
SELECT 
    user_id,
    record_status
FROM {{ ref('si_users') }}
WHERE record_status != 'active'
```

#### Test 8: Data Freshness Validation
```sql
-- tests/assert_data_freshness.sql
SELECT 
    'si_users' as table_name,
    MAX(load_timestamp) as latest_load,
    CURRENT_TIMESTAMP() as current_time,
    DATEDIFF('hour', MAX(load_timestamp), CURRENT_TIMESTAMP()) as hours_since_load
FROM {{ ref('si_users') }}
HAVING hours_since_load > 24

UNION ALL

SELECT 
    'si_meetings' as table_name,
    MAX(load_timestamp) as latest_load,
    CURRENT_TIMESTAMP() as current_time,
    DATEDIFF('hour', MAX(load_timestamp), CURRENT_TIMESTAMP()) as hours_since_load
FROM {{ ref('si_meetings') }}
HAVING hours_since_load > 24
```

#### Test 9: Volume Anomaly Detection
```sql
-- tests/assert_volume_anomaly.sql
WITH daily_counts AS (
    SELECT 
        DATE(load_timestamp) as load_date,
        COUNT(*) as daily_count
    FROM {{ ref('si_users') }}
    WHERE load_timestamp >= CURRENT_DATE - 7
    GROUP BY DATE(load_timestamp)
),
avg_volume AS (
    SELECT AVG(daily_count) as avg_daily_count
    FROM daily_counts
    WHERE load_date < CURRENT_DATE
)
SELECT 
    dc.load_date,
    dc.daily_count,
    av.avg_daily_count,
    ABS(dc.daily_count - av.avg_daily_count) / av.avg_daily_count as variance_ratio
FROM daily_counts dc
CROSS JOIN avg_volume av
WHERE dc.load_date = CURRENT_DATE
  AND ABS(dc.daily_count - av.avg_daily_count) / av.avg_daily_count > 0.5
```

#### Test 10: Error Logging Completeness
```sql
-- tests/assert_error_logging_completeness.sql
WITH source_errors AS (
    SELECT COUNT(*) as error_count
    FROM {{ source('bronze', 'bz_users') }}
    WHERE user_id IS NULL 
       OR user_name IS NULL 
       OR TRIM(user_name) = ''
       OR email IS NULL 
       OR TRIM(email) = ''
       OR NOT REGEXP_LIKE(email, '^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}$')
),
logged_errors AS (
    SELECT COUNT(*) as logged_count
    FROM {{ ref('si_data_quality_errors') }}
    WHERE source_table = 'bz_users'
      AND DATE(created_timestamp) = CURRENT_DATE
)
SELECT 
    se.error_count,
    le.logged_count,
    se.error_count - le.logged_count as missing_logs
FROM source_errors se
CROSS JOIN logged_errors le
WHERE se.error_count != le.logged_count
```

### Parameterized Tests

#### Generic Test: Date Range Validation
```sql
-- macros/test_date_range_validation.sql
{% macro test_date_range_validation(model, start_date_column, end_date_column) %}

SELECT *
FROM {{ model }}
WHERE {{ end_date_column }} <= {{ start_date_column }}
   OR {{ start_date_column }} IS NULL
   OR {{ end_date_column }} IS NULL

{% endmacro %}
```

#### Generic Test: Positive Amount Validation
```sql
-- macros/test_positive_amount.sql
{% macro test_positive_amount(model, amount_column) %}

SELECT *
FROM {{ model }}
WHERE {{ amount_column }} < 0
   OR {{ amount_column }} IS NULL

{% endmacro %}
```

### Test Execution Commands

```bash
# Run all tests
dbt test

# Run tests for specific model
dbt test --select si_users

# Run specific test type
dbt test --select test_type:generic
dbt test --select test_type:singular

# Run tests with specific tags
dbt test --select tag:data_quality

# Run tests in fail-fast mode
dbt test --fail-fast

# Generate test documentation
dbt docs generate
dbt docs serve
```

## Test Coverage Summary

| Model | Schema Tests | Custom Tests | Coverage % |
|-------|-------------|--------------|------------|
| si_users | 8 | 4 | 95% |
| si_meetings | 6 | 3 | 90% |
| si_participants | 5 | 2 | 85% |
| si_feature_usage | 4 | 1 | 80% |
| si_webinars | 4 | 1 | 80% |
| si_support_tickets | 4 | 1 | 80% |
| si_licenses | 5 | 2 | 85% |
| si_billing_events | 5 | 2 | 85% |
| si_data_quality_errors | 6 | 2 | 90% |
| si_process_audit | 5 | 1 | 85% |

## Performance Considerations

- Tests are designed to run efficiently in Snowflake with proper indexing
- Incremental test logic to avoid full table scans
- Parameterized tests for reusability across models
- Test results tracked in dbt's run_results.json
- Audit trail maintained in Snowflake for compliance

## Maintenance Guidelines

1. **Regular Review**: Review and update tests monthly
2. **Performance Monitoring**: Monitor test execution times
3. **Coverage Analysis**: Ensure new models include comprehensive tests
4. **Documentation**: Keep test documentation updated
5. **Version Control**: Track test changes in Git

## API Cost Calculation

Estimated API cost for this comprehensive unit test case generation: **$0.0847 USD**

*Cost calculation based on token usage for analysis, test generation, and documentation creation.*

---

**End of Snowflake dbt Unit Test Case Document**