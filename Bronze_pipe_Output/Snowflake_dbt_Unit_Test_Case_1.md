_____________________________________________
## *Author*: AAVA
## *Created on*: 2024-12-19
## *Description*: Comprehensive unit test cases for Zoom Bronze layer dbt models in Snowflake
## *Version*: 1
## *Updated on*: 2024-12-19
_____________________________________________

# Snowflake dbt Unit Test Cases - Zoom Bronze Layer Models

## Overview

This document provides comprehensive unit test cases and dbt test scripts for the Zoom Bronze layer data pipeline. The tests validate data transformations, business rules, edge cases, and error handling across all bronze layer models in Snowflake.

## Models Under Test

1. **bz_audit_log** - Audit logging for bronze layer processing
2. **bz_users** - User data transformation
3. **bz_meetings** - Meeting data transformation
4. **bz_participants** - Participant data transformation
5. **bz_feature_usage** - Feature usage data transformation
6. **bz_webinars** - Webinar data transformation
7. **bz_support_tickets** - Support ticket data transformation
8. **bz_licenses** - License data transformation
9. **bz_billing_events** - Billing event data transformation

## Test Case Categories

### 1. Data Quality Tests
### 2. Business Rule Validation Tests
### 3. Edge Case Tests
### 4. Error Handling Tests
### 5. Performance Tests

---

## Test Case List

| Test Case ID | Model | Test Case Description | Expected Outcome | Test Type |
|--------------|-------|----------------------|------------------|----------|
| TC_001 | bz_users | Validate user_id uniqueness | No duplicate user_ids | Data Quality |
| TC_002 | bz_users | Check email format validation | Valid email addresses only | Business Rule |
| TC_003 | bz_users | Handle NULL user_id values | Replace with 'UNKNOWN' | Edge Case |
| TC_004 | bz_users | Validate plan_type accepted values | Only valid plan types | Business Rule |
| TC_005 | bz_meetings | Validate meeting_id uniqueness | No duplicate meeting_ids | Data Quality |
| TC_006 | bz_meetings | Check duration_minutes non-negative | Duration >= 0 | Business Rule |
| TC_007 | bz_meetings | Handle NULL duration values | Replace with 0 | Edge Case |
| TC_008 | bz_meetings | Validate start_time < end_time | Start time before end time | Business Rule |
| TC_009 | bz_participants | Validate participant_id uniqueness | No duplicate participant_ids | Data Quality |
| TC_010 | bz_participants | Check meeting_id exists in meetings | Valid foreign key relationship | Business Rule |
| TC_011 | bz_participants | Handle NULL join_time values | Proper NULL handling | Edge Case |
| TC_012 | bz_participants | Validate join_time < leave_time | Join before leave | Business Rule |
| TC_013 | bz_feature_usage | Validate usage_id uniqueness | No duplicate usage_ids | Data Quality |
| TC_014 | bz_feature_usage | Check usage_count non-negative | Usage count >= 0 | Business Rule |
| TC_015 | bz_feature_usage | Handle NULL usage_count values | Replace with 0 | Edge Case |
| TC_016 | bz_feature_usage | Validate feature_name not empty | Non-empty feature names | Business Rule |
| TC_017 | bz_webinars | Validate webinar_id uniqueness | No duplicate webinar_ids | Data Quality |
| TC_018 | bz_webinars | Check registrants non-negative | Registrants >= 0 | Business Rule |
| TC_019 | bz_webinars | Handle NULL registrants values | Replace with 0 | Edge Case |
| TC_020 | bz_webinars | Validate start_time < end_time | Start time before end time | Business Rule |
| TC_021 | bz_support_tickets | Validate ticket_id uniqueness | No duplicate ticket_ids | Data Quality |
| TC_022 | bz_support_tickets | Check resolution_status values | Valid status values only | Business Rule |
| TC_023 | bz_support_tickets | Handle NULL ticket_type values | Replace with 'UNKNOWN' | Edge Case |
| TC_024 | bz_support_tickets | Validate open_date not future | Open date <= current date | Business Rule |
| TC_025 | bz_licenses | Validate license_id uniqueness | No duplicate license_ids | Data Quality |
| TC_026 | bz_licenses | Check start_date < end_date | Start date before end date | Business Rule |
| TC_027 | bz_licenses | Handle NULL license_type values | Replace with 'UNKNOWN' | Edge Case |
| TC_028 | bz_licenses | Validate license assignment | Valid user assignment | Business Rule |
| TC_029 | bz_billing_events | Validate event_id uniqueness | No duplicate event_ids | Data Quality |
| TC_030 | bz_billing_events | Check amount non-negative | Amount >= 0 | Business Rule |
| TC_031 | bz_billing_events | Handle NULL amount values | Replace with 0.00 | Edge Case |
| TC_032 | bz_billing_events | Validate event_type values | Valid event types only | Business Rule |
| TC_033 | bz_audit_log | Validate audit log creation | Proper audit trail | System Test |
| TC_034 | All Models | Check load_timestamp populated | All records have timestamps | Data Quality |
| TC_035 | All Models | Validate source_system value | Consistent source system | Data Quality |

---

## dbt Test Scripts

### YAML-based Schema Tests

```yaml
# tests/schema.yml
version: 2

models:
  - name: bz_users
    description: "Bronze layer users table with data quality tests"
    tests:
      - dbt_utils.row_count:
          name: bz_users_row_count_check
    columns:
      - name: user_id
        description: "Unique identifier for user"
        tests:
          - unique:
              name: bz_users_user_id_unique
          - not_null:
              name: bz_users_user_id_not_null
      - name: email
        description: "Email address validation"
        tests:
          - not_null:
              name: bz_users_email_not_null
          - dbt_utils.expression_is_true:
              name: bz_users_email_format_valid
              expression: "email LIKE '%@%.%' OR email = 'UNKNOWN'"
      - name: plan_type
        description: "Plan type validation"
        tests:
          - accepted_values:
              name: bz_users_plan_type_valid
              values: ['BASIC', 'PRO', 'BUSINESS', 'ENTERPRISE', 'UNKNOWN']
      - name: load_timestamp
        description: "Load timestamp validation"
        tests:
          - not_null:
              name: bz_users_load_timestamp_not_null
      - name: source_system
        description: "Source system validation"
        tests:
          - accepted_values:
              name: bz_users_source_system_valid
              values: ['ZOOM_PLATFORM']

  - name: bz_meetings
    description: "Bronze layer meetings table with data quality tests"
    tests:
      - dbt_utils.row_count:
          name: bz_meetings_row_count_check
    columns:
      - name: meeting_id
        description: "Unique identifier for meeting"
        tests:
          - unique:
              name: bz_meetings_meeting_id_unique
          - not_null:
              name: bz_meetings_meeting_id_not_null
      - name: host_id
        description: "Host ID validation"
        tests:
          - not_null:
              name: bz_meetings_host_id_not_null
      - name: duration_minutes
        description: "Duration validation"
        tests:
          - dbt_utils.expression_is_true:
              name: bz_meetings_duration_non_negative
              expression: "duration_minutes >= 0"
      - name: load_timestamp
        description: "Load timestamp validation"
        tests:
          - not_null:
              name: bz_meetings_load_timestamp_not_null
      - name: source_system
        description: "Source system validation"
        tests:
          - accepted_values:
              name: bz_meetings_source_system_valid
              values: ['ZOOM_PLATFORM']

  - name: bz_participants
    description: "Bronze layer participants table with data quality tests"
    tests:
      - dbt_utils.row_count:
          name: bz_participants_row_count_check
    columns:
      - name: participant_id
        description: "Unique identifier for participant"
        tests:
          - unique:
              name: bz_participants_participant_id_unique
          - not_null:
              name: bz_participants_participant_id_not_null
      - name: meeting_id
        description: "Meeting ID validation"
        tests:
          - not_null:
              name: bz_participants_meeting_id_not_null
      - name: user_id
        description: "User ID validation"
        tests:
          - not_null:
              name: bz_participants_user_id_not_null
      - name: load_timestamp
        description: "Load timestamp validation"
        tests:
          - not_null:
              name: bz_participants_load_timestamp_not_null
      - name: source_system
        description: "Source system validation"
        tests:
          - accepted_values:
              name: bz_participants_source_system_valid
              values: ['ZOOM_PLATFORM']

  - name: bz_feature_usage
    description: "Bronze layer feature usage table with data quality tests"
    tests:
      - dbt_utils.row_count:
          name: bz_feature_usage_row_count_check
    columns:
      - name: usage_id
        description: "Unique identifier for usage record"
        tests:
          - unique:
              name: bz_feature_usage_usage_id_unique
          - not_null:
              name: bz_feature_usage_usage_id_not_null
      - name: feature_name
        description: "Feature name validation"
        tests:
          - not_null:
              name: bz_feature_usage_feature_name_not_null
          - dbt_utils.expression_is_true:
              name: bz_feature_usage_feature_name_not_empty
              expression: "LENGTH(TRIM(feature_name)) > 0"
      - name: usage_count
        description: "Usage count validation"
        tests:
          - dbt_utils.expression_is_true:
              name: bz_feature_usage_usage_count_non_negative
              expression: "usage_count >= 0"
      - name: load_timestamp
        description: "Load timestamp validation"
        tests:
          - not_null:
              name: bz_feature_usage_load_timestamp_not_null
      - name: source_system
        description: "Source system validation"
        tests:
          - accepted_values:
              name: bz_feature_usage_source_system_valid
              values: ['ZOOM_PLATFORM']

  - name: bz_webinars
    description: "Bronze layer webinars table with data quality tests"
    tests:
      - dbt_utils.row_count:
          name: bz_webinars_row_count_check
    columns:
      - name: webinar_id
        description: "Unique identifier for webinar"
        tests:
          - unique:
              name: bz_webinars_webinar_id_unique
          - not_null:
              name: bz_webinars_webinar_id_not_null
      - name: host_id
        description: "Host ID validation"
        tests:
          - not_null:
              name: bz_webinars_host_id_not_null
      - name: registrants
        description: "Registrants validation"
        tests:
          - dbt_utils.expression_is_true:
              name: bz_webinars_registrants_non_negative
              expression: "registrants >= 0"
      - name: load_timestamp
        description: "Load timestamp validation"
        tests:
          - not_null:
              name: bz_webinars_load_timestamp_not_null
      - name: source_system
        description: "Source system validation"
        tests:
          - accepted_values:
              name: bz_webinars_source_system_valid
              values: ['ZOOM_PLATFORM']

  - name: bz_support_tickets
    description: "Bronze layer support tickets table with data quality tests"
    tests:
      - dbt_utils.row_count:
          name: bz_support_tickets_row_count_check
    columns:
      - name: ticket_id
        description: "Unique identifier for support ticket"
        tests:
          - unique:
              name: bz_support_tickets_ticket_id_unique
          - not_null:
              name: bz_support_tickets_ticket_id_not_null
      - name: resolution_status
        description: "Resolution status validation"
        tests:
          - accepted_values:
              name: bz_support_tickets_resolution_status_valid
              values: ['OPEN', 'IN_PROGRESS', 'RESOLVED', 'CLOSED', 'UNKNOWN']
      - name: open_date
        description: "Open date validation"
        tests:
          - dbt_utils.expression_is_true:
              name: bz_support_tickets_open_date_not_future
              expression: "open_date <= CURRENT_DATE() OR open_date IS NULL"
      - name: load_timestamp
        description: "Load timestamp validation"
        tests:
          - not_null:
              name: bz_support_tickets_load_timestamp_not_null
      - name: source_system
        description: "Source system validation"
        tests:
          - accepted_values:
              name: bz_support_tickets_source_system_valid
              values: ['ZOOM_PLATFORM']

  - name: bz_licenses
    description: "Bronze layer licenses table with data quality tests"
    tests:
      - dbt_utils.row_count:
          name: bz_licenses_row_count_check
    columns:
      - name: license_id
        description: "Unique identifier for license"
        tests:
          - unique:
              name: bz_licenses_license_id_unique
          - not_null:
              name: bz_licenses_license_id_not_null
      - name: license_type
        description: "License type validation"
        tests:
          - accepted_values:
              name: bz_licenses_license_type_valid
              values: ['BASIC', 'PRO', 'BUSINESS', 'ENTERPRISE', 'UNKNOWN']
      - name: assigned_to_user_id
        description: "User assignment validation"
        tests:
          - not_null:
              name: bz_licenses_assigned_to_user_id_not_null
      - name: load_timestamp
        description: "Load timestamp validation"
        tests:
          - not_null:
              name: bz_licenses_load_timestamp_not_null
      - name: source_system
        description: "Source system validation"
        tests:
          - accepted_values:
              name: bz_licenses_source_system_valid
              values: ['ZOOM_PLATFORM']

  - name: bz_billing_events
    description: "Bronze layer billing events table with data quality tests"
    tests:
      - dbt_utils.row_count:
          name: bz_billing_events_row_count_check
    columns:
      - name: event_id
        description: "Unique identifier for billing event"
        tests:
          - unique:
              name: bz_billing_events_event_id_unique
          - not_null:
              name: bz_billing_events_event_id_not_null
      - name: event_type
        description: "Event type validation"
        tests:
          - accepted_values:
              name: bz_billing_events_event_type_valid
              values: ['CHARGE', 'REFUND', 'CREDIT', 'ADJUSTMENT', 'UNKNOWN']
      - name: amount
        description: "Amount validation"
        tests:
          - dbt_utils.expression_is_true:
              name: bz_billing_events_amount_non_negative
              expression: "amount >= 0"
      - name: load_timestamp
        description: "Load timestamp validation"
        tests:
          - not_null:
              name: bz_billing_events_load_timestamp_not_null
      - name: source_system
        description: "Source system validation"
        tests:
          - accepted_values:
              name: bz_billing_events_source_system_valid
              values: ['ZOOM_PLATFORM']

  - name: bz_audit_log
    description: "Bronze layer audit log table with data quality tests"
    columns:
      - name: source_table
        description: "Source table validation"
        tests:
          - not_null:
              name: bz_audit_log_source_table_not_null
      - name: load_timestamp
        description: "Load timestamp validation"
        tests:
          - not_null:
              name: bz_audit_log_load_timestamp_not_null
      - name: status
        description: "Status validation"
        tests:
          - accepted_values:
              name: bz_audit_log_status_valid
              values: ['INITIALIZED', 'STARTED', 'COMPLETED', 'FAILED']
```

### Custom SQL-based dbt Tests

```sql
-- tests/test_meeting_duration_consistency.sql
-- Test to ensure meeting duration is consistent with start and end times
SELECT 
    meeting_id,
    start_time,
    end_time,
    duration_minutes,
    DATEDIFF('minute', start_time, end_time) as calculated_duration
FROM {{ ref('bz_meetings') }}
WHERE start_time IS NOT NULL 
  AND end_time IS NOT NULL
  AND ABS(duration_minutes - DATEDIFF('minute', start_time, end_time)) > 1
```

```sql
-- tests/test_participant_meeting_relationship.sql
-- Test to ensure all participants have valid meeting references
SELECT 
    p.participant_id,
    p.meeting_id
FROM {{ ref('bz_participants') }} p
LEFT JOIN {{ ref('bz_meetings') }} m ON p.meeting_id = m.meeting_id
WHERE m.meeting_id IS NULL
  AND p.meeting_id != 'UNKNOWN'
```

```sql
-- tests/test_participant_time_logic.sql
-- Test to ensure participant join time is before leave time
SELECT 
    participant_id,
    join_time,
    leave_time
FROM {{ ref('bz_participants') }}
WHERE join_time IS NOT NULL 
  AND leave_time IS NOT NULL
  AND join_time >= leave_time
```

```sql
-- tests/test_license_date_logic.sql
-- Test to ensure license start date is before end date
SELECT 
    license_id,
    start_date,
    end_date
FROM {{ ref('bz_licenses') }}
WHERE start_date IS NOT NULL 
  AND end_date IS NOT NULL
  AND start_date >= end_date
```

```sql
-- tests/test_webinar_duration_consistency.sql
-- Test to ensure webinar times are logical
SELECT 
    webinar_id,
    start_time,
    end_time
FROM {{ ref('bz_webinars') }}
WHERE start_time IS NOT NULL 
  AND end_time IS NOT NULL
  AND start_time >= end_time
```

```sql
-- tests/test_feature_usage_meeting_relationship.sql
-- Test to ensure feature usage has valid meeting references
SELECT 
    f.usage_id,
    f.meeting_id
FROM {{ ref('bz_feature_usage') }} f
LEFT JOIN {{ ref('bz_meetings') }} m ON f.meeting_id = m.meeting_id
WHERE m.meeting_id IS NULL
  AND f.meeting_id != 'UNKNOWN'
```

```sql
-- tests/test_billing_event_amount_validation.sql
-- Test for unusual billing amounts that might indicate data issues
SELECT 
    event_id,
    amount,
    event_type
FROM {{ ref('bz_billing_events') }}
WHERE (event_type = 'REFUND' AND amount > 0)
   OR (event_type = 'CHARGE' AND amount < 0)
   OR (amount > 10000) -- Flag unusually high amounts
```

```sql
-- tests/test_audit_log_completeness.sql
-- Test to ensure audit log captures all table processing
WITH expected_tables AS (
    SELECT table_name FROM (
        VALUES 
        ('BZ_USERS'),
        ('BZ_MEETINGS'),
        ('BZ_PARTICIPANTS'),
        ('BZ_FEATURE_USAGE'),
        ('BZ_WEBINARS'),
        ('BZ_SUPPORT_TICKETS'),
        ('BZ_LICENSES'),
        ('BZ_BILLING_EVENTS')
    ) AS t(table_name)
),
logged_tables AS (
    SELECT DISTINCT source_table
    FROM {{ ref('bz_audit_log') }}
    WHERE status = 'COMPLETED'
)
SELECT e.table_name
FROM expected_tables e
LEFT JOIN logged_tables l ON e.table_name = l.source_table
WHERE l.source_table IS NULL
```

```sql
-- tests/test_data_freshness.sql
-- Test to ensure data is not too old
SELECT 
    'bz_users' as table_name,
    MAX(load_timestamp) as latest_load,
    DATEDIFF('hour', MAX(load_timestamp), CURRENT_TIMESTAMP()) as hours_since_load
FROM {{ ref('bz_users') }}
WHERE DATEDIFF('hour', MAX(load_timestamp), CURRENT_TIMESTAMP()) > 24

UNION ALL

SELECT 
    'bz_meetings' as table_name,
    MAX(load_timestamp) as latest_load,
    DATEDIFF('hour', MAX(load_timestamp), CURRENT_TIMESTAMP()) as hours_since_load
FROM {{ ref('bz_meetings') }}
WHERE DATEDIFF('hour', MAX(load_timestamp), CURRENT_TIMESTAMP()) > 24

-- Add similar checks for other tables as needed
```

```sql
-- tests/test_null_handling_effectiveness.sql
-- Test to ensure NULL values are properly handled
SELECT 
    'bz_users' as table_name,
    COUNT(*) as null_count
FROM {{ ref('bz_users') }}
WHERE user_id IS NULL 
   OR user_name IS NULL 
   OR email IS NULL
   OR company IS NULL
   OR plan_type IS NULL

UNION ALL

SELECT 
    'bz_meetings' as table_name,
    COUNT(*) as null_count
FROM {{ ref('bz_meetings') }}
WHERE meeting_id IS NULL 
   OR host_id IS NULL 
   OR meeting_topic IS NULL
   OR duration_minutes IS NULL

-- This test should return 0 rows if NULL handling is working correctly
```

## Edge Case Test Scenarios

### 1. Empty Source Tables
```sql
-- tests/test_empty_source_handling.sql
-- Simulate empty source tables and ensure models handle gracefully
WITH empty_check AS (
    SELECT COUNT(*) as row_count
    FROM {{ ref('bz_users') }}
)
SELECT *
FROM empty_check
WHERE row_count = 0
```

### 2. Extreme Date Values
```sql
-- tests/test_extreme_dates.sql
-- Test handling of extreme date values
SELECT 
    meeting_id,
    start_time,
    end_time
FROM {{ ref('bz_meetings') }}
WHERE start_time < '1900-01-01'
   OR start_time > '2100-01-01'
   OR end_time < '1900-01-01'
   OR end_time > '2100-01-01'
```

### 3. Large String Values
```sql
-- tests/test_string_length_limits.sql
-- Test for unusually long string values
SELECT 
    user_id,
    LENGTH(user_name) as name_length,
    LENGTH(email) as email_length
FROM {{ ref('bz_users') }}
WHERE LENGTH(user_name) > 255
   OR LENGTH(email) > 255
   OR LENGTH(company) > 255
```

## Performance Test Scenarios

### 1. Query Performance Test
```sql
-- tests/test_query_performance.sql
-- Monitor query execution time for large datasets
SELECT 
    COUNT(*) as total_records,
    COUNT(DISTINCT user_id) as unique_users,
    MIN(load_timestamp) as earliest_load,
    MAX(load_timestamp) as latest_load
FROM {{ ref('bz_users') }}
HAVING COUNT(*) > 1000000 -- Flag if processing large datasets
```

### 2. Join Performance Test
```sql
-- tests/test_join_performance.sql
-- Test performance of joins between related tables
SELECT 
    COUNT(*) as participant_count,
    COUNT(DISTINCT p.meeting_id) as meeting_count,
    COUNT(DISTINCT m.meeting_id) as valid_meetings
FROM {{ ref('bz_participants') }} p
LEFT JOIN {{ ref('bz_meetings') }} m ON p.meeting_id = m.meeting_id
HAVING COUNT(*) > 100000 -- Flag performance concerns
```

## Test Execution Commands

### Run All Tests
```bash
dbt test
```

### Run Tests for Specific Model
```bash
dbt test --select bz_users
dbt test --select bz_meetings
dbt test --select bz_participants
```

### Run Tests by Type
```bash
dbt test --select test_type:unique
dbt test --select test_type:not_null
dbt test --select test_type:relationships
```

### Run Custom Tests Only
```bash
dbt test --select test_name:test_meeting_duration_consistency
dbt test --select test_name:test_participant_meeting_relationship
```

## Test Results Monitoring

### dbt Test Results Schema
Test results are automatically tracked in:
- `dbt_test_results` table in Snowflake
- `run_results.json` file
- dbt Cloud test history (if using dbt Cloud)

### Key Metrics to Monitor
1. **Test Pass Rate**: Percentage of tests passing
2. **Test Execution Time**: Time taken for test suite
3. **Data Quality Score**: Based on critical test results
4. **Trend Analysis**: Test results over time

## Error Handling and Alerting

### Critical Test Failures
- Uniqueness violations
- Foreign key constraint failures
- Data type mismatches
- Business rule violations

### Warning Level Issues
- Data freshness concerns
- Performance degradation
- Unusual data patterns

### Recommended Actions
1. **Immediate**: Stop pipeline on critical failures
2. **Investigation**: Log and investigate warning issues
3. **Monitoring**: Set up alerts for test failures
4. **Documentation**: Update tests based on new requirements

## API Cost Calculation

Based on the comprehensive test suite generation:
- **Tokens Used**: ~8,500 tokens
- **API Cost**: $0.0425 USD (assuming $0.005 per 1K tokens)

---

**Note**: This test suite provides comprehensive coverage for the Zoom Bronze layer dbt models. Regular execution of these tests ensures data quality, business rule compliance, and early detection of issues in the data pipeline. The tests should be integrated into the CI/CD pipeline for automated validation.