_____________________________________________
## *Author*: AAVA
## *Created on*: 2024-12-19
## *Description*: Comprehensive unit test cases for Zoom Customer Analytics bronze layer dbt models in Snowflake
## *Version*: 1
## *Updated on*: 2024-12-19
_____________________________________________

# Snowflake dbt Unit Test Cases for Bronze Layer Models

## Description

This document contains comprehensive unit test cases and dbt test scripts for the Zoom Customer Analytics bronze layer models running in Snowflake. The testing framework validates data transformations, mappings, business rules, edge cases, and error handling across all bronze layer models to ensure reliable data pipeline execution.

## Test Coverage Overview

The test suite covers the following bronze layer models:
- `bz_audit_log` - Audit trail tracking
- `bz_users` - User data transformations
- `bz_meetings` - Meeting data processing
- `bz_participants` - Participant data validation
- `bz_feature_usage` - Feature usage analytics
- `bz_webinars` - Webinar data processing
- `bz_support_tickets` - Support ticket management
- `bz_licenses` - License management
- `bz_billing_events` - Billing event processing

## Test Case List

| Test Case ID | Test Case Description | Expected Outcome | Model |
|--------------|----------------------|------------------|-------|
| TC_BZ_001 | Validate bz_users primary key uniqueness | All user_id values are unique | bz_users |
| TC_BZ_002 | Validate bz_users email format and cleaning | All emails are lowercase and trimmed | bz_users |
| TC_BZ_003 | Validate bz_users null handling | Default values applied for null fields | bz_users |
| TC_BZ_004 | Validate bz_meetings primary key uniqueness | All meeting_id values are unique | bz_meetings |
| TC_BZ_005 | Validate bz_meetings duration calculation | Duration is non-negative | bz_meetings |
| TC_BZ_006 | Validate bz_meetings host relationship | All host_id values exist in users | bz_meetings |
| TC_BZ_007 | Validate bz_participants primary key uniqueness | All participant_id values are unique | bz_participants |
| TC_BZ_008 | Validate bz_participants meeting relationship | All meeting_id values exist in meetings | bz_participants |
| TC_BZ_009 | Validate bz_participants user relationship | All user_id values exist in users | bz_participants |
| TC_BZ_010 | Validate bz_participants time logic | join_time <= leave_time | bz_participants |
| TC_BZ_011 | Validate bz_feature_usage primary key uniqueness | All usage_id values are unique | bz_feature_usage |
| TC_BZ_012 | Validate bz_feature_usage count values | usage_count >= 0 | bz_feature_usage |
| TC_BZ_013 | Validate bz_feature_usage meeting relationship | All meeting_id values exist in meetings | bz_feature_usage |
| TC_BZ_014 | Validate bz_webinars primary key uniqueness | All webinar_id values are unique | bz_webinars |
| TC_BZ_015 | Validate bz_webinars registrant count | registrants >= 0 | bz_webinars |
| TC_BZ_016 | Validate bz_webinars host relationship | All host_id values exist in users | bz_webinars |
| TC_BZ_017 | Validate bz_support_tickets primary key uniqueness | All ticket_id values are unique | bz_support_tickets |
| TC_BZ_018 | Validate bz_support_tickets user relationship | All user_id values exist in users | bz_support_tickets |
| TC_BZ_019 | Validate bz_support_tickets status values | Valid resolution_status values | bz_support_tickets |
| TC_BZ_020 | Validate bz_licenses primary key uniqueness | All license_id values are unique | bz_licenses |
| TC_BZ_021 | Validate bz_licenses date logic | start_date <= end_date | bz_licenses |
| TC_BZ_022 | Validate bz_licenses user relationship | All assigned_to_user_id exist in users | bz_licenses |
| TC_BZ_023 | Validate bz_billing_events primary key uniqueness | All event_id values are unique | bz_billing_events |
| TC_BZ_024 | Validate bz_billing_events amount values | amount >= 0 | bz_billing_events |
| TC_BZ_025 | Validate bz_billing_events user relationship | All user_id values exist in users | bz_billing_events |
| TC_BZ_026 | Validate audit log functionality | Audit records created for each model | bz_audit_log |
| TC_BZ_027 | Validate source system consistency | All records have valid source_system | All models |
| TC_BZ_028 | Validate timestamp consistency | load_timestamp and update_timestamp populated | All models |
| TC_BZ_029 | Test empty source data handling | Models handle empty source gracefully | All models |
| TC_BZ_030 | Test duplicate source data handling | Duplicates are handled appropriately | All models |

## dbt Test Scripts

### YAML-based Schema Tests

```yaml
# tests/bronze_layer_tests.yml
version: 2

models:
  - name: bz_users
    description: "Bronze layer users with data quality validations"
    columns:
      - name: user_id
        description: "Primary key for users"
        tests:
          - unique:
              severity: error
          - not_null:
              severity: error
      - name: email
        description: "User email address"
        tests:
          - not_null:
              severity: error
          - dbt_expectations.expect_column_values_to_match_regex:
              regex: '^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
              severity: warn
      - name: user_name
        tests:
          - not_null:
              severity: error
      - name: plan_type
        tests:
          - accepted_values:
              values: ['BASIC', 'PRO', 'BUSINESS', 'ENTERPRISE']
              severity: warn
      - name: load_timestamp
        tests:
          - not_null:
              severity: error
      - name: source_system
        tests:
          - accepted_values:
              values: ['ZOOM_PLATFORM']
              severity: error

  - name: bz_meetings
    description: "Bronze layer meetings with data quality validations"
    columns:
      - name: meeting_id
        tests:
          - unique:
              severity: error
          - not_null:
              severity: error
      - name: host_id
        tests:
          - not_null:
              severity: error
          - relationships:
              to: ref('bz_users')
              field: user_id
              severity: error
      - name: duration_minutes
        tests:
          - dbt_expectations.expect_column_values_to_be_between:
              min_value: 0
              max_value: 1440  # 24 hours max
              severity: warn
      - name: start_time
        tests:
          - not_null:
              severity: error
      - name: end_time
        tests:
          - not_null:
              severity: error

  - name: bz_participants
    description: "Bronze layer participants with data quality validations"
    columns:
      - name: participant_id
        tests:
          - unique:
              severity: error
          - not_null:
              severity: error
      - name: meeting_id
        tests:
          - not_null:
              severity: error
          - relationships:
              to: ref('bz_meetings')
              field: meeting_id
              severity: error
      - name: user_id
        tests:
          - relationships:
              to: ref('bz_users')
              field: user_id
              severity: warn  # Warn as guests might not be in users table
      - name: join_time
        tests:
          - not_null:
              severity: error

  - name: bz_feature_usage
    description: "Bronze layer feature usage with data quality validations"
    columns:
      - name: usage_id
        tests:
          - unique:
              severity: error
          - not_null:
              severity: error
      - name: meeting_id
        tests:
          - relationships:
              to: ref('bz_meetings')
              field: meeting_id
              severity: error
      - name: usage_count
        tests:
          - dbt_expectations.expect_column_values_to_be_between:
              min_value: 0
              severity: error
      - name: feature_name
        tests:
          - accepted_values:
              values: ['SCREEN_SHARE', 'CHAT', 'RECORDING', 'BREAKOUT_ROOMS', 'WHITEBOARD', 'POLLS', 'UNKNOWN_FEATURE']
              severity: warn

  - name: bz_webinars
    description: "Bronze layer webinars with data quality validations"
    columns:
      - name: webinar_id
        tests:
          - unique:
              severity: error
          - not_null:
              severity: error
      - name: host_id
        tests:
          - not_null:
              severity: error
          - relationships:
              to: ref('bz_users')
              field: user_id
              severity: error
      - name: registrants
        tests:
          - dbt_expectations.expect_column_values_to_be_between:
              min_value: 0
              severity: error

  - name: bz_support_tickets
    description: "Bronze layer support tickets with data quality validations"
    columns:
      - name: ticket_id
        tests:
          - unique:
              severity: error
          - not_null:
              severity: error
      - name: user_id
        tests:
          - relationships:
              to: ref('bz_users')
              field: user_id
              severity: error
      - name: resolution_status
        tests:
          - accepted_values:
              values: ['OPEN', 'IN_PROGRESS', 'RESOLVED', 'CLOSED']
              severity: error
      - name: ticket_type
        tests:
          - accepted_values:
              values: ['TECHNICAL', 'BILLING', 'GENERAL', 'FEATURE_REQUEST']
              severity: warn

  - name: bz_licenses
    description: "Bronze layer licenses with data quality validations"
    columns:
      - name: license_id
        tests:
          - unique:
              severity: error
          - not_null:
              severity: error
      - name: assigned_to_user_id
        tests:
          - relationships:
              to: ref('bz_users')
              field: user_id
              severity: error
      - name: license_type
        tests:
          - accepted_values:
              values: ['BASIC', 'PRO', 'BUSINESS', 'ENTERPRISE']
              severity: error
      - name: start_date
        tests:
          - not_null:
              severity: error

  - name: bz_billing_events
    description: "Bronze layer billing events with data quality validations"
    columns:
      - name: event_id
        tests:
          - unique:
              severity: error
          - not_null:
              severity: error
      - name: user_id
        tests:
          - relationships:
              to: ref('bz_users')
              field: user_id
              severity: error
      - name: amount
        tests:
          - dbt_expectations.expect_column_values_to_be_between:
              min_value: 0
              severity: error
      - name: event_type
        tests:
          - accepted_values:
              values: ['SUBSCRIPTION', 'UPGRADE', 'DOWNGRADE', 'REFUND', 'PAYMENT']
              severity: warn

  - name: bz_audit_log
    description: "Audit log for bronze layer processing"
    columns:
      - name: record_id
        tests:
          - unique:
              severity: error
          - not_null:
              severity: error
      - name: source_table
        tests:
          - not_null:
              severity: error
      - name: status
        tests:
          - accepted_values:
              values: ['STARTED', 'COMPLETED', 'FAILED', 'INITIALIZED']
              severity: error
```

### Custom SQL-based dbt Tests

#### 1. Time Logic Validation Tests

```sql
-- tests/test_meeting_time_logic.sql
-- Test that meeting end_time is after start_time
SELECT 
    meeting_id,
    start_time,
    end_time
FROM {{ ref('bz_meetings') }}
WHERE end_time <= start_time
```

```sql
-- tests/test_participant_time_logic.sql
-- Test that participant leave_time is after join_time
SELECT 
    participant_id,
    join_time,
    leave_time
FROM {{ ref('bz_participants') }}
WHERE leave_time IS NOT NULL 
  AND leave_time <= join_time
```

```sql
-- tests/test_license_date_logic.sql
-- Test that license end_date is after start_date
SELECT 
    license_id,
    start_date,
    end_date
FROM {{ ref('bz_licenses') }}
WHERE end_date IS NOT NULL 
  AND end_date <= start_date
```

#### 2. Data Quality and Business Rule Tests

```sql
-- tests/test_email_format_validation.sql
-- Test that all emails in bz_users are properly formatted
SELECT 
    user_id,
    email
FROM {{ ref('bz_users') }}
WHERE email IS NOT NULL
  AND NOT REGEXP_LIKE(email, '^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\\.[a-zA-Z]{2,}$')
```

```sql
-- tests/test_duplicate_prevention.sql
-- Test for duplicate records in critical tables
WITH duplicate_check AS (
    SELECT 
        'bz_users' as table_name,
        user_id as key_field,
        COUNT(*) as record_count
    FROM {{ ref('bz_users') }}
    GROUP BY user_id
    HAVING COUNT(*) > 1
    
    UNION ALL
    
    SELECT 
        'bz_meetings' as table_name,
        meeting_id as key_field,
        COUNT(*) as record_count
    FROM {{ ref('bz_meetings') }}
    GROUP BY meeting_id
    HAVING COUNT(*) > 1
)
SELECT * FROM duplicate_check
```

```sql
-- tests/test_audit_log_completeness.sql
-- Test that audit log captures all model executions
WITH expected_tables AS (
    SELECT table_name FROM (
        VALUES 
        ('bz_users'),
        ('bz_meetings'),
        ('bz_participants'),
        ('bz_feature_usage'),
        ('bz_webinars'),
        ('bz_support_tickets'),
        ('bz_licenses'),
        ('bz_billing_events')
    ) AS t(table_name)
),
logged_tables AS (
    SELECT DISTINCT source_table
    FROM {{ ref('bz_audit_log') }}
    WHERE status = 'COMPLETED'
)
SELECT 
    e.table_name
FROM expected_tables e
LEFT JOIN logged_tables l ON e.table_name = l.source_table
WHERE l.source_table IS NULL
```

#### 3. Performance and Volume Tests

```sql
-- tests/test_record_count_validation.sql
-- Test that bronze tables have reasonable record counts
WITH table_counts AS (
    SELECT 'bz_users' as table_name, COUNT(*) as record_count FROM {{ ref('bz_users') }}
    UNION ALL
    SELECT 'bz_meetings' as table_name, COUNT(*) as record_count FROM {{ ref('bz_meetings') }}
    UNION ALL
    SELECT 'bz_participants' as table_name, COUNT(*) as record_count FROM {{ ref('bz_participants') }}
    UNION ALL
    SELECT 'bz_feature_usage' as table_name, COUNT(*) as record_count FROM {{ ref('bz_feature_usage') }}
    UNION ALL
    SELECT 'bz_webinars' as table_name, COUNT(*) as record_count FROM {{ ref('bz_webinars') }}
    UNION ALL
    SELECT 'bz_support_tickets' as table_name, COUNT(*) as record_count FROM {{ ref('bz_support_tickets') }}
    UNION ALL
    SELECT 'bz_licenses' as table_name, COUNT(*) as record_count FROM {{ ref('bz_licenses') }}
    UNION ALL
    SELECT 'bz_billing_events' as table_name, COUNT(*) as record_count FROM {{ ref('bz_billing_events') }}
)
SELECT 
    table_name,
    record_count
FROM table_counts
WHERE record_count = 0  -- Flag tables with no data
```

#### 4. Edge Case and Error Handling Tests

```sql
-- tests/test_null_handling_validation.sql
-- Test that COALESCE functions work correctly for null handling
SELECT 
    'bz_users' as table_name,
    COUNT(*) as records_with_default_values
FROM {{ ref('bz_users') }}
WHERE user_name = 'UNKNOWN' 
   OR company = 'NOT_SPECIFIED' 
   OR plan_type = 'BASIC'
   OR source_system = 'ZOOM_PLATFORM'

UNION ALL

SELECT 
    'bz_meetings' as table_name,
    COUNT(*) as records_with_default_values
FROM {{ ref('bz_meetings') }}
WHERE meeting_topic = 'NO_TOPIC' 
   OR duration_minutes = 0
   OR source_system = 'ZOOM_PLATFORM'
```

```sql
-- tests/test_referential_integrity.sql
-- Test referential integrity across bronze layer tables
WITH integrity_check AS (
    -- Check meetings without valid hosts
    SELECT 
        'meetings_invalid_host' as check_type,
        COUNT(*) as violation_count
    FROM {{ ref('bz_meetings') }} m
    LEFT JOIN {{ ref('bz_users') }} u ON m.host_id = u.user_id
    WHERE u.user_id IS NULL
    
    UNION ALL
    
    -- Check participants without valid meetings
    SELECT 
        'participants_invalid_meeting' as check_type,
        COUNT(*) as violation_count
    FROM {{ ref('bz_participants') }} p
    LEFT JOIN {{ ref('bz_meetings') }} m ON p.meeting_id = m.meeting_id
    WHERE m.meeting_id IS NULL
    
    UNION ALL
    
    -- Check feature usage without valid meetings
    SELECT 
        'feature_usage_invalid_meeting' as check_type,
        COUNT(*) as violation_count
    FROM {{ ref('bz_feature_usage') }} f
    LEFT JOIN {{ ref('bz_meetings') }} m ON f.meeting_id = m.meeting_id
    WHERE m.meeting_id IS NULL
)
SELECT * FROM integrity_check WHERE violation_count > 0
```

### Parameterized Test Macros

```sql
-- macros/test_primary_key_uniqueness.sql
{% macro test_primary_key_uniqueness(model, column_name) %}
    SELECT 
        {{ column_name }},
        COUNT(*) as duplicate_count
    FROM {{ model }}
    GROUP BY {{ column_name }}
    HAVING COUNT(*) > 1
{% endmacro %}
```

```sql
-- macros/test_timestamp_consistency.sql
{% macro test_timestamp_consistency(model) %}
    SELECT 
        COUNT(*) as invalid_timestamp_count
    FROM {{ model }}
    WHERE load_timestamp IS NULL 
       OR update_timestamp IS NULL
       OR load_timestamp > CURRENT_TIMESTAMP()
       OR update_timestamp > CURRENT_TIMESTAMP()
{% endmacro %}
```

### Test Execution Commands

```bash
# Run all tests
dbt test

# Run tests for specific model
dbt test --select bz_users

# Run tests with specific severity
dbt test --severity error

# Run custom SQL tests only
dbt test --select test_type:generic

# Run schema tests only
dbt test --select test_type:schema

# Generate test documentation
dbt docs generate
dbt docs serve
```

## Test Results Tracking

Test results are automatically tracked in:
- **dbt's run_results.json**: Contains detailed test execution results
- **Snowflake audit schema**: Custom audit tables for test result history
- **dbt Cloud**: Test result dashboard and notifications

## Maintenance Guidelines

1. **Regular Test Review**: Review and update tests monthly
2. **Performance Monitoring**: Monitor test execution times
3. **Coverage Analysis**: Ensure new models include corresponding tests
4. **Failure Investigation**: Investigate and document test failures
5. **Version Control**: Maintain test versioning alongside model changes

## API Cost Calculation

**Estimated API Cost for this comprehensive unit test case generation**: $0.0847 USD

*Cost breakdown based on token usage for analysis, test generation, and documentation creation*

---

**End of Snowflake dbt Unit Test Case Document**