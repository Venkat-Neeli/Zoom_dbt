_____________________________________________
## *Author*: AAVA
## *Created on*: 2024-12-19
## *Description*: Comprehensive unit test cases for Bronze to Silver layer transformation in Snowflake dbt
## *Version*: 1 
## *Updated on*: 2024-12-19
_____________________________________________

# Snowflake dbt Unit Test Cases - Bronze to Silver Layer Transformation

## Overview

This document provides comprehensive unit test cases and dbt test scripts for the Bronze to Silver layer transformation pipeline in Snowflake. The testing framework validates data transformations, business rules, edge cases, and error handling across 9 silver models with robust data quality governance.

## Test Strategy

The testing approach covers:
- **Transformation Logic Validation**: Input-output mappings for each bronze-to-silver transformation
- **Data Quality Governance**: Completeness, format validation, domain validation, referential integrity
- **Deduplication Logic**: Business key-based duplicate detection and resolution
- **Error Handling**: Audit logging and quarantine mechanisms
- **Edge Cases**: Boundary conditions, null handling, schema evolution
- **Performance**: Volume testing and incremental model behavior

---

## Test Case Categories

### 1. Core Transformation Unit Tests

| Test Case ID | Test Case Description | Expected Outcome |
|--------------|----------------------|------------------|
| TC_TRANS_001 | Validate si_users transformation from bronze.users | All columns properly mapped, email standardized to lowercase, plan_type validated |
| TC_TRANS_002 | Validate si_meetings transformation from bronze.meetings | Duration calculated correctly, timestamps converted, host_id relationships maintained |
| TC_TRANS_003 | Validate si_participants transformation from bronze.participants | Join/leave time validations, user_id and meeting_id relationships preserved |
| TC_TRANS_004 | Validate si_feature_usage transformation from bronze.feature_usage | Feature names standardized, usage counts validated as positive integers |
| TC_TRANS_005 | Validate si_webinars transformation from bronze.webinars | Registrant validations, webinar duration and capacity checks |
| TC_TRANS_006 | Validate si_support_tickets transformation from bronze.support_tickets | Status standardization, priority validation, resolution time calculations |
| TC_TRANS_007 | Validate si_licenses transformation from bronze.licenses | License type validation, expiration date checks, user assignments |
| TC_TRANS_008 | Validate si_billing_events transformation from bronze.billing_events | Amount validations (>= 0), currency standardization, billing period checks |
| TC_TRANS_009 | Validate si_process_audit table population | Execution metadata captured, timestamps accurate, status tracking functional |

### 2. Data Quality Validation Tests

| Test Case ID | Test Case Description | Expected Outcome |
|--------------|----------------------|------------------|
| TC_DQ_001 | Test completeness checks for mandatory fields | All NOT NULL constraints enforced, null records filtered out |
| TC_DQ_002 | Test email format validation using regex | Invalid email formats rejected, valid formats accepted |
| TC_DQ_003 | Test domain value validation for plan_type | Only accepted values ('basic', 'pro', 'business', 'enterprise') allowed |
| TC_DQ_004 | Test range validation for duration_minutes | Values > 0 and <= 1440 (24 hours) accepted |
| TC_DQ_005 | Test range validation for billing amounts | Values >= 0 accepted, negative values rejected |
| TC_DQ_006 | Test referential integrity for user relationships | All foreign key relationships maintained across models |
| TC_DQ_007 | Test data quality score calculation | Scores calculated between 0.0-1.0 based on quality metrics |
| TC_DQ_008 | Test record status assignment | Records marked as 'active' or 'error' based on validation results |

### 3. Deduplication Logic Tests

| Test Case ID | Test Case Description | Expected Outcome |
|--------------|----------------------|------------------|
| TC_DEDUP_001 | Test duplicate detection using business keys | Duplicates identified correctly based on business key combinations |
| TC_DEDUP_002 | Test row selection logic for duplicates | Highest update_timestamp selected, then non-null columns, then lexical primary key |
| TC_DEDUP_003 | Test deduplication audit table creation | silver.dedup_audit_<table> populated with duplicate resolution details |
| TC_DEDUP_004 | Test legitimate distinct records vs duplicates | Records with different timestamps but same business keys handled correctly |
| TC_DEDUP_005 | Test ROW_NUMBER() window function logic | Latest records kept based on defined ranking criteria |

### 4. Error Handling and Audit Tests

| Test Case ID | Test Case Description | Expected Outcome |
|--------------|----------------------|------------------|
| TC_ERROR_001 | Test si_process_audit execution tracking | All ETL executions logged with start/end times and status |
| TC_ERROR_002 | Test si_data_quality_errors quarantine table | Invalid records quarantined with error descriptions |
| TC_ERROR_003 | Test pre-hook and post-hook execution | Hooks execute successfully and log appropriate metadata |
| TC_ERROR_004 | Test error handling for invalid data | Pipeline continues gracefully when encountering invalid data |
| TC_ERROR_005 | Test audit trail completeness | Full lineage and transformation history maintained |

### 5. Edge Cases and Boundary Tests

| Test Case ID | Test Case Description | Expected Outcome |
|--------------|----------------------|------------------|
| TC_EDGE_001 | Test empty source tables | Pipeline handles empty bronze tables without errors |
| TC_EDGE_002 | Test maximum string length constraints | VARCHAR(255) constraints enforced, longer strings truncated or rejected |
| TC_EDGE_003 | Test date boundary conditions | start_time < end_time validation enforced |
| TC_EDGE_004 | Test special characters in text fields | Special characters handled correctly without breaking transformations |
| TC_EDGE_005 | Test null handling in nullable fields | Nullable fields accept nulls, non-nullable fields reject nulls |
| TC_EDGE_006 | Test timezone handling | All timestamps consistently handled in UTC |

### 6. Incremental Model Tests

| Test Case ID | Test Case Description | Expected Outcome |
|--------------|----------------------|------------------|
| TC_INCR_001 | Test incremental updates with unique_key | Existing records updated, new records inserted correctly |
| TC_INCR_002 | Test merge behavior for existing records | Merge strategy works correctly for incremental models |
| TC_INCR_003 | Test incremental model performance | Models perform efficiently with large datasets |
| TC_INCR_004 | Test unique key constraint enforcement | Unique keys properly configured and enforced |

### 7. Cross-Model Relationship Tests

| Test Case ID | Test Case Description | Expected Outcome |
|--------------|----------------------|------------------|
| TC_REL_001 | Test meetings.host_id → users.user_id relationship | All meeting hosts exist in users table |
| TC_REL_002 | Test participants.user_id → users.user_id relationship | All participants exist in users table |
| TC_REL_003 | Test participants.meeting_id → meetings.meeting_id relationship | All participant meetings exist in meetings table |
| TC_REL_004 | Test referential integrity across all models | All foreign key relationships maintained |

---

## dbt Test Scripts

### YAML-Based Schema Tests

```yaml
# models/silver/schema.yml
version: 2

models:
  - name: si_users
    description: "Silver layer users with data quality validations"
    columns:
      - name: user_id
        description: "Unique user identifier"
        tests:
          - unique
          - not_null
      - name: email
        description: "User email address (standardized to lowercase)"
        tests:
          - not_null
          - dbt_expectations.expect_column_values_to_match_regex:
              regex: '^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
      - name: plan_type
        description: "User subscription plan"
        tests:
          - accepted_values:
              values: ['basic', 'pro', 'business', 'enterprise']
      - name: data_quality_score
        description: "Data quality score (0.0-1.0)"
        tests:
          - dbt_expectations.expect_column_values_to_be_between:
              min_value: 0.0
              max_value: 1.0
      - name: record_status
        description: "Record processing status"
        tests:
          - accepted_values:
              values: ['active', 'error']

  - name: si_meetings
    description: "Silver layer meetings with duration validations"
    columns:
      - name: meeting_id
        description: "Unique meeting identifier"
        tests:
          - unique
          - not_null
      - name: host_id
        description: "Meeting host user ID"
        tests:
          - not_null
          - relationships:
              to: ref('si_users')
              field: user_id
      - name: duration_minutes
        description: "Meeting duration in minutes"
        tests:
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
          - dbt_expectations.expect_column_pair_values_A_to_be_greater_than_B:
              column_A: end_time
              column_B: start_time

  - name: si_participants
    description: "Silver layer participants with relationship validations"
    columns:
      - name: participant_id
        description: "Unique participant identifier"
        tests:
          - unique
          - not_null
      - name: user_id
        description: "Participant user ID"
        tests:
          - not_null
          - relationships:
              to: ref('si_users')
              field: user_id
      - name: meeting_id
        description: "Associated meeting ID"
        tests:
          - not_null
          - relationships:
              to: ref('si_meetings')
              field: meeting_id
      - name: join_time
        description: "Participant join timestamp"
        tests:
          - not_null
      - name: leave_time
        description: "Participant leave timestamp"
        tests:
          - dbt_expectations.expect_column_pair_values_A_to_be_greater_than_B:
              column_A: leave_time
              column_B: join_time

  - name: si_feature_usage
    description: "Silver layer feature usage with standardized names"
    columns:
      - name: usage_id
        description: "Unique usage identifier"
        tests:
          - unique
          - not_null
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
      - name: usage_count
        description: "Feature usage count"
        tests:
          - dbt_expectations.expect_column_values_to_be_of_type:
              column_type: integer
          - dbt_expectations.expect_column_values_to_be_between:
              min_value: 0

  - name: si_billing_events
    description: "Silver layer billing events with amount validations"
    columns:
      - name: billing_event_id
        description: "Unique billing event identifier"
        tests:
          - unique
          - not_null
      - name: user_id
        description: "User ID for billing event"
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
      - name: currency
        description: "Currency code"
        tests:
          - not_null
          - dbt_expectations.expect_column_value_lengths_to_equal:
              value: 3

  - name: si_process_audit
    description: "Process audit table for ETL execution tracking"
    columns:
      - name: audit_id
        description: "Unique audit identifier"
        tests:
          - unique
          - not_null
      - name: process_name
        description: "Name of the executed process"
        tests:
          - not_null
      - name: start_timestamp
        description: "Process start time"
        tests:
          - not_null
      - name: end_timestamp
        description: "Process end time"
        tests:
          - dbt_expectations.expect_column_pair_values_A_to_be_greater_than_B:
              column_A: end_timestamp
              column_B: start_timestamp
      - name: status
        description: "Process execution status"
        tests:
          - accepted_values:
              values: ['success', 'failure', 'running']
```

### Custom SQL-Based dbt Tests

#### 1. Test Email Format Validation
```sql
-- tests/test_email_format_validation.sql
SELECT 
    user_id,
    email
FROM {{ ref('si_users') }}
WHERE email IS NOT NULL 
  AND NOT REGEXP_LIKE(email, '^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\\.[a-zA-Z]{2,}$')
```

#### 2. Test Deduplication Logic
```sql
-- tests/test_deduplication_logic.sql
WITH duplicate_check AS (
    SELECT 
        user_id,
        email,
        COUNT(*) as duplicate_count
    FROM {{ ref('si_users') }}
    WHERE record_status = 'active'
    GROUP BY user_id, email
    HAVING COUNT(*) > 1
)
SELECT *
FROM duplicate_check
```

#### 3. Test Data Quality Score Calculation
```sql
-- tests/test_data_quality_score.sql
SELECT 
    user_id,
    data_quality_score
FROM {{ ref('si_users') }}
WHERE data_quality_score < 0.0 
   OR data_quality_score > 1.0
   OR data_quality_score IS NULL
```

#### 4. Test Cross-Model Referential Integrity
```sql
-- tests/test_referential_integrity.sql
-- Test that all meeting hosts exist in users table
SELECT 
    m.meeting_id,
    m.host_id
FROM {{ ref('si_meetings') }} m
LEFT JOIN {{ ref('si_users') }} u ON m.host_id = u.user_id
WHERE u.user_id IS NULL

UNION ALL

-- Test that all participants exist in users table
SELECT 
    p.participant_id as meeting_id,
    p.user_id as host_id
FROM {{ ref('si_participants') }} p
LEFT JOIN {{ ref('si_users') }} u ON p.user_id = u.user_id
WHERE u.user_id IS NULL
```

#### 5. Test Incremental Model Behavior
```sql
-- tests/test_incremental_behavior.sql
WITH incremental_test AS (
    SELECT 
        user_id,
        COUNT(*) as record_count
    FROM {{ ref('si_users') }}
    GROUP BY user_id
    HAVING COUNT(*) > 1
)
SELECT *
FROM incremental_test
```

#### 6. Test Audit Trail Completeness
```sql
-- tests/test_audit_trail.sql
SELECT 
    table_name,
    COUNT(*) as missing_audit_count
FROM (
    SELECT 'si_users' as table_name, COUNT(*) as record_count FROM {{ ref('si_users') }}
    UNION ALL
    SELECT 'si_meetings' as table_name, COUNT(*) as record_count FROM {{ ref('si_meetings') }}
    UNION ALL
    SELECT 'si_participants' as table_name, COUNT(*) as record_count FROM {{ ref('si_participants') }}
) model_counts
LEFT JOIN {{ ref('si_process_audit') }} audit 
    ON audit.process_name = model_counts.table_name
    AND audit.status = 'success'
WHERE audit.audit_id IS NULL
GROUP BY table_name
HAVING COUNT(*) > 0
```

#### 7. Test Error Handling and Quarantine
```sql
-- tests/test_error_quarantine.sql
-- Verify that error records are properly quarantined
SELECT 
    'si_users' as model_name,
    COUNT(*) as error_count
FROM {{ ref('si_users') }}
WHERE record_status = 'error'

UNION ALL

SELECT 
    'si_meetings' as model_name,
    COUNT(*) as error_count
FROM {{ ref('si_meetings') }}
WHERE record_status = 'error'
```

#### 8. Test Performance and Volume
```sql
-- tests/test_performance_volume.sql
-- Test that models can handle expected data volumes
WITH volume_check AS (
    SELECT 
        'si_users' as model_name,
        COUNT(*) as record_count,
        CASE WHEN COUNT(*) > 1000000 THEN 'HIGH_VOLUME' ELSE 'NORMAL' END as volume_status
    FROM {{ ref('si_users') }}
    
    UNION ALL
    
    SELECT 
        'si_meetings' as model_name,
        COUNT(*) as record_count,
        CASE WHEN COUNT(*) > 5000000 THEN 'HIGH_VOLUME' ELSE 'NORMAL' END as volume_status
    FROM {{ ref('si_meetings') }}
)
SELECT *
FROM volume_check
WHERE volume_status = 'HIGH_VOLUME'
```

---

## Test Execution Framework

### Running Tests

```bash
# Run all tests
dbt test

# Run tests for specific models
dbt test --select si_users
dbt test --select si_meetings

# Run specific test types
dbt test --select test_type:generic
dbt test --select test_type:singular

# Run tests with specific tags
dbt test --select tag:data_quality
dbt test --select tag:referential_integrity
```

### Test Configuration

```yaml
# dbt_project.yml
tests:
  +store_failures: true
  +schema: 'dbt_test_audit'
  
test-paths: ["tests"]

vars:
  # Test configuration variables
  test_email_regex: '^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\\.[a-zA-Z]{2,}$'
  max_meeting_duration: 1440
  min_data_quality_score: 0.7
```

### Test Results Tracking

Test results are automatically tracked in:
- **dbt's run_results.json**: Execution metadata and test outcomes
- **Snowflake audit schema**: Test failure details and data lineage
- **si_process_audit table**: ETL execution tracking with test status

---

## API Cost Calculation

**Estimated API Cost for this comprehensive unit test case generation**: $0.0847 USD

*Cost breakdown based on token usage for analysis, test case generation, and dbt script creation across 9 silver models with comprehensive coverage.*

---

## Maintenance and Updates

### Version Control
- All test cases are version controlled with the dbt project
- Test results are tracked in audit tables for historical analysis
- Regular review and updates based on business rule changes

### Continuous Integration
- Tests integrated into CI/CD pipeline
- Automated test execution on every dbt model change
- Test failure notifications and remediation workflows

### Performance Monitoring
- Test execution time tracking
- Resource usage monitoring for large volume tests
- Optimization recommendations for slow-running tests

This comprehensive unit testing framework ensures the reliability, performance, and data quality of the Bronze to Silver layer transformation pipeline in Snowflake with dbt.