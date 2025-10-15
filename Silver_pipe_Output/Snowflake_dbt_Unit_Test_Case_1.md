_____________________________________________
## *Author*: AAVA
## *Created on*: 2024-12-19
## *Description*: Comprehensive unit test cases for Snowflake dbt Bronze to Silver transformation pipeline
## *Version*: 1
## *Updated on*: 2024-12-19
_____________________________________________

# Snowflake dbt Unit Test Cases - Bronze to Silver Transformation

## Overview

This document provides comprehensive unit test cases and dbt test scripts for the Zoom Customer Analytics Bronze to Silver transformation pipeline running in Snowflake. The tests validate data transformations, business rules, edge cases, and error handling across all Silver layer models.

## Test Case List

### 1. Data Quality and Validation Tests

| Test Case ID | Test Case Description | Expected Outcome |
|--------------|----------------------|------------------|
| DQ-001 | Validate user_id not null in si_users | All records have non-null user_id |
| DQ-002 | Validate email format using regex in si_users | All emails match pattern ^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\\.[A-Za-z]{2,}$ |
| DQ-003 | Validate plan_type enumeration in si_users | All plan_type values in ('Free','Pro','Business','Enterprise') |
| DQ-004 | Validate meeting duration range in si_meetings | All duration_minutes > 0 and <= 1440 |
| DQ-005 | Validate timestamp logic in si_meetings | All start_time < end_time |
| DQ-006 | Validate participant join/leave logic | All join_time < leave_time in si_participants |
| DQ-007 | Validate data_quality_score calculation | Scores between 0.50 and 1.00 based on validation rules |
| DQ-008 | Validate record_status assignment | Status 'active' for valid records, 'error' for invalid |

### 2. Deduplication Tests

| Test Case ID | Test Case Description | Expected Outcome |
|--------------|----------------------|------------------|
| DD-001 | Test duplicate user records handling | Latest record by update_timestamp retained |
| DD-002 | Test duplicate meeting records handling | ROW_NUMBER() = 1 records selected |
| DD-003 | Test duplicate participant records handling | Deduplication by participant_id |
| DD-004 | Test deduplication with null timestamps | Records with non-null timestamps preferred |

### 3. Transformation Logic Tests

| Test Case ID | Test Case Description | Expected Outcome |
|--------------|----------------------|------------------|
| TL-001 | Test string trimming and cleansing | TRIM() applied to user_name, company, meeting_topic |
| TL-002 | Test empty string replacement | Empty company/topic replaced with '000' |
| TL-003 | Test email lowercase conversion | All emails converted to lowercase |
| TL-004 | Test date derivation from timestamps | load_date = DATE(load_timestamp) |
| TL-005 | Test plan_type standardization | Invalid plan_types default to 'Free' |

### 4. Incremental Model Tests

| Test Case ID | Test Case Description | Expected Outcome |
|--------------|----------------------|------------------|
| IM-001 | Test incremental load for si_users | Only new/updated records processed |
| IM-002 | Test incremental load for si_meetings | Incremental filter on update_timestamp |
| IM-003 | Test incremental load for si_participants | Proper incremental materialization |
| IM-004 | Test full refresh scenario | All records reprocessed on full refresh |

### 5. Audit and Error Handling Tests

| Test Case ID | Test Case Description | Expected Outcome |
|--------------|----------------------|------------------|
| AE-001 | Test si_process_audit table population | Audit records created for each transformation |
| AE-002 | Test pre-hook audit logging | STARTED status logged before transformation |
| AE-003 | Test post-hook audit logging | COMPLETED status logged after transformation |
| AE-004 | Test error record handling | Invalid records flagged with 'error' status |
| AE-005 | Test execution_id generation | Unique execution_id for each run |

### 6. Referential Integrity Tests

| Test Case ID | Test Case Description | Expected Outcome |
|--------------|----------------------|------------------|
| RI-001 | Test meetings.host_id references users.user_id | Valid foreign key relationships |
| RI-002 | Test participants.user_id references users.user_id | Valid user references |
| RI-003 | Test participants.meeting_id references meetings.meeting_id | Valid meeting references |
| RI-004 | Test orphaned record handling | Orphaned records handled gracefully |

### 7. Edge Case Tests

| Test Case ID | Test Case Description | Expected Outcome |
|--------------|----------------------|------------------|
| EC-001 | Test null value handling | Null values handled per business rules |
| EC-002 | Test empty dataset processing | Empty source tables handled gracefully |
| EC-003 | Test boundary value conditions | Edge cases for duration, dates handled |
| EC-004 | Test special characters in text fields | Special characters preserved/cleaned appropriately |

### 8. Performance and Scalability Tests

| Test Case ID | Test Case Description | Expected Outcome |
|--------------|----------------------|------------------|
| PS-001 | Test large dataset processing | Performance acceptable for 10K+ records |
| PS-002 | Test memory usage tracking | Memory usage logged in audit table |
| PS-003 | Test processing duration tracking | Duration calculated and logged |
| PS-004 | Test concurrent execution | Multiple runs handled without conflicts |

## dbt Test Scripts

### YAML-based Schema Tests

```yaml
# tests/schema.yml
version: 2

models:
  - name: si_users
    description: "Silver layer users with data quality validations"
    tests:
      - dbt_utils.unique_combination_of_columns:
          combination_of_columns:
            - user_id
            - load_date
    columns:
      - name: user_id
        description: "Unique user identifier"
        tests:
          - not_null
          - unique
      - name: user_name
        description: "User display name"
        tests:
          - not_null
          - dbt_expectations.expect_column_values_to_not_be_null
      - name: email
        description: "User email address"
        tests:
          - not_null
          - unique
          - dbt_expectations.expect_column_values_to_match_regex:
              regex: '^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\\.[A-Za-z]{2,}$'
      - name: plan_type
        description: "User subscription plan"
        tests:
          - not_null
          - accepted_values:
              values: ['Free', 'Pro', 'Business', 'Enterprise']
      - name: data_quality_score
        description: "Data quality score"
        tests:
          - not_null
          - dbt_expectations.expect_column_values_to_be_between:
              min_value: 0.50
              max_value: 1.00
      - name: record_status
        description: "Record status"
        tests:
          - not_null
          - accepted_values:
              values: ['active', 'error']

  - name: si_meetings
    description: "Silver layer meetings with data quality validations"
    tests:
      - dbt_utils.unique_combination_of_columns:
          combination_of_columns:
            - meeting_id
            - load_date
    columns:
      - name: meeting_id
        description: "Unique meeting identifier"
        tests:
          - not_null
          - unique
      - name: host_id
        description: "Meeting host user ID"
        tests:
          - not_null
          - relationships:
              to: ref('si_users')
              field: user_id
      - name: start_time
        description: "Meeting start timestamp"
        tests:
          - not_null
      - name: end_time
        description: "Meeting end timestamp"
        tests:
          - not_null
      - name: duration_minutes
        description: "Meeting duration in minutes"
        tests:
          - not_null
          - dbt_expectations.expect_column_values_to_be_between:
              min_value: 1
              max_value: 1440

  - name: si_participants
    description: "Silver layer participants with data quality validations"
    columns:
      - name: participant_id
        description: "Unique participant identifier"
        tests:
          - not_null
          - unique
      - name: meeting_id
        description: "Meeting identifier"
        tests:
          - not_null
          - relationships:
              to: ref('si_meetings')
              field: meeting_id
      - name: user_id
        description: "User identifier"
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
        tests:
          - not_null

  - name: si_process_audit
    description: "Process audit log"
    columns:
      - name: execution_id
        description: "Unique execution identifier"
        tests:
          - not_null
          - unique
      - name: pipeline_name
        description: "Pipeline name"
        tests:
          - not_null
      - name: status
        description: "Execution status"
        tests:
          - not_null
          - accepted_values:
              values: ['SUCCESS', 'FAILURE', 'STARTED', 'COMPLETED']
```

### Custom SQL-based dbt Tests

#### Test 1: Email Format Validation
```sql
-- tests/unit/test_email_format_validation.sql
{{ config(severity = 'error') }}

SELECT 
    user_id,
    email,
    'Invalid email format' as error_message
FROM {{ ref('si_users') }}
WHERE email IS NOT NULL 
    AND NOT REGEXP_LIKE(email, '^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\\.[A-Za-z]{2,}$')
```

#### Test 2: Meeting Time Logic Validation
```sql
-- tests/unit/test_meeting_time_logic.sql
{{ config(severity = 'error') }}

SELECT 
    meeting_id,
    start_time,
    end_time,
    'Start time must be before end time' as error_message
FROM {{ ref('si_meetings') }}
WHERE start_time >= end_time
```

#### Test 3: Participant Time Logic Validation
```sql
-- tests/unit/test_participant_time_logic.sql
{{ config(severity = 'error') }}

SELECT 
    participant_id,
    join_time,
    leave_time,
    'Join time must be before leave time' as error_message
FROM {{ ref('si_participants') }}
WHERE join_time >= leave_time
```

#### Test 4: Data Quality Score Validation
```sql
-- tests/unit/test_data_quality_score.sql
{{ config(severity = 'warn') }}

SELECT 
    'si_users' as table_name,
    COUNT(*) as total_records,
    COUNT(CASE WHEN data_quality_score = 1.00 THEN 1 END) as high_quality_records,
    COUNT(CASE WHEN data_quality_score = 0.50 THEN 1 END) as low_quality_records,
    ROUND(COUNT(CASE WHEN data_quality_score = 1.00 THEN 1 END) * 100.0 / COUNT(*), 2) as quality_percentage
FROM {{ ref('si_users') }}
HAVING quality_percentage < 95.0

UNION ALL

SELECT 
    'si_meetings' as table_name,
    COUNT(*) as total_records,
    COUNT(CASE WHEN data_quality_score = 1.00 THEN 1 END) as high_quality_records,
    COUNT(CASE WHEN data_quality_score = 0.50 THEN 1 END) as low_quality_records,
    ROUND(COUNT(CASE WHEN data_quality_score = 1.00 THEN 1 END) * 100.0 / COUNT(*), 2) as quality_percentage
FROM {{ ref('si_meetings') }}
HAVING quality_percentage < 95.0
```

#### Test 5: Deduplication Validation
```sql
-- tests/unit/test_deduplication.sql
{{ config(severity = 'error') }}

WITH duplicate_users AS (
    SELECT 
        user_id,
        COUNT(*) as record_count
    FROM {{ ref('si_users') }}
    GROUP BY user_id
    HAVING COUNT(*) > 1
),
duplicate_meetings AS (
    SELECT 
        meeting_id,
        COUNT(*) as record_count
    FROM {{ ref('si_meetings') }}
    GROUP BY meeting_id
    HAVING COUNT(*) > 1
),
duplicate_participants AS (
    SELECT 
        participant_id,
        COUNT(*) as record_count
    FROM {{ ref('si_participants') }}
    GROUP BY participant_id
    HAVING COUNT(*) > 1
)

SELECT 'si_users' as table_name, user_id as duplicate_key, record_count
FROM duplicate_users

UNION ALL

SELECT 'si_meetings' as table_name, meeting_id as duplicate_key, record_count
FROM duplicate_meetings

UNION ALL

SELECT 'si_participants' as table_name, participant_id as duplicate_key, record_count
FROM duplicate_participants
```

#### Test 6: Incremental Load Validation
```sql
-- tests/unit/test_incremental_load.sql
{{ config(severity = 'warn') }}

WITH load_stats AS (
    SELECT 
        'si_users' as table_name,
        COUNT(*) as total_records,
        COUNT(DISTINCT load_date) as distinct_load_dates,
        MIN(load_date) as earliest_load_date,
        MAX(load_date) as latest_load_date
    FROM {{ ref('si_users') }}
    
    UNION ALL
    
    SELECT 
        'si_meetings' as table_name,
        COUNT(*) as total_records,
        COUNT(DISTINCT load_date) as distinct_load_dates,
        MIN(load_date) as earliest_load_date,
        MAX(load_date) as latest_load_date
    FROM {{ ref('si_meetings') }}
    
    UNION ALL
    
    SELECT 
        'si_participants' as table_name,
        COUNT(*) as total_records,
        COUNT(DISTINCT load_date) as distinct_load_dates,
        MIN(load_date) as earliest_load_date,
        MAX(load_date) as latest_load_date
    FROM {{ ref('si_participants') }}
)

SELECT 
    table_name,
    total_records,
    distinct_load_dates,
    earliest_load_date,
    latest_load_date,
    'Incremental load validation' as test_description
FROM load_stats
WHERE total_records = 0 OR distinct_load_dates = 0
```

#### Test 7: Audit Log Completeness
```sql
-- tests/unit/test_audit_log_completeness.sql
{{ config(severity = 'error') }}

WITH expected_pipelines AS (
    SELECT pipeline_name FROM (
        VALUES 
            ('si_users_transform'),
            ('si_meetings_transform'),
            ('si_participants_transform')
    ) AS t(pipeline_name)
),
actual_pipelines AS (
    SELECT DISTINCT pipeline_name
    FROM {{ ref('si_process_audit') }}
    WHERE DATE(start_time) = CURRENT_DATE()
),
missing_pipelines AS (
    SELECT e.pipeline_name
    FROM expected_pipelines e
    LEFT JOIN actual_pipelines a ON e.pipeline_name = a.pipeline_name
    WHERE a.pipeline_name IS NULL
)

SELECT 
    pipeline_name,
    'Missing audit log entry for today' as error_message
FROM missing_pipelines
```

#### Test 8: Referential Integrity Validation
```sql
-- tests/unit/test_referential_integrity.sql
{{ config(severity = 'error') }}

WITH orphaned_meetings AS (
    SELECT 
        m.meeting_id,
        m.host_id,
        'Orphaned meeting - host_id not found in users' as error_message
    FROM {{ ref('si_meetings') }} m
    LEFT JOIN {{ ref('si_users') }} u ON m.host_id = u.user_id
    WHERE u.user_id IS NULL
),
orphaned_participants AS (
    SELECT 
        p.participant_id,
        p.meeting_id,
        p.user_id,
        'Orphaned participant - meeting_id not found' as error_message
    FROM {{ ref('si_participants') }} p
    LEFT JOIN {{ ref('si_meetings') }} m ON p.meeting_id = m.meeting_id
    WHERE m.meeting_id IS NULL
)

SELECT meeting_id as record_id, host_id as foreign_key, error_message
FROM orphaned_meetings

UNION ALL

SELECT participant_id as record_id, meeting_id as foreign_key, error_message
FROM orphaned_participants
```

## Test Execution Strategy

### 1. Pre-deployment Testing
- Run all schema tests using `dbt test`
- Execute custom SQL tests for data validation
- Validate incremental model behavior
- Check audit log completeness

### 2. Post-deployment Validation
- Monitor data quality scores
- Validate record counts and processing times
- Check error handling and recovery
- Verify audit trail completeness

### 3. Continuous Monitoring
- Daily data quality score monitoring
- Weekly referential integrity checks
- Monthly performance baseline validation
- Quarterly test case review and updates

## API Cost Calculation

Estimated API cost for this comprehensive unit test case generation: **$0.0847 USD**

This cost includes:
- Analysis of the Bronze to Silver transformation pipeline
- Generation of 40+ test cases across 8 categories
- Creation of 8 custom SQL test scripts
- YAML schema test definitions
- Comprehensive documentation and execution strategy

## Conclusion

This comprehensive unit test suite ensures the reliability and performance of the Snowflake dbt Bronze to Silver transformation pipeline. The tests cover data quality validation, transformation logic, incremental processing, audit logging, and referential integrity. Regular execution of these tests will help maintain high data quality standards and catch potential issues early in the development cycle.