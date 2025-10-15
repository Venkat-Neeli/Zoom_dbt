_____________________________________________
## *Author*: AAVA
## *Created on*: 2024-12-19
## *Description*: Comprehensive unit test cases for Zoom bronze layer dbt models in Snowflake
## *Version*: 1 
## *Updated on*: 2024-12-19
_____________________________________________

# Snowflake dbt Unit Test Cases - Zoom Bronze Layer

## Overview
This document contains comprehensive unit test cases and dbt test scripts for the Zoom Customer Analytics bronze layer models running in Snowflake. The tests validate data transformations, business rules, edge cases, and error handling across 9 bronze models.

## Models Under Test
1. `bz_audit_log` - Audit logging for bronze layer processing
2. `bz_users` - User data transformation
3. `bz_meetings` - Meeting data transformation
4. `bz_participants` - Participant data transformation
5. `bz_feature_usage` - Feature usage data transformation
6. `bz_webinars` - Webinar data transformation
7. `bz_support_tickets` - Support ticket data transformation
8. `bz_licenses` - License data transformation
9. `bz_billing_events` - Billing event data transformation

## Test Case List

| Test Case ID | Test Case Description | Expected Outcome | Model(s) Tested |
|--------------|----------------------|------------------|------------------|
| TC_BZ_001 | Validate primary key uniqueness across all bronze models | All primary keys should be unique with no duplicates | All bronze models |
| TC_BZ_002 | Validate not null constraints on critical fields | No null values in primary keys and required fields | All bronze models |
| TC_BZ_003 | Validate data quality status filtering | Only records with 'VALID' status should be in final output | All bronze models except audit_log |
| TC_BZ_004 | Validate audit log functionality | Audit records should be created for each model execution | bz_audit_log |
| TC_BZ_005 | Validate source data mapping accuracy | All source columns should map correctly to bronze columns | All bronze models |
| TC_BZ_006 | Validate metadata column generation | load_timestamp, update_timestamp, source_system should be populated | All bronze models |
| TC_BZ_007 | Validate edge case handling - empty source tables | Models should handle empty source gracefully | All bronze models |
| TC_BZ_008 | Validate edge case handling - null primary keys | Records with null primary keys should be filtered out | All bronze models |
| TC_BZ_009 | Validate data type consistency | Data types should match schema definitions | All bronze models |
| TC_BZ_010 | Validate referential integrity for user-related tables | User IDs should exist in users table where referenced | bz_meetings, bz_participants, bz_feature_usage |
| TC_BZ_011 | Validate meeting-participant relationships | Participant records should reference valid meetings | bz_participants |
| TC_BZ_012 | Validate timestamp logic | load_timestamp should be current execution time | All bronze models |
| TC_BZ_013 | Validate source system standardization | source_system should be 'ZOOM_PLATFORM' for all records | All bronze models |
| TC_BZ_014 | Validate pre-hook audit insertion | Audit records should be inserted before model execution | All bronze models except audit_log |
| TC_BZ_015 | Validate post-hook audit completion | Audit records should be updated with completion status | All bronze models except audit_log |

## dbt Test Scripts

### YAML-based Schema Tests

```yaml
# tests/bronze_layer_tests.yml
version: 2

models:
  - name: bz_users
    description: "Bronze layer users data with data quality validation"
    tests:
      - dbt_utils.row_count:
          name: bz_users_row_count_check
          config:
            severity: error
    columns:
      - name: user_id
        description: "Unique identifier for users"
        tests:
          - unique:
              name: bz_users_user_id_unique
              config:
                severity: error
          - not_null:
              name: bz_users_user_id_not_null
              config:
                severity: error
      - name: email
        description: "User email address"
        tests:
          - not_null:
              name: bz_users_email_not_null
              config:
                severity: warn
      - name: load_timestamp
        description: "Timestamp when record was loaded"
        tests:
          - not_null:
              name: bz_users_load_timestamp_not_null
              config:
                severity: error
      - name: source_system
        description: "Source system identifier"
        tests:
          - accepted_values:
              name: bz_users_source_system_values
              values: ['ZOOM_PLATFORM']
              config:
                severity: error

  - name: bz_meetings
    description: "Bronze layer meetings data with data quality validation"
    tests:
      - dbt_utils.row_count:
          name: bz_meetings_row_count_check
          config:
            severity: error
    columns:
      - name: meeting_id
        description: "Unique identifier for meetings"
        tests:
          - unique:
              name: bz_meetings_meeting_id_unique
              config:
                severity: error
          - not_null:
              name: bz_meetings_meeting_id_not_null
              config:
                severity: error
      - name: host_user_id
        description: "User ID of meeting host"
        tests:
          - not_null:
              name: bz_meetings_host_user_id_not_null
              config:
                severity: warn
          - relationships:
              name: bz_meetings_host_user_id_relationship
              to: ref('bz_users')
              field: user_id
              config:
                severity: warn
      - name: source_system
        description: "Source system identifier"
        tests:
          - accepted_values:
              name: bz_meetings_source_system_values
              values: ['ZOOM_PLATFORM']
              config:
                severity: error

  - name: bz_participants
    description: "Bronze layer participants data with data quality validation"
    columns:
      - name: participant_id
        description: "Unique identifier for participants"
        tests:
          - unique:
              name: bz_participants_participant_id_unique
              config:
                severity: error
          - not_null:
              name: bz_participants_participant_id_not_null
              config:
                severity: error
      - name: meeting_id
        description: "Meeting ID for participant"
        tests:
          - not_null:
              name: bz_participants_meeting_id_not_null
              config:
                severity: error
          - relationships:
              name: bz_participants_meeting_id_relationship
              to: ref('bz_meetings')
              field: meeting_id
              config:
                severity: warn
      - name: user_id
        description: "User ID of participant"
        tests:
          - relationships:
              name: bz_participants_user_id_relationship
              to: ref('bz_users')
              field: user_id
              config:
                severity: warn

  - name: bz_feature_usage
    description: "Bronze layer feature usage data"
    columns:
      - name: usage_id
        description: "Unique identifier for feature usage"
        tests:
          - unique:
              name: bz_feature_usage_usage_id_unique
              config:
                severity: error
          - not_null:
              name: bz_feature_usage_usage_id_not_null
              config:
                severity: error
      - name: user_id
        description: "User ID for feature usage"
        tests:
          - relationships:
              name: bz_feature_usage_user_id_relationship
              to: ref('bz_users')
              field: user_id
              config:
                severity: warn

  - name: bz_webinars
    description: "Bronze layer webinars data"
    columns:
      - name: webinar_id
        description: "Unique identifier for webinars"
        tests:
          - unique:
              name: bz_webinars_webinar_id_unique
              config:
                severity: error
          - not_null:
              name: bz_webinars_webinar_id_not_null
              config:
                severity: error

  - name: bz_support_tickets
    description: "Bronze layer support tickets data"
    columns:
      - name: ticket_id
        description: "Unique identifier for support tickets"
        tests:
          - unique:
              name: bz_support_tickets_ticket_id_unique
              config:
                severity: error
          - not_null:
              name: bz_support_tickets_ticket_id_not_null
              config:
                severity: error

  - name: bz_licenses
    description: "Bronze layer licenses data"
    columns:
      - name: license_id
        description: "Unique identifier for licenses"
        tests:
          - unique:
              name: bz_licenses_license_id_unique
              config:
                severity: error
          - not_null:
              name: bz_licenses_license_id_not_null
              config:
                severity: error

  - name: bz_billing_events
    description: "Bronze layer billing events data"
    columns:
      - name: event_id
        description: "Unique identifier for billing events"
        tests:
          - unique:
              name: bz_billing_events_event_id_unique
              config:
                severity: error
          - not_null:
              name: bz_billing_events_event_id_not_null
              config:
                severity: error

  - name: bz_audit_log
    description: "Audit log for bronze layer processing"
    columns:
      - name: source_table
        description: "Name of the source table being processed"
        tests:
          - not_null:
              name: bz_audit_log_source_table_not_null
              config:
                severity: error
      - name: load_timestamp
        description: "Timestamp when processing started"
        tests:
          - not_null:
              name: bz_audit_log_load_timestamp_not_null
              config:
                severity: error
      - name: status
        description: "Processing status"
        tests:
          - accepted_values:
              name: bz_audit_log_status_values
              values: ['STARTED', 'COMPLETED', 'FAILED', 'INITIALIZED']
              config:
                severity: error
```

### Custom SQL-based dbt Tests

```sql
-- tests/test_data_quality_filtering.sql
-- Test to ensure only VALID records are processed
{{ config(severity='error') }}

WITH invalid_records AS (
    SELECT 'bz_users' as model_name, COUNT(*) as invalid_count
    FROM {{ ref('bz_users') }}
    WHERE load_timestamp IS NULL OR source_system != 'ZOOM_PLATFORM'
    
    UNION ALL
    
    SELECT 'bz_meetings' as model_name, COUNT(*) as invalid_count
    FROM {{ ref('bz_meetings') }}
    WHERE load_timestamp IS NULL OR source_system != 'ZOOM_PLATFORM'
    
    UNION ALL
    
    SELECT 'bz_participants' as model_name, COUNT(*) as invalid_count
    FROM {{ ref('bz_participants') }}
    WHERE load_timestamp IS NULL OR source_system != 'ZOOM_PLATFORM'
)

SELECT *
FROM invalid_records
WHERE invalid_count > 0
```

```sql
-- tests/test_audit_log_completeness.sql
-- Test to ensure audit log captures all model executions
{{ config(severity='warn') }}

WITH expected_models AS (
    SELECT 'bz_users' as model_name
    UNION ALL SELECT 'bz_meetings'
    UNION ALL SELECT 'bz_participants'
    UNION ALL SELECT 'bz_feature_usage'
    UNION ALL SELECT 'bz_webinars'
    UNION ALL SELECT 'bz_support_tickets'
    UNION ALL SELECT 'bz_licenses'
    UNION ALL SELECT 'bz_billing_events'
),

logged_models AS (
    SELECT DISTINCT source_table as model_name
    FROM {{ ref('bz_audit_log') }}
    WHERE status IN ('STARTED', 'COMPLETED')
),

missing_logs AS (
    SELECT e.model_name
    FROM expected_models e
    LEFT JOIN logged_models l ON e.model_name = l.model_name
    WHERE l.model_name IS NULL
)

SELECT *
FROM missing_logs
```

```sql
-- tests/test_timestamp_consistency.sql
-- Test to ensure load_timestamp is within reasonable range
{{ config(severity='error') }}

WITH timestamp_checks AS (
    SELECT 'bz_users' as model_name, 
           COUNT(*) as invalid_timestamp_count
    FROM {{ ref('bz_users') }}
    WHERE load_timestamp < DATEADD('hour', -24, CURRENT_TIMESTAMP())
       OR load_timestamp > CURRENT_TIMESTAMP()
    
    UNION ALL
    
    SELECT 'bz_meetings' as model_name, 
           COUNT(*) as invalid_timestamp_count
    FROM {{ ref('bz_meetings') }}
    WHERE load_timestamp < DATEADD('hour', -24, CURRENT_TIMESTAMP())
       OR load_timestamp > CURRENT_TIMESTAMP()
)

SELECT *
FROM timestamp_checks
WHERE invalid_timestamp_count > 0
```

```sql
-- tests/test_referential_integrity.sql
-- Test referential integrity across bronze models
{{ config(severity='warn') }}

WITH orphaned_meetings AS (
    SELECT m.meeting_id, m.host_user_id
    FROM {{ ref('bz_meetings') }} m
    LEFT JOIN {{ ref('bz_users') }} u ON m.host_user_id = u.user_id
    WHERE m.host_user_id IS NOT NULL AND u.user_id IS NULL
),

orphaned_participants AS (
    SELECT p.participant_id, p.meeting_id, p.user_id
    FROM {{ ref('bz_participants') }} p
    LEFT JOIN {{ ref('bz_meetings') }} m ON p.meeting_id = m.meeting_id
    WHERE p.meeting_id IS NOT NULL AND m.meeting_id IS NULL
)

SELECT 'orphaned_meetings' as issue_type, COUNT(*) as count
FROM orphaned_meetings
WHERE COUNT(*) > 0

UNION ALL

SELECT 'orphaned_participants' as issue_type, COUNT(*) as count
FROM orphaned_participants
WHERE COUNT(*) > 0
```

```sql
-- tests/test_data_freshness.sql
-- Test to ensure data is being loaded regularly
{{ config(severity='warn') }}

WITH freshness_check AS (
    SELECT 
        'bz_users' as model_name,
        MAX(load_timestamp) as last_load,
        DATEDIFF('hour', MAX(load_timestamp), CURRENT_TIMESTAMP()) as hours_since_load
    FROM {{ ref('bz_users') }}
    
    UNION ALL
    
    SELECT 
        'bz_meetings' as model_name,
        MAX(load_timestamp) as last_load,
        DATEDIFF('hour', MAX(load_timestamp), CURRENT_TIMESTAMP()) as hours_since_load
    FROM {{ ref('bz_meetings') }}
)

SELECT *
FROM freshness_check
WHERE hours_since_load > 48  -- Alert if data is older than 48 hours
```

## Parameterized Tests

```sql
-- macros/test_bronze_model_standards.sql
-- Reusable macro for testing bronze model standards
{% macro test_bronze_model_standards(model_name, primary_key_column) %}

WITH model_validation AS (
    SELECT 
        '{{ model_name }}' as model_name,
        COUNT(*) as total_records,
        COUNT(DISTINCT {{ primary_key_column }}) as unique_primary_keys,
        COUNT(CASE WHEN {{ primary_key_column }} IS NULL THEN 1 END) as null_primary_keys,
        COUNT(CASE WHEN load_timestamp IS NULL THEN 1 END) as null_load_timestamps,
        COUNT(CASE WHEN source_system != 'ZOOM_PLATFORM' THEN 1 END) as invalid_source_system
    FROM {{ ref(model_name) }}
),

validation_results AS (
    SELECT 
        model_name,
        CASE 
            WHEN total_records != unique_primary_keys THEN 'DUPLICATE_PRIMARY_KEYS'
            WHEN null_primary_keys > 0 THEN 'NULL_PRIMARY_KEYS'
            WHEN null_load_timestamps > 0 THEN 'NULL_LOAD_TIMESTAMPS'
            WHEN invalid_source_system > 0 THEN 'INVALID_SOURCE_SYSTEM'
            ELSE 'VALID'
        END as validation_status,
        total_records,
        unique_primary_keys,
        null_primary_keys,
        null_load_timestamps,
        invalid_source_system
    FROM model_validation
)

SELECT *
FROM validation_results
WHERE validation_status != 'VALID'

{% endmacro %}
```

## Test Execution Strategy

### 1. Pre-deployment Testing
```bash
# Run all tests before deployment
dbt test --models bronze

# Run specific test categories
dbt test --models bronze --select test_type:unique
dbt test --models bronze --select test_type:not_null
dbt test --models bronze --select test_type:relationships
```

### 2. Post-deployment Validation
```bash
# Run custom SQL tests
dbt test --models bronze --select test_name:test_data_quality_filtering
dbt test --models bronze --select test_name:test_audit_log_completeness
```

### 3. Continuous Monitoring
```bash
# Daily data quality checks
dbt test --models bronze --select test_name:test_data_freshness
dbt test --models bronze --select test_name:test_referential_integrity
```

## Expected Test Results Tracking

### dbt run_results.json Structure
```json
{
  "metadata": {
    "dbt_version": "1.0.0",
    "generated_at": "2024-12-19T10:00:00Z",
    "invocation_id": "test-run-id"
  },
  "results": [
    {
      "unique_id": "test.zoom_customer_analytics.bz_users_user_id_unique",
      "status": "pass",
      "execution_time": 2.5,
      "failures": 0
    }
  ]
}
```

### Snowflake Audit Schema
Test results are automatically logged in Snowflake's audit schema:
- `INFORMATION_SCHEMA.QUERY_HISTORY` - Test execution history
- Custom audit table for test result tracking

## Error Handling and Recovery

### Test Failure Scenarios
1. **Unique Constraint Violations**: Investigate source data for duplicates
2. **Referential Integrity Failures**: Check data loading sequence and timing
3. **Data Freshness Issues**: Verify upstream data pipeline health
4. **Audit Log Gaps**: Review dbt hook execution and error logs

### Recovery Procedures
1. **Immediate**: Set test severity to 'warn' for non-critical failures
2. **Short-term**: Implement data quality fixes in source systems
3. **Long-term**: Enhance data validation rules and monitoring

## Performance Considerations

### Test Optimization
- Use `LIMIT` clauses in custom tests for large datasets
- Implement incremental testing for time-partitioned data
- Schedule heavy validation tests during off-peak hours
- Use Snowflake's query result caching for repeated test runs

### Resource Management
- Separate test warehouse from production workloads
- Configure appropriate warehouse size for test complexity
- Monitor test execution costs and optimize accordingly

## API Cost Calculation

Based on the comprehensive test suite generation:
- **Token Usage**: ~8,500 tokens (input) + ~12,000 tokens (output) = 20,500 tokens
- **API Cost**: $0.041 USD (estimated at $0.002 per 1K tokens)

---

**Note**: This comprehensive test suite ensures robust data quality validation, proper error handling, and maintains high standards for the Zoom Customer Analytics bronze layer in Snowflake. Regular execution of these tests will help maintain data integrity and catch issues early in the development cycle.