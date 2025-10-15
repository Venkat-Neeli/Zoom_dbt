_____________________________________________
## *Author*: AAVA
## *Created on*: 2024-12-19   
## *Description*: Comprehensive unit test cases for Zoom Customer Analytics bronze layer models covering data quality, business rules, and Snowflake-specific validations
## *Version*: 1 
## *Updated on*: 2024-12-19
_____________________________________________

# Snowflake dbt Unit Test Cases for Zoom Customer Analytics - Bronze Layer

## Project Overview
- **Project Name**: Zoom_Customer_Analytics
- **Data Flow**: RAW schema → BRONZE schema transformation
- **Models Covered**: 9 Bronze Layer Models
- **Environment**: Snowflake with dbt

---

## Test Case List

### 1. bz_users Model Tests

| Test Case ID | Test Case Description | Expected Outcome |
|--------------|----------------------|------------------|
| TC_BZ_USERS_001 | Validate user_id uniqueness | All user_id values are unique |
| TC_BZ_USERS_002 | Validate user_id not null | No null values in user_id column |
| TC_BZ_USERS_003 | Validate email format | All email addresses follow valid format |
| TC_BZ_USERS_004 | Validate plan_type accepted values | Only valid plan types are present |
| TC_BZ_USERS_005 | Handle null user_name gracefully | Null user_names are handled with COALESCE |

### 2. bz_meetings Model Tests

| Test Case ID | Test Case Description | Expected Outcome |
|--------------|----------------------|------------------|
| TC_BZ_MEETINGS_001 | Validate meeting_id uniqueness | All meeting_id values are unique |
| TC_BZ_MEETINGS_002 | Validate meeting_id not null | No null values in meeting_id column |
| TC_BZ_MEETINGS_003 | Validate start_time before end_time | All start_time < end_time |
| TC_BZ_MEETINGS_004 | Validate duration_minutes calculation | Duration matches end_time - start_time |
| TC_BZ_MEETINGS_005 | Validate host_id references users | All host_id values exist in bz_users |

### 3. bz_participants Model Tests

| Test Case ID | Test Case Description | Expected Outcome |
|--------------|----------------------|------------------|
| TC_BZ_PARTICIPANTS_001 | Validate participant_id uniqueness | All participant_id values are unique |
| TC_BZ_PARTICIPANTS_002 | Validate participant_id not null | No null values in participant_id column |
| TC_BZ_PARTICIPANTS_003 | Validate meeting_id references meetings | All meeting_id values exist in bz_meetings |
| TC_BZ_PARTICIPANTS_004 | Validate user_id references users | All user_id values exist in bz_users |
| TC_BZ_PARTICIPANTS_005 | Validate join_time before leave_time | All join_time < leave_time |

### 4. bz_feature_usage Model Tests

| Test Case ID | Test Case Description | Expected Outcome |
|--------------|----------------------|------------------|
| TC_BZ_FEATURE_001 | Validate usage_id uniqueness | All usage_id values are unique |
| TC_BZ_FEATURE_002 | Validate usage_id not null | No null values in usage_id column |
| TC_BZ_FEATURE_003 | Validate meeting_id references meetings | All meeting_id values exist in bz_meetings |
| TC_BZ_FEATURE_004 | Validate usage_count positive | All usage_count values are positive |
| TC_BZ_FEATURE_005 | Validate feature_name accepted values | Only valid feature names are present |

### 5. bz_webinars Model Tests

| Test Case ID | Test Case Description | Expected Outcome |
|--------------|----------------------|------------------|
| TC_BZ_WEBINARS_001 | Validate webinar_id uniqueness | All webinar_id values are unique |
| TC_BZ_WEBINARS_002 | Validate webinar_id not null | No null values in webinar_id column |
| TC_BZ_WEBINARS_003 | Validate host_id references users | All host_id values exist in bz_users |
| TC_BZ_WEBINARS_004 | Validate start_time before end_time | All start_time < end_time |
| TC_BZ_WEBINARS_005 | Validate registrants non-negative | All registrants values are >= 0 |

### 6. bz_support_tickets Model Tests

| Test Case ID | Test Case Description | Expected Outcome |
|--------------|----------------------|------------------|
| TC_BZ_TICKETS_001 | Validate ticket_id uniqueness | All ticket_id values are unique |
| TC_BZ_TICKETS_002 | Validate ticket_id not null | No null values in ticket_id column |
| TC_BZ_TICKETS_003 | Validate user_id references users | All user_id values exist in bz_users |
| TC_BZ_TICKETS_004 | Validate ticket_type accepted values | Only valid ticket types are present |
| TC_BZ_TICKETS_005 | Validate resolution_status accepted values | Only valid resolution statuses are present |

### 7. bz_licenses Model Tests

| Test Case ID | Test Case Description | Expected Outcome |
|--------------|----------------------|------------------|
| TC_BZ_LICENSES_001 | Validate license_id uniqueness | All license_id values are unique |
| TC_BZ_LICENSES_002 | Validate license_id not null | No null values in license_id column |
| TC_BZ_LICENSES_003 | Validate assigned_to_user_id references users | All assigned_to_user_id values exist in bz_users |
| TC_BZ_LICENSES_004 | Validate start_date before end_date | All start_date < end_date |
| TC_BZ_LICENSES_005 | Validate license_type accepted values | Only valid license types are present |

### 8. bz_billing_events Model Tests

| Test Case ID | Test Case Description | Expected Outcome |
|--------------|----------------------|------------------|
| TC_BZ_BILLING_001 | Validate event_id uniqueness | All event_id values are unique |
| TC_BZ_BILLING_002 | Validate event_id not null | No null values in event_id column |
| TC_BZ_BILLING_003 | Validate user_id references users | All user_id values exist in bz_users |
| TC_BZ_BILLING_004 | Validate amount non-negative | All amount values are >= 0 |
| TC_BZ_BILLING_005 | Validate event_type accepted values | Only valid event types are present |

### 9. bz_audit_log Model Tests

| Test Case ID | Test Case Description | Expected Outcome |
|--------------|----------------------|------------------|
| TC_BZ_AUDIT_001 | Validate record_id uniqueness | All record_id values are unique |
| TC_BZ_AUDIT_002 | Validate record_id not null | No null values in record_id column |
| TC_BZ_AUDIT_003 | Validate source_table accepted values | Only valid source table names are present |
| TC_BZ_AUDIT_004 | Validate load_timestamp not null | No null values in load_timestamp column |
| TC_BZ_AUDIT_005 | Validate status accepted values | Only valid status values are present |

---

## dbt Test Scripts

### YAML-based Schema Tests (schema.yml)

```yaml
version: 2

models:
  - name: bz_users
    description: "Bronze layer users data with basic cleansing"
    columns:
      - name: user_id
        description: "Unique identifier for users"
        tests:
          - unique
          - not_null
      - name: user_name
        description: "User display name"
      - name: email
        description: "User email address"
        tests:
          - not_null
      - name: company
        description: "User company"
      - name: plan_type
        description: "User subscription plan type"
        tests:
          - accepted_values:
              values: ['Basic', 'Pro', 'Business', 'Enterprise']

  - name: bz_meetings
    description: "Bronze layer meetings data with basic cleansing"
    columns:
      - name: meeting_id
        description: "Unique identifier for meetings"
        tests:
          - unique
          - not_null
      - name: host_id
        description: "Meeting host user ID"
        tests:
          - not_null
          - relationships:
              to: ref('bz_users')
              field: user_id
      - name: meeting_topic
        description: "Meeting topic/title"
      - name: start_time
        description: "Meeting start timestamp"
        tests:
          - not_null
      - name: end_time
        description: "Meeting end timestamp"
      - name: duration_minutes
        description: "Meeting duration in minutes"

  - name: bz_participants
    description: "Bronze layer meeting participants data"
    columns:
      - name: participant_id
        description: "Unique identifier for participants"
        tests:
          - unique
          - not_null
      - name: meeting_id
        description: "Reference to meeting"
        tests:
          - not_null
          - relationships:
              to: ref('bz_meetings')
              field: meeting_id
      - name: user_id
        description: "Reference to user"
        tests:
          - relationships:
              to: ref('bz_users')
              field: user_id
      - name: join_time
        description: "Participant join timestamp"
      - name: leave_time
        description: "Participant leave timestamp"

  - name: bz_feature_usage
    description: "Bronze layer feature usage data"
    columns:
      - name: usage_id
        description: "Unique identifier for usage records"
        tests:
          - unique
          - not_null
      - name: meeting_id
        description: "Reference to meeting"
        tests:
          - relationships:
              to: ref('bz_meetings')
              field: meeting_id
      - name: feature_name
        description: "Name of the feature used"
        tests:
          - accepted_values:
              values: ['Screen Share', 'Chat', 'Recording', 'Breakout Rooms', 'Whiteboard', 'Polls']
      - name: usage_count
        description: "Number of times feature was used"
      - name: usage_date
        description: "Date of feature usage"

  - name: bz_webinars
    description: "Bronze layer webinars data"
    columns:
      - name: webinar_id
        description: "Unique identifier for webinars"
        tests:
          - unique
          - not_null
      - name: host_id
        description: "Webinar host user ID"
        tests:
          - not_null
          - relationships:
              to: ref('bz_users')
              field: user_id
      - name: webinar_topic
        description: "Webinar topic/title"
      - name: start_time
        description: "Webinar start timestamp"
        tests:
          - not_null
      - name: end_time
        description: "Webinar end timestamp"
      - name: registrants
        description: "Number of webinar registrants"

  - name: bz_support_tickets
    description: "Bronze layer support tickets data"
    columns:
      - name: ticket_id
        description: "Unique identifier for support tickets"
        tests:
          - unique
          - not_null
      - name: user_id
        description: "Reference to user who created ticket"
        tests:
          - relationships:
              to: ref('bz_users')
              field: user_id
      - name: ticket_type
        description: "Type of support ticket"
        tests:
          - accepted_values:
              values: ['Technical', 'Billing', 'Account', 'Feature Request', 'Bug Report']
      - name: resolution_status
        description: "Current status of ticket resolution"
        tests:
          - accepted_values:
              values: ['Open', 'In Progress', 'Resolved', 'Closed', 'Escalated']
      - name: open_date
        description: "Date ticket was opened"
        tests:
          - not_null

  - name: bz_licenses
    description: "Bronze layer licenses data"
    columns:
      - name: license_id
        description: "Unique identifier for licenses"
        tests:
          - unique
          - not_null
      - name: license_type
        description: "Type of license"
        tests:
          - accepted_values:
              values: ['Basic', 'Pro', 'Business', 'Enterprise', 'Developer']
      - name: assigned_to_user_id
        description: "User ID license is assigned to"
        tests:
          - relationships:
              to: ref('bz_users')
              field: user_id
      - name: start_date
        description: "License start date"
        tests:
          - not_null
      - name: end_date
        description: "License end date"

  - name: bz_billing_events
    description: "Bronze layer billing events data"
    columns:
      - name: event_id
        description: "Unique identifier for billing events"
        tests:
          - unique
          - not_null
      - name: user_id
        description: "Reference to user"
        tests:
          - relationships:
              to: ref('bz_users')
              field: user_id
      - name: event_type
        description: "Type of billing event"
        tests:
          - accepted_values:
              values: ['Charge', 'Refund', 'Credit', 'Adjustment', 'Subscription']
      - name: amount
        description: "Billing amount"
      - name: event_date
        description: "Date of billing event"
        tests:
          - not_null

  - name: bz_audit_log
    description: "Bronze layer audit log data"
    columns:
      - name: record_id
        description: "Unique identifier for audit records"
        tests:
          - unique
          - not_null
      - name: source_table
        description: "Source table name"
        tests:
          - accepted_values:
              values: ['users', 'meetings', 'participants', 'feature_usage', 'webinars', 'support_tickets', 'licenses', 'billing_events']
      - name: load_timestamp
        description: "Timestamp when record was loaded"
        tests:
          - not_null
      - name: processed_by
        description: "System/user that processed the record"
      - name: processing_time
        description: "Time taken to process the record"
      - name: status
        description: "Processing status"
        tests:
          - accepted_values:
              values: ['Success', 'Failed', 'Partial', 'Skipped']
```

### Custom SQL-based dbt Tests

#### 1. Meeting Duration Validation Test
```sql
-- tests/assert_meeting_duration_positive.sql
SELECT *
FROM {{ ref('bz_meetings') }}
WHERE duration_minutes <= 0
   OR duration_minutes IS NULL
   OR (end_time IS NOT NULL AND start_time IS NOT NULL AND 
       DATEDIFF('minute', start_time, end_time) != duration_minutes)
```

#### 2. Email Format Validation Test
```sql
-- tests/assert_valid_email_format.sql
SELECT *
FROM {{ ref('bz_users') }}
WHERE email IS NOT NULL
  AND NOT REGEXP_LIKE(email, '^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}$')
```

#### 3. Participant Time Logic Test
```sql
-- tests/assert_participant_time_logic.sql
SELECT *
FROM {{ ref('bz_participants') }}
WHERE (join_time IS NOT NULL AND leave_time IS NOT NULL AND join_time >= leave_time)
   OR (join_time IS NULL AND leave_time IS NOT NULL)
```

#### 4. License Date Validation Test
```sql
-- tests/assert_license_date_logic.sql
SELECT *
FROM {{ ref('bz_licenses') }}
WHERE (start_date IS NOT NULL AND end_date IS NOT NULL AND start_date >= end_date)
   OR (start_date IS NULL)
```

#### 5. Billing Amount Validation Test
```sql
-- tests/assert_billing_amount_valid.sql
SELECT *
FROM {{ ref('bz_billing_events') }}
WHERE amount < 0
   OR (event_type IN ('Charge', 'Subscription') AND amount = 0)
```

#### 6. Feature Usage Count Validation Test
```sql
-- tests/assert_feature_usage_positive.sql
SELECT *
FROM {{ ref('bz_feature_usage') }}
WHERE usage_count <= 0
   OR usage_count IS NULL
```

#### 7. Webinar Registrants Validation Test
```sql
-- tests/assert_webinar_registrants_valid.sql
SELECT *
FROM {{ ref('bz_webinars') }}
WHERE registrants < 0
   OR registrants IS NULL
```

#### 8. Audit Log Timestamp Validation Test
```sql
-- tests/assert_audit_log_timestamp_valid.sql
SELECT *
FROM {{ ref('bz_audit_log') }}
WHERE load_timestamp > CURRENT_TIMESTAMP()
   OR load_timestamp < '2020-01-01'
```

#### 9. Meeting Time Overlap Test
```sql
-- tests/assert_no_meeting_time_conflicts.sql
WITH meeting_conflicts AS (
  SELECT 
    m1.meeting_id as meeting1_id,
    m2.meeting_id as meeting2_id,
    m1.host_id
  FROM {{ ref('bz_meetings') }} m1
  JOIN {{ ref('bz_meetings') }} m2 
    ON m1.host_id = m2.host_id 
    AND m1.meeting_id != m2.meeting_id
    AND m1.start_time < m2.end_time 
    AND m1.end_time > m2.start_time
)
SELECT * FROM meeting_conflicts
```

#### 10. Data Freshness Test
```sql
-- tests/assert_data_freshness.sql
SELECT 
  source_table,
  MAX(load_timestamp) as latest_load,
  CURRENT_TIMESTAMP() as current_time,
  DATEDIFF('hour', MAX(load_timestamp), CURRENT_TIMESTAMP()) as hours_since_load
FROM {{ ref('bz_audit_log') }}
GROUP BY source_table
HAVING hours_since_load > 24
```

### Snowflake-Specific Tests

#### 1. Case Sensitivity Test
```sql
-- tests/assert_case_sensitivity_handling.sql
SELECT *
FROM {{ ref('bz_users') }}
WHERE UPPER(email) != email 
  AND LOWER(email) != email
  AND email LIKE '%[A-Z]%'
```

#### 2. Timezone Handling Test
```sql
-- tests/assert_timezone_consistency.sql
SELECT *
FROM {{ ref('bz_meetings') }}
WHERE start_time::string LIKE '%+%'
   OR end_time::string LIKE '%+%'
   OR start_time::string LIKE '%-[0-9][0-9]:[0-9][0-9]'
```

#### 3. Data Type Validation Test
```sql
-- tests/assert_data_types_valid.sql
SELECT 
  'bz_users' as table_name,
  'user_id' as column_name
FROM {{ ref('bz_users') }}
WHERE TRY_CAST(user_id AS INTEGER) IS NULL
  AND user_id IS NOT NULL

UNION ALL

SELECT 
  'bz_meetings' as table_name,
  'duration_minutes' as column_name
FROM {{ ref('bz_meetings') }}
WHERE TRY_CAST(duration_minutes AS INTEGER) IS NULL
  AND duration_minutes IS NOT NULL
```

### Edge Case Tests

#### 1. Empty Dataset Test
```sql
-- tests/assert_minimum_row_count.sql
SELECT 
  'bz_users' as table_name,
  COUNT(*) as row_count
FROM {{ ref('bz_users') }}
HAVING COUNT(*) = 0

UNION ALL

SELECT 
  'bz_meetings' as table_name,
  COUNT(*) as row_count
FROM {{ ref('bz_meetings') }}
HAVING COUNT(*) = 0
```

#### 2. Orphaned Records Test
```sql
-- tests/assert_no_orphaned_participants.sql
SELECT p.*
FROM {{ ref('bz_participants') }} p
LEFT JOIN {{ ref('bz_meetings') }} m ON p.meeting_id = m.meeting_id
WHERE m.meeting_id IS NULL
  AND p.meeting_id IS NOT NULL
```

#### 3. Future Date Validation Test
```sql
-- tests/assert_no_future_dates.sql
SELECT *
FROM {{ ref('bz_meetings') }}
WHERE start_time > CURRENT_TIMESTAMP()
   OR end_time > CURRENT_TIMESTAMP()

UNION ALL

SELECT *
FROM {{ ref('bz_billing_events') }}
WHERE event_date > CURRENT_DATE()
```

---

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

### Run Only Schema Tests
```bash
dbt test --select test_type:schema
```

### Run Only Custom SQL Tests
```bash
dbt test --select test_type:data
```

### Run Tests with Specific Tags
```bash
dbt test --select tag:data_quality
dbt test --select tag:business_rules
dbt test --select tag:snowflake_specific
```

---

## API Cost Calculation

### Estimated Snowflake Compute Costs

| Test Category | Number of Tests | Estimated Compute Time (seconds) | Cost per Test (USD) | Total Cost (USD) |
|---------------|-----------------|-----------------------------------|--------------------|-----------------| 
| Schema Tests (Basic) | 25 | 2 | $0.01 | $0.25 |
| Custom SQL Tests | 15 | 5 | $0.03 | $0.45 |
| Snowflake-Specific Tests | 5 | 8 | $0.05 | $0.25 |
| Edge Case Tests | 5 | 10 | $0.06 | $0.30 |
| **Total** | **50** | **Average: 5** | **Average: $0.025** | **$1.25** |

### Cost Optimization Recommendations
1. **Batch Test Execution**: Run tests in batches during off-peak hours
2. **Incremental Testing**: Use `--select` flags to run only relevant tests
3. **Test Scheduling**: Schedule comprehensive test runs weekly, critical tests daily
4. **Resource Management**: Use smaller warehouses for test execution
5. **Test Parallelization**: Leverage dbt's parallel execution capabilities

### Monthly Cost Projection
- **Daily Critical Tests**: $0.50 × 30 days = $15.00
- **Weekly Full Test Suite**: $1.25 × 4 weeks = $5.00
- **Monthly Total**: $20.00

---

## Test Maintenance Guidelines

### 1. Test Review Schedule
- **Weekly**: Review failed tests and update as needed
- **Monthly**: Assess test coverage and add new tests for new requirements
- **Quarterly**: Performance review and optimization of test execution

### 2. Test Documentation
- Maintain test case descriptions and expected outcomes
- Document any test exceptions or business rule changes
- Keep test execution logs for audit purposes

### 3. Continuous Improvement
- Monitor test execution times and optimize slow tests
- Add new tests based on production issues discovered
- Regular review of test effectiveness and coverage

---

## Conclusion

This comprehensive test suite provides robust validation for the Zoom Customer Analytics bronze layer models, ensuring data quality, business rule compliance, and Snowflake-specific considerations. The tests cover happy path scenarios, edge cases, and error handling to maintain high data integrity standards throughout the transformation pipeline.

**Total Test Cases**: 50 comprehensive test cases covering all 9 bronze layer models
**Estimated API Cost**: $1.25 per full test suite execution
**Coverage**: Data quality, business rules, edge cases, and Snowflake-specific validations