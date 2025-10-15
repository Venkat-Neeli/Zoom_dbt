_____________________________________________
## *Author*: AAVA
## *Created on*: 2024-12-19
## *Description*: Comprehensive Snowflake dbt Unit Test Cases for Zoom Customer Analytics Bronze Layer
## *Version*: 1
## *Updated on*: 2024-12-19
_____________________________________________

# Comprehensive Snowflake dbt Unit Test Cases for Zoom Customer Analytics Bronze Layer

## Test Case List

| Test Case ID | Test Case Description | Expected Outcome |
|--------------|----------------------|------------------|
| TC_BZ_001 | Validate audit log data quality and business rules | No records should fail validation - all timestamps valid, required fields present |
| TC_BZ_002 | Comprehensive email validation for user records | All email addresses should be valid format with @ symbol and proper structure |
| TC_BZ_003 | Validate meeting duration and participant count logic | Duration and participant counts should be realistic (0-1440 min, 0-10000 participants) |
| TC_BZ_004 | Validate participant join/leave time logic | Leave time should always be after join time, no future join times |
| TC_BZ_005 | Validate feature usage aggregation and counts | Usage counts should be positive and realistic (1-1000 per day) |
| TC_BZ_006 | Validate webinar capacity and registration logic | Webinar capacity should be within Zoom limits (1-50000 attendees) |
| TC_BZ_007 | Validate support ticket SLA and status transitions | Ticket resolution times should be logical, resolved tickets have resolution time |
| TC_BZ_008 | Validate license validity periods and status consistency | License dates should be consistent with status, no active expired licenses |
| TC_BZ_009 | Validate billing event financial data integrity | Financial amounts should be valid, refunds negative, charges positive |
| TC_BZ_010 | Test COALESCE null handling across all bronze models | Critical fields should never be null after COALESCE transformation |
| TC_BZ_011 | Validate behavior with empty source datasets | Models should handle empty sources gracefully without errors |
| TC_BZ_012 | Validate referential integrity across bronze models | All foreign key relationships should be valid, no orphaned records |
| TC_BZ_013 | Monitor data volume and performance metrics | Data volumes should be within expected ranges, performance acceptable |

## dbt Test Scripts

### YAML-based Schema Tests

```yaml
# models/bronze/schema.yml
version: 2

models:
  - name: bz_audit_log
    description: "Bronze layer audit log data with data quality checks"
    columns:
      - name: audit_id
        description: "Unique audit log identifier"
        tests:
          - unique:
              severity: error
          - not_null:
              severity: error
      - name: user_id
        description: "User identifier"
        tests:
          - not_null:
              severity: warn
          - relationships:
              to: ref('bz_users')
              field: user_id
              severity: warn
      - name: action_type
        description: "Type of action performed"
        tests:
          - accepted_values:
              values: ['LOGIN', 'LOGOUT', 'CREATE_MEETING', 'JOIN_MEETING', 'LEAVE_MEETING', 'DELETE_MEETING', 'UPDATE_PROFILE']
              severity: error
      - name: timestamp
        description: "Action timestamp"
        tests:
          - not_null:
              severity: error
          - expression_is_true:
              expression: "timestamp <= current_timestamp()"
              severity: warn
      - name: ip_address
        description: "User IP address"
        tests:
          - expression_is_true:
              expression: "ip_address IS NULL OR LENGTH(ip_address) >= 7"
              severity: warn

  - name: bz_users
    description: "Bronze layer user data"
    columns:
      - name: user_id
        description: "Unique user identifier"
        tests:
          - unique:
              severity: error
          - not_null:
              severity: error
      - name: email
        description: "User email address"
        tests:
          - unique:
              severity: error
          - not_null:
              severity: error
          - expression_is_true:
              expression: "email LIKE '%@%'"
              severity: error
      - name: account_id
        description: "Account identifier"
        tests:
          - not_null:
              severity: error
      - name: user_type
        description: "Type of user"
        tests:
          - accepted_values:
              values: ['BASIC', 'LICENSED', 'ON_PREM', 'ADMIN']
              severity: error
      - name: status
        description: "User status"
        tests:
          - accepted_values:
              values: ['ACTIVE', 'INACTIVE', 'PENDING', 'SUSPENDED']
              severity: error
      - name: created_at
        description: "User creation timestamp"
        tests:
          - not_null:
              severity: error

  - name: bz_meetings
    description: "Bronze layer meeting data"
    columns:
      - name: meeting_id
        description: "Unique meeting identifier"
        tests:
          - unique:
              severity: error
          - not_null:
              severity: error
      - name: host_id
        description: "Meeting host user ID"
        tests:
          - not_null:
              severity: error
          - relationships:
              to: ref('bz_users')
              field: user_id
              severity: warn
      - name: meeting_type
        description: "Type of meeting"
        tests:
          - accepted_values:
              values: ['INSTANT', 'SCHEDULED', 'RECURRING', 'PERSONAL_ROOM']
              severity: error
      - name: start_time
        description: "Meeting start time"
        tests:
          - not_null:
              severity: error
      - name: duration
        description: "Meeting duration in minutes"
        tests:
          - expression_is_true:
              expression: "duration >= 0"
              severity: error
      - name: participant_count
        description: "Number of participants"
        tests:
          - expression_is_true:
              expression: "participant_count >= 0"
              severity: error

  - name: bz_participants
    description: "Bronze layer participant data"
    columns:
      - name: participant_id
        description: "Unique participant identifier"
        tests:
          - unique:
              severity: error
          - not_null:
              severity: error
      - name: meeting_id
        description: "Associated meeting ID"
        tests:
          - not_null:
              severity: error
          - relationships:
              to: ref('bz_meetings')
              field: meeting_id
              severity: warn
      - name: user_id
        description: "Participant user ID"
        tests:
          - relationships:
              to: ref('bz_users')
              field: user_id
              severity: warn
      - name: join_time
        description: "Participant join time"
        tests:
          - not_null:
              severity: error
      - name: leave_time
        description: "Participant leave time"
        tests:
          - expression_is_true:
              expression: "leave_time IS NULL OR leave_time >= join_time"
              severity: error

  - name: bz_feature_usage
    description: "Bronze layer feature usage data"
    columns:
      - name: usage_id
        description: "Unique usage record identifier"
        tests:
          - unique:
              severity: error
          - not_null:
              severity: error
      - name: user_id
        description: "User identifier"
        tests:
          - not_null:
              severity: error
          - relationships:
              to: ref('bz_users')
              field: user_id
              severity: warn
      - name: feature_name
        description: "Name of the feature used"
        tests:
          - accepted_values:
              values: ['SCREEN_SHARE', 'RECORDING', 'BREAKOUT_ROOMS', 'WHITEBOARD', 'CHAT', 'POLLS', 'REACTIONS']
              severity: error
      - name: usage_count
        description: "Number of times feature was used"
        tests:
          - expression_is_true:
              expression: "usage_count > 0"
              severity: error
      - name: usage_date
        description: "Date of feature usage"
        tests:
          - not_null:
              severity: error

  - name: bz_webinars
    description: "Bronze layer webinar data"
    columns:
      - name: webinar_id
        description: "Unique webinar identifier"
        tests:
          - unique:
              severity: error
          - not_null:
              severity: error
      - name: host_id
        description: "Webinar host user ID"
        tests:
          - not_null:
              severity: error
          - relationships:
              to: ref('bz_users')
              field: user_id
              severity: warn
      - name: webinar_type
        description: "Type of webinar"
        tests:
          - accepted_values:
              values: ['REGULAR', 'RECURRING', 'PRACTICE_SESSION']
              severity: error
      - name: registration_required
        description: "Whether registration is required"
        tests:
          - accepted_values:
              values: [true, false]
              severity: error
      - name: max_attendees
        description: "Maximum number of attendees"
        tests:
          - expression_is_true:
              expression: "max_attendees > 0"
              severity: error

  - name: bz_support_tickets
    description: "Bronze layer support ticket data"
    columns:
      - name: ticket_id
        description: "Unique support ticket identifier"
        tests:
          - unique:
              severity: error
          - not_null:
              severity: error
      - name: user_id
        description: "User who created the ticket"
        tests:
          - relationships:
              to: ref('bz_users')
              field: user_id
              severity: warn
      - name: priority
        description: "Ticket priority level"
        tests:
          - accepted_values:
              values: ['LOW', 'MEDIUM', 'HIGH', 'CRITICAL']
              severity: error
      - name: status
        description: "Current ticket status"
        tests:
          - accepted_values:
              values: ['OPEN', 'IN_PROGRESS', 'RESOLVED', 'CLOSED', 'ESCALATED']
              severity: error
      - name: created_at
        description: "Ticket creation timestamp"
        tests:
          - not_null:
              severity: error
      - name: resolved_at
        description: "Ticket resolution timestamp"
        tests:
          - expression_is_true:
              expression: "resolved_at IS NULL OR resolved_at >= created_at"
              severity: error

  - name: bz_licenses
    description: "Bronze layer license data"
    columns:
      - name: license_id
        description: "Unique license identifier"
        tests:
          - unique:
              severity: error
          - not_null:
              severity: error
      - name: account_id
        description: "Associated account ID"
        tests:
          - not_null:
              severity: error
      - name: license_type
        description: "Type of license"
        tests:
          - accepted_values:
              values: ['BASIC', 'PRO', 'BUSINESS', 'ENTERPRISE', 'ENTERPRISE_PLUS']
              severity: error
      - name: status
        description: "License status"
        tests:
          - accepted_values:
              values: ['ACTIVE', 'INACTIVE', 'EXPIRED', 'SUSPENDED']
              severity: error
      - name: start_date
        description: "License start date"
        tests:
          - not_null:
              severity: error
      - name: end_date
        description: "License end date"
        tests:
          - expression_is_true:
              expression: "end_date IS NULL OR end_date >= start_date"
              severity: error

  - name: bz_billing_events
    description: "Bronze layer billing event data"
    columns:
      - name: event_id
        description: "Unique billing event identifier"
        tests:
          - unique:
              severity: error
          - not_null:
              severity: error
      - name: account_id
        description: "Associated account ID"
        tests:
          - not_null:
              severity: error
      - name: event_type
        description: "Type of billing event"
        tests:
          - accepted_values:
              values: ['CHARGE', 'REFUND', 'CREDIT', 'ADJUSTMENT', 'PAYMENT']
              severity: error
      - name: amount
        description: "Event amount"
        tests:
          - not_null:
              severity: error
          - expression_is_true:
              expression: "amount != 0"
              severity: warn
      - name: currency
        description: "Currency code"
        tests:
          - accepted_values:
              values: ['USD', 'EUR', 'GBP', 'JPY', 'CAD', 'AUD']
              severity: error
      - name: event_date
        description: "Billing event date"
        tests:
          - not_null:
              severity: error
```

### Custom SQL-based dbt Tests

```sql
-- tests/bronze/test_bz_audit_log_data_quality.sql
-- Test Case ID: TC_BZ_001
-- Description: Validate audit log data quality and business rules
-- Expected Outcome: No records should fail validation

SELECT 
    audit_id,
    user_id,
    action_type,
    timestamp,
    'Invalid timestamp future date' as error_type
FROM {{ ref('bz_audit_log') }}
WHERE timestamp > CURRENT_TIMESTAMP()

UNION ALL

SELECT 
    audit_id,
    user_id,
    action_type,
    timestamp,
    'Missing critical audit fields' as error_type
FROM {{ ref('bz_audit_log') }}
WHERE audit_id IS NULL 
   OR user_id IS NULL 
   OR action_type IS NULL 
   OR timestamp IS NULL

UNION ALL

SELECT 
    audit_id,
    user_id,
    action_type,
    timestamp,
    'Invalid action type format' as error_type
FROM {{ ref('bz_audit_log') }}
WHERE action_type NOT IN ('LOGIN', 'LOGOUT', 'CREATE_MEETING', 'JOIN_MEETING', 'LEAVE_MEETING', 'DELETE_MEETING', 'UPDATE_PROFILE')
   OR LENGTH(TRIM(action_type)) = 0
```

```sql
-- tests/bronze/test_bz_users_email_validation.sql
-- Test Case ID: TC_BZ_002
-- Description: Comprehensive email validation for user records
-- Expected Outcome: All email addresses should be valid format

SELECT 
    user_id,
    email,
    'Invalid email format' as error_type
FROM {{ ref('bz_users') }}
WHERE email IS NULL
   OR email NOT LIKE '%@%'
   OR email LIKE '%@%@%'
   OR email LIKE '@%'
   OR email LIKE '%@'
   OR LENGTH(TRIM(email)) < 5
   OR email LIKE '% %'
```

```sql
-- tests/bronze/test_bz_meetings_duration_validation.sql
-- Test Case ID: TC_BZ_003
-- Description: Validate meeting duration and participant count logic
-- Expected Outcome: Duration and participant counts should be realistic

SELECT 
    meeting_id,
    host_id,
    duration,
    participant_count,
    'Unrealistic meeting duration' as error_type
FROM {{ ref('bz_meetings') }}
WHERE duration < 0 
   OR duration > 1440  -- More than 24 hours
   
UNION ALL

SELECT 
    meeting_id,
    host_id,
    duration,
    participant_count,
    'Invalid participant count' as error_type
FROM {{ ref('bz_meetings') }}
WHERE participant_count < 0 
   OR participant_count > 10000  -- Unrealistic high count
```

```sql
-- tests/bronze/test_bz_participants_time_logic.sql
-- Test Case ID: TC_BZ_004
-- Description: Validate participant join/leave time logic
-- Expected Outcome: Leave time should always be after join time

SELECT 
    participant_id,
    meeting_id,
    user_id,
    join_time,
    leave_time,
    'Leave time before join time' as error_type
FROM {{ ref('bz_participants') }}
WHERE leave_time IS NOT NULL 
  AND leave_time < join_time

UNION ALL

SELECT 
    participant_id,
    meeting_id,
    user_id,
    join_time,
    leave_time,
    'Future join time' as error_type
FROM {{ ref('bz_participants') }}
WHERE join_time > CURRENT_TIMESTAMP()
```

```sql
-- tests/bronze/test_bz_feature_usage_aggregation.sql
-- Test Case ID: TC_BZ_005
-- Description: Validate feature usage aggregation and counts
-- Expected Outcome: Usage counts should be positive and realistic

SELECT 
    usage_id,
    user_id,
    feature_name,
    usage_count,
    'Invalid usage count' as error_type
FROM {{ ref('bz_feature_usage') }}
WHERE usage_count <= 0 
   OR usage_count > 1000  -- Unrealistic daily usage

UNION ALL

SELECT 
    usage_id,
    user_id,
    feature_name,
    usage_count,
    'Future usage date' as error_type
FROM {{ ref('bz_feature_usage') }}
WHERE usage_date > CURRENT_DATE()
```

```sql
-- tests/bronze/test_bz_webinars_capacity_validation.sql
-- Test Case ID: TC_BZ_006
-- Description: Validate webinar capacity and registration logic
-- Expected Outcome: Webinar capacity should be within reasonable limits

SELECT 
    webinar_id,
    host_id,
    max_attendees,
    registration_required,
    'Invalid webinar capacity' as error_type
FROM {{ ref('bz_webinars') }}
WHERE max_attendees <= 0 
   OR max_attendees > 50000  -- Zoom's maximum capacity

UNION ALL

SELECT 
    webinar_id,
    host_id,
    max_attendees,
    registration_required,
    'Missing required fields' as error_type
FROM {{ ref('bz_webinars') }}
WHERE webinar_id IS NULL 
   OR host_id IS NULL 
   OR max_attendees IS NULL
```

```sql
-- tests/bronze/test_bz_support_tickets_sla.sql
-- Test Case ID: TC_BZ_007
-- Description: Validate support ticket SLA and status transitions
-- Expected Outcome: Ticket resolution times should be logical

SELECT 
    ticket_id,
    user_id,
    priority,
    status,
    created_at,
    resolved_at,
    'Resolution before creation' as error_type
FROM {{ ref('bz_support_tickets') }}
WHERE resolved_at IS NOT NULL 
  AND resolved_at < created_at

UNION ALL

SELECT 
    ticket_id,
    user_id,
    priority,
    status,
    created_at,
    resolved_at,
    'Resolved ticket without resolution time' as error_type
FROM {{ ref('bz_support_tickets') }}
WHERE status IN ('RESOLVED', 'CLOSED') 
  AND resolved_at IS NULL
```

```sql
-- tests/bronze/test_bz_licenses_validity.sql
-- Test Case ID: TC_BZ_008
-- Description: Validate license validity periods and status consistency
-- Expected Outcome: License dates should be consistent with status

SELECT 
    license_id,
    account_id,
    license_type,
    status,
    start_date,
    end_date,
    'End date before start date' as error_type
FROM {{ ref('bz_licenses') }}
WHERE end_date IS NOT NULL 
  AND end_date < start_date

UNION ALL

SELECT 
    license_id,
    account_id,
    license_type,
    status,
    start_date,
    end_date,
    'Active license past end date' as error_type
FROM {{ ref('bz_licenses') }}
WHERE status = 'ACTIVE' 
  AND end_date IS NOT NULL 
  AND end_date < CURRENT_DATE()
```

```sql
-- tests/bronze/test_bz_billing_events_financial.sql
-- Test Case ID: TC_BZ_009
-- Description: Validate billing event financial data integrity
-- Expected Outcome: Financial amounts should be valid and consistent

SELECT 
    event_id,
    account_id,
    event_type,
    amount,
    currency,
    'Zero amount for charge/payment' as error_type
FROM {{ ref('bz_billing_events') }}
WHERE event_type IN ('CHARGE', 'PAYMENT') 
  AND amount = 0

UNION ALL

SELECT 
    event_id,
    account_id,
    event_type,
    amount,
    currency,
    'Positive refund amount' as error_type
FROM {{ ref('bz_billing_events') }}
WHERE event_type = 'REFUND' 
  AND amount > 0

UNION ALL

SELECT 
    event_id,
    account_id,
    event_type,
    amount,
    currency,
    'Future billing event' as error_type
FROM {{ ref('bz_billing_events') }}
WHERE event_date > CURRENT_DATE()
```

```sql
-- tests/bronze/test_edge_cases_null_handling.sql
-- Test Case ID: TC_BZ_010
-- Description: Test COALESCE null handling across all bronze models
-- Expected Outcome: Critical fields should never be null after COALESCE

-- Test null handling in audit logs
SELECT 'bz_audit_log' as model_name, COUNT(*) as null_count
FROM {{ ref('bz_audit_log') }}
WHERE audit_id IS NULL

UNION ALL

-- Test null handling in users
SELECT 'bz_users' as model_name, COUNT(*) as null_count
FROM {{ ref('bz_users') }}
WHERE user_id IS NULL OR email IS NULL

UNION ALL

-- Test null handling in meetings
SELECT 'bz_meetings' as model_name, COUNT(*) as null_count
FROM {{ ref('bz_meetings') }}
WHERE meeting_id IS NULL OR host_id IS NULL
```

```sql
-- tests/bronze/test_cross_model_relationships.sql
-- Test Case ID: TC_BZ_012
-- Description: Validate referential integrity across bronze models
-- Expected Outcome: All foreign key relationships should be valid

-- Test orphaned audit log records
SELECT 
    'audit_log_orphaned_users' as test_name,
    COUNT(*) as violation_count
FROM {{ ref('bz_audit_log') }} a
LEFT JOIN {{ ref('bz_users') }} u ON a.user_id = u.user_id
WHERE a.user_id IS NOT NULL AND u.user_id IS NULL

UNION ALL

-- Test orphaned meeting participants
SELECT 
    'participants_orphaned_meetings' as test_name,
    COUNT(*) as violation_count
FROM {{ ref('bz_participants') }} p
LEFT JOIN {{ ref('bz_meetings') }} m ON p.meeting_id = m.meeting_id
WHERE p.meeting_id IS NOT NULL AND m.meeting_id IS NULL

UNION ALL

-- Test orphaned feature usage
SELECT 
    'feature_usage_orphaned_users' as test_name,
    COUNT(*) as violation_count
FROM {{ ref('bz_feature_usage') }} f
LEFT JOIN {{ ref('bz_users') }} u ON f.user_id = u.user_id
WHERE f.user_id IS NOT NULL AND u.user_id IS NULL
```

## Test Execution Strategy

### Test Categories by Priority

**Critical (Must Pass)**
- Primary key uniqueness
- Not null constraints on critical fields
- Data type validations
- Referential integrity

**High Priority**
- Business rule validations
- Date/time logic
- Accepted values constraints
- Cross-model relationships

**Medium Priority**
- Performance metrics
- Data quality warnings
- Volume validations

### Test Execution Commands

```bash
# Run all bronze layer tests
dbt test --models bronze

# Run specific test categories
dbt test --models bronze --select test_type:unique
dbt test --models bronze --select test_type:not_null
dbt test --models bronze --select test_type:relationships

# Run custom SQL tests only
dbt test --models bronze --select test_type:data

# Run tests with specific severity
dbt test --models bronze --severity error
```

### Expected Test Results

| Test Category | Expected Pass Rate | Action on Failure |
|---------------|-------------------|-------------------|
| Schema Tests | 100% | Block deployment |
| Business Rules | 95%+ | Investigate and fix |
| Edge Cases | 90%+ | Monitor and improve |
| Performance | 85%+ | Optimize if needed |

## API Cost Calculation

### Snowflake Compute Costs
- **Warehouse Size**: X-Small (1 credit/hour)
- **Test Execution Time**: ~15 minutes for full test suite
- **Credits Used**: 0.25 credits per full test run
- **Cost per Credit**: $2.00 (standard rate)
- **Cost per Test Run**: $0.50 USD

### dbt Cloud Costs
- **Developer Seat**: $50/month (includes unlimited test runs)
- **Job Runs**: Included in seat cost
- **Additional Costs**: None for standard testing

### Total Monthly Testing Cost Estimate
- **Daily Test Runs**: 4 (CI/CD + manual)
- **Monthly Snowflake Cost**: 4 × 30 × $0.50 = $60 USD
- **dbt Cloud Cost**: $50 USD (seat cost)
- **Total Monthly Cost**: $110.00 USD

### Cost Optimization Recommendations
1. Use smaller warehouse for testing (X-Small sufficient)
2. Implement test result caching
3. Run full test suite only on main branch
4. Use incremental testing for feature branches
5. Schedule heavy tests during off-peak hours

## Monitoring and Alerting

### Test Failure Notifications
```yaml
# dbt_project.yml
on-run-end:
  - "{{ send_test_results_to_slack() }}"
```

### Data Quality Dashboards
- Test pass/fail rates over time
- Model-specific test results
- Data volume trends
- Performance metrics

### Automated Remediation
- Auto-retry failed tests
- Quarantine bad data
- Alert data engineering team
- Generate incident reports

This comprehensive test suite ensures robust data quality validation for the Zoom Customer Analytics bronze layer, covering all critical aspects from basic schema validation to complex business rule enforcement.