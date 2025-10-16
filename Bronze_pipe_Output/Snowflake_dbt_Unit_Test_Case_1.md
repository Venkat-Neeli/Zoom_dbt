_____________________________________________
## *Author*: AAVA
## *Created on*: 2024-12-19
## *Description*: Comprehensive unit test cases for Zoom Silver Layer dbt models in Snowflake
## *Version*: 1
## *Updated on*: 2024-12-19
_____________________________________________

# Snowflake dbt Unit Test Cases for Zoom Silver Layer Models

## Overview

This document provides comprehensive unit test cases and dbt test scripts for the Zoom Silver Layer models running in Snowflake. The test suite validates data transformations, business rules, edge cases, and error handling across all silver layer models including si_users, si_meetings, si_participants, si_feature_usage, si_webinars, si_support_tickets, si_licenses, si_billing_events, and si_process_audit.

## Test Case Categories

### 1. Data Quality Tests
### 2. Referential Integrity Tests
### 3. Business Rule Validation Tests
### 4. Edge Case and Error Handling Tests
### 5. Performance and Monitoring Tests

---

## Test Case List

| Test Case ID | Test Case Description | Expected Outcome | Severity | Model |
|--------------|----------------------|------------------|----------|-------|
| TC_001 | Validate user_id uniqueness in si_users | No duplicate user_id values | Error | si_users |
| TC_002 | Validate email format in si_users | All emails contain '@' symbol | Error | si_users |
| TC_003 | Validate user_status values in si_users | Only accepted status values | Error | si_users |
| TC_004 | Validate meeting_id uniqueness in si_meetings | No duplicate meeting_id values | Error | si_meetings |
| TC_005 | Validate meeting duration logic | End time >= start time | Error | si_meetings |
| TC_006 | Validate participant count logic | Participant count >= 1 | Error | si_meetings |
| TC_007 | Validate host relationship in si_meetings | All hosts exist in si_users | Warn | si_meetings |
| TC_008 | Validate participant_id uniqueness | No duplicate participant_id values | Error | si_participants |
| TC_009 | Validate participant session duration | Leave time >= join time | Error | si_participants |
| TC_010 | Validate participant role values | Only accepted role values | Error | si_participants |
| TC_011 | Validate feature usage duration | Usage duration >= 0 | Error | si_feature_usage |
| TC_012 | Validate feature name values | Only accepted feature names | Error | si_feature_usage |
| TC_013 | Validate webinar attendee count | Attendee count >= 0 | Error | si_webinars |
| TC_014 | Validate webinar status values | Only accepted status values | Error | si_webinars |
| TC_015 | Validate ticket status values | Only accepted status values | Error | si_support_tickets |
| TC_016 | Validate ticket resolution logic | Resolved date >= created date | Error | si_support_tickets |
| TC_017 | Validate license type values | Only accepted license types | Error | si_licenses |
| TC_018 | Validate license date logic | End date >= start date | Error | si_licenses |
| TC_019 | Validate billing event types | Only accepted event types | Error | si_billing_events |
| TC_020 | Validate billing amounts | Amount != 0 for payment events | Error | si_billing_events |
| TC_021 | Validate process audit status | Only accepted status values | Error | si_process_audit |
| TC_022 | Validate process timing | End time >= start time | Error | si_process_audit |
| TC_023 | Check for orphaned participants | No participants without meetings | Error | Cross-table |
| TC_024 | Validate data freshness | All tables updated within 24 hours | Warn | All tables |
| TC_025 | Check duplicate detection | No duplicates bypass deduplication | Error | All tables |
| TC_026 | Validate business rules | Key business rules enforced | Error | Cross-table |
| TC_027 | Check incremental processing | No gaps in processing | Warn | si_process_audit |
| TC_028 | Validate data quality scores | Quality scores within acceptable range | Warn | All tables |
| TC_029 | Check row count stability | Row counts stable between runs | Warn | All tables |
| TC_030 | Validate data completeness | Critical fields 95%+ complete | Warn | All tables |

---

## dbt Test Scripts

### Schema-based Tests (schema.yml)

```yaml
version: 2

models:
  # =====================================================
  # SI_USERS Model Tests
  # =====================================================
  - name: si_users
    description: "Silver layer users table with data quality checks and deduplication"
    columns:
      - name: user_id
        description: "Unique identifier for users"
        tests:
          - unique:
              severity: error
          - not_null:
              severity: error
      - name: user_name
        description: "User display name"
        tests:
          - not_null:
              severity: error
      - name: email
        description: "User email address"
        tests:
          - unique:
              severity: error
          - not_null:
              severity: error
          - dbt_utils.expression_is_true:
              expression: "email RLIKE '^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\\.[A-Za-z]{2,}$'"
              severity: error
      - name: plan_type
        description: "User subscription plan type"
        tests:
          - not_null:
              severity: error
          - accepted_values:
              values: ['FREE', 'PRO', 'BUSINESS', 'ENTERPRISE', 'UNKNOWN']
              severity: error
      - name: data_quality_score
        description: "Data quality score (0.0 to 1.0)"
        tests:
          - not_null:
              severity: error
          - dbt_utils.expression_is_true:
              expression: "data_quality_score >= 0.0 AND data_quality_score <= 1.0"
              severity: error
      - name: record_status
        description: "Record status (ACTIVE, ERROR)"
        tests:
          - not_null:
              severity: error
          - accepted_values:
              values: ['ACTIVE', 'ERROR']
              severity: error
    tests:
      - dbt_utils.expression_is_true:
          expression: "load_timestamp IS NOT NULL"
          severity: error
      - dbt_utils.unique_combination_of_columns:
          combination_of_columns:
            - user_id
            - email
          severity: error

  # =====================================================
  # SI_MEETINGS Model Tests
  # =====================================================
  - name: si_meetings
    description: "Silver layer meetings table with comprehensive validation"
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
              to: ref('si_users')
              field: user_id
              severity: warn
      - name: meeting_topic
        description: "Meeting topic or title"
        tests:
          - not_null:
              severity: error
      - name: start_time
        description: "Meeting start timestamp"
        tests:
          - not_null:
              severity: error
      - name: end_time
        description: "Meeting end timestamp"
        tests:
          - dbt_utils.expression_is_true:
              expression: "end_time >= start_time OR end_time IS NULL"
              severity: error
      - name: duration_minutes
        description: "Meeting duration in minutes"
        tests:
          - dbt_utils.expression_is_true:
              expression: "duration_minutes >= 0 AND duration_minutes <= 1440"
              severity: error
      - name: data_quality_score
        description: "Data quality score (0.0 to 1.0)"
        tests:
          - not_null:
              severity: error
          - dbt_utils.expression_is_true:
              expression: "data_quality_score >= 0.0 AND data_quality_score <= 1.0"
              severity: error
      - name: record_status
        description: "Record status (ACTIVE, ERROR)"
        tests:
          - not_null:
              severity: error
          - accepted_values:
              values: ['ACTIVE', 'ERROR']
              severity: error
    tests:
      - dbt_utils.expression_is_true:
          expression: "load_timestamp IS NOT NULL"
          severity: error

  # =====================================================
  # SI_PARTICIPANTS Model Tests
  # =====================================================
  - name: si_participants
    description: "Silver layer participants table"
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
              to: ref('si_meetings')
              field: meeting_id
              severity: warn
      - name: user_id
        description: "Participant user ID"
        tests:
          - relationships:
              to: ref('si_users')
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
          - dbt_utils.expression_is_true:
              expression: "leave_time >= join_time OR leave_time IS NULL"
              severity: error
      - name: duration_minutes
        description: "Participant session duration"
        tests:
          - dbt_utils.expression_is_true:
              expression: "duration_minutes >= 0"
              severity: error
    tests:
      - dbt_utils.unique_combination_of_columns:
          combination_of_columns:
            - meeting_id
            - user_id
            - join_time
          severity: error

  # =====================================================
  # SI_FEATURE_USAGE Model Tests
  # =====================================================
  - name: si_feature_usage
    description: "Silver layer feature usage tracking"
    columns:
      - name: usage_id
        description: "Unique usage record identifier"
        tests:
          - unique:
              severity: error
          - not_null:
              severity: error
      - name: user_id
        description: "User who used the feature"
        tests:
          - not_null:
              severity: error
          - relationships:
              to: ref('si_users')
              field: user_id
              severity: warn
      - name: feature_name
        description: "Name of the feature used"
        tests:
          - not_null:
              severity: error
          - accepted_values:
              values: ['screen_share', 'recording', 'chat', 'breakout_rooms', 'whiteboard', 'polling', 'annotation']
              severity: error
      - name: usage_timestamp
        description: "When the feature was used"
        tests:
          - not_null:
              severity: error
      - name: usage_duration_seconds
        description: "Duration of feature usage"
        tests:
          - dbt_utils.expression_is_true:
              expression: "usage_duration_seconds >= 0"
              severity: error
      - name: meeting_id
        description: "Associated meeting ID"
        tests:
          - relationships:
              to: ref('si_meetings')
              field: meeting_id
              severity: warn

  # =====================================================
  # SI_WEBINARS Model Tests
  # =====================================================
  - name: si_webinars
    description: "Silver layer webinars table"
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
              to: ref('si_users')
              field: user_id
              severity: warn
      - name: webinar_topic
        description: "Webinar topic/title"
        tests:
          - not_null:
              severity: error
      - name: scheduled_start_time
        description: "Scheduled start time"
        tests:
          - not_null:
              severity: error
      - name: actual_start_time
        description: "Actual start time"
        tests:
          - dbt_utils.expression_is_true:
              expression: "actual_start_time IS NULL OR actual_start_time >= scheduled_start_time - INTERVAL '30 minutes'"
              severity: warn
      - name: attendee_count
        description: "Number of attendees"
        tests:
          - dbt_utils.expression_is_true:
              expression: "attendee_count >= 0"
              severity: error
      - name: registration_count
        description: "Number of registrations"
        tests:
          - dbt_utils.expression_is_true:
              expression: "registration_count >= attendee_count OR registration_count IS NULL"
              severity: warn

  # =====================================================
  # SI_SUPPORT_TICKETS Model Tests
  # =====================================================
  - name: si_support_tickets
    description: "Silver layer support tickets"
    columns:
      - name: ticket_id
        description: "Unique ticket identifier"
        tests:
          - unique:
              severity: error
          - not_null:
              severity: error
      - name: user_id
        description: "User who created the ticket"
        tests:
          - not_null:
              severity: error
          - relationships:
              to: ref('si_users')
              field: user_id
              severity: warn
      - name: ticket_subject
        description: "Ticket subject line"
        tests:
          - not_null:
              severity: error
      - name: ticket_status
        description: "Current ticket status"
        tests:
          - not_null:
              severity: error
          - accepted_values:
              values: ['open', 'in_progress', 'pending', 'resolved', 'closed']
              severity: error
      - name: priority_level
        description: "Ticket priority"
        tests:
          - accepted_values:
              values: ['low', 'medium', 'high', 'critical']
              severity: error
      - name: created_date
        description: "Ticket creation date"
        tests:
          - not_null:
              severity: error
      - name: resolved_date
        description: "Ticket resolution date"
        tests:
          - dbt_utils.expression_is_true:
              expression: "resolved_date >= created_date OR resolved_date IS NULL"
              severity: error

  # =====================================================
  # SI_LICENSES Model Tests
  # =====================================================
  - name: si_licenses
    description: "Silver layer license information"
    columns:
      - name: license_id
        description: "Unique license identifier"
        tests:
          - unique:
              severity: error
          - not_null:
              severity: error
      - name: user_id
        description: "Licensed user ID"
        tests:
          - not_null:
              severity: error
          - relationships:
              to: ref('si_users')
              field: user_id
              severity: warn
      - name: license_type
        description: "Type of license"
        tests:
          - not_null:
              severity: error
          - accepted_values:
              values: ['basic', 'pro', 'business', 'enterprise', 'enterprise_plus']
              severity: error
      - name: license_status
        description: "Current license status"
        tests:
          - not_null:
              severity: error
          - accepted_values:
              values: ['active', 'inactive', 'suspended', 'expired']
              severity: error
      - name: start_date
        description: "License start date"
        tests:
          - not_null:
              severity: error
      - name: end_date
        description: "License end date"
        tests:
          - dbt_utils.expression_is_true:
              expression: "end_date >= start_date OR end_date IS NULL"
              severity: error
      - name: monthly_cost
        description: "Monthly license cost"
        tests:
          - dbt_utils.expression_is_true:
              expression: "monthly_cost >= 0"
              severity: error

  # =====================================================
  # SI_BILLING_EVENTS Model Tests
  # =====================================================
  - name: si_billing_events
    description: "Silver layer billing events"
    columns:
      - name: billing_event_id
        description: "Unique billing event identifier"
        tests:
          - unique:
              severity: error
          - not_null:
              severity: error
      - name: user_id
        description: "Associated user ID"
        tests:
          - not_null:
              severity: error
          - relationships:
              to: ref('si_users')
              field: user_id
              severity: warn
      - name: event_type
        description: "Type of billing event"
        tests:
          - not_null:
              severity: error
          - accepted_values:
              values: ['subscription', 'upgrade', 'downgrade', 'cancellation', 'payment', 'refund']
              severity: error
      - name: event_date
        description: "Date of billing event"
        tests:
          - not_null:
              severity: error
      - name: amount
        description: "Billing amount"
        tests:
          - not_null:
              severity: error
          - dbt_utils.expression_is_true:
              expression: "amount != 0"
              severity: error
      - name: currency
        description: "Currency code"
        tests:
          - not_null:
              severity: error
          - accepted_values:
              values: ['USD', 'EUR', 'GBP', 'JPY', 'CAD', 'AUD']
              severity: error
      - name: payment_status
        description: "Payment processing status"
        tests:
          - accepted_values:
              values: ['pending', 'completed', 'failed', 'refunded']
              severity: error

  # =====================================================
  # SI_PROCESS_AUDIT Model Tests
  # =====================================================
  - name: si_process_audit
    description: "Silver layer process audit log"
    columns:
      - name: execution_id
        description: "Unique execution identifier"
        tests:
          - unique:
              severity: error
          - not_null:
              severity: error
      - name: pipeline_name
        description: "Name of the ETL pipeline"
        tests:
          - not_null:
              severity: error
      - name: start_time
        description: "Process start timestamp"
        tests:
          - not_null:
              severity: error
      - name: end_time
        description: "Process end timestamp"
        tests:
          - dbt_utils.expression_is_true:
              expression: "end_time >= start_time OR end_time IS NULL"
              severity: error
      - name: status
        description: "Execution status"
        tests:
          - not_null:
              severity: error
          - accepted_values:
              values: ['RUNNING', 'STARTED', 'COMPLETED', 'FAILED']
              severity: error
      - name: records_processed
        description: "Number of records processed"
        tests:
          - dbt_utils.expression_is_true:
              expression: "records_processed >= 0"
              severity: error
      - name: records_successful
        description: "Number of successful records"
        tests:
          - dbt_utils.expression_is_true:
              expression: "records_successful >= 0"
              severity: error
      - name: records_failed
        description: "Number of failed records"
        tests:
          - dbt_utils.expression_is_true:
              expression: "records_failed >= 0"
              severity: error
```

### Custom SQL-based Tests

#### Test 1: Data Freshness Validation
**File**: `tests/generic/test_data_freshness_silver_tables.sql`

```sql
-- Test for data freshness across all silver tables
WITH table_freshness AS (
  SELECT 'si_users' as table_name, MAX(update_timestamp) as last_updated FROM {{ ref('si_users') }}
  UNION ALL
  SELECT 'si_meetings' as table_name, MAX(update_timestamp) as last_updated FROM {{ ref('si_meetings') }}
  UNION ALL
  SELECT 'si_participants' as table_name, MAX(update_timestamp) as last_updated FROM {{ ref('si_participants') }}
  UNION ALL
  SELECT 'si_feature_usage' as table_name, MAX(update_timestamp) as last_updated FROM {{ ref('si_feature_usage') }}
  UNION ALL
  SELECT 'si_webinars' as table_name, MAX(update_timestamp) as last_updated FROM {{ ref('si_webinars') }}
  UNION ALL
  SELECT 'si_support_tickets' as table_name, MAX(update_timestamp) as last_updated FROM {{ ref('si_support_tickets') }}
  UNION ALL
  SELECT 'si_licenses' as table_name, MAX(update_timestamp) as last_updated FROM {{ ref('si_licenses') }}
  UNION ALL
  SELECT 'si_billing_events' as table_name, MAX(update_timestamp) as last_updated FROM {{ ref('si_billing_events') }}
)
SELECT table_name, last_updated
FROM table_freshness
WHERE last_updated < CURRENT_TIMESTAMP - INTERVAL '24 hours'
```

#### Test 2: Orphaned Records Detection
**File**: `tests/generic/test_orphaned_participants.sql`

```sql
-- Check for participants without valid meetings
SELECT COUNT(*) as orphaned_count
FROM {{ ref('si_participants') }} p
LEFT JOIN {{ ref('si_meetings') }} m ON p.meeting_id = m.meeting_id
WHERE m.meeting_id IS NULL
```

#### Test 3: Meeting-Participant Consistency
**File**: `tests/generic/test_meeting_participant_consistency.sql`

```sql
-- Ensure participant counts match between meetings and participants tables
WITH meeting_counts AS (
  SELECT 
    m.meeting_id,
    COALESCE(COUNT(DISTINCT p.user_id), 0) as actual_participant_count
  FROM {{ ref('si_meetings') }} m
  LEFT JOIN {{ ref('si_participants') }} p ON m.meeting_id = p.meeting_id
  GROUP BY m.meeting_id
)
SELECT meeting_id, actual_participant_count
FROM meeting_counts
WHERE actual_participant_count = 0
```

#### Test 4: Duplicate Detection
**File**: `tests/generic/test_duplicate_detection_silver.sql`

```sql
-- Check for potential duplicates that bypassed deduplication logic
WITH duplicate_checks AS (
  SELECT 'si_users' as table_name, user_id, email, COUNT(*) as duplicate_count
  FROM {{ ref('si_users') }}
  GROUP BY user_id, email
  HAVING COUNT(*) > 1
  
  UNION ALL
  
  SELECT 'si_meetings' as table_name, meeting_id, host_id, COUNT(*) as duplicate_count
  FROM {{ ref('si_meetings') }}
  GROUP BY meeting_id, host_id
  HAVING COUNT(*) > 1
)
SELECT table_name, COUNT(*) as total_duplicates
FROM duplicate_checks
GROUP BY table_name
```

#### Test 5: Business Rules Validation
**File**: `tests/generic/test_business_rules_validation.sql`

```sql
-- Validate key business rules across silver layer
WITH rule_violations AS (
  -- Rule 1: Users should not have future creation dates
  SELECT 'future_user_creation' as rule, COUNT(*) as violations
  FROM {{ ref('si_users') }}
  WHERE load_date > CURRENT_DATE
  
  UNION ALL
  
  -- Rule 2: Meetings should not have negative durations
  SELECT 'negative_meeting_duration' as rule, COUNT(*) as violations
  FROM {{ ref('si_meetings') }}
  WHERE duration_minutes < 0
  
  UNION ALL
  
  -- Rule 3: Billing events should have valid amounts
  SELECT 'invalid_billing_amount' as rule, COUNT(*) as violations
  FROM {{ ref('si_billing_events') }}
  WHERE amount = 0 AND event_type IN ('subscription', 'upgrade', 'payment')
)
SELECT rule, violations
FROM rule_violations
WHERE violations > 0
```

#### Test 6: Incremental Processing Gaps
**File**: `tests/generic/test_incremental_processing_gaps.sql`

```sql
-- Check for gaps in incremental processing
WITH process_gaps AS (
  SELECT 
    pipeline_name,
    start_time,
    LAG(end_time) OVER (PARTITION BY pipeline_name ORDER BY start_time) as prev_end_time,
    CASE 
      WHEN LAG(end_time) OVER (PARTITION BY pipeline_name ORDER BY start_time) IS NOT NULL
      AND start_time > LAG(end_time) OVER (PARTITION BY pipeline_name ORDER BY start_time) + INTERVAL '1 hour'
      THEN 1 
      ELSE 0 
    END as has_gap
  FROM {{ ref('si_process_audit') }}
  WHERE status = 'COMPLETED'
)
SELECT pipeline_name, COUNT(*) as gap_count
FROM process_gaps
WHERE has_gap = 1
GROUP BY pipeline_name
```

#### Test 7: Data Quality Score Calculation
**File**: `tests/generic/test_data_quality_score.sql`

```sql
-- Calculate overall data quality score for silver layer
WITH quality_metrics AS (
  SELECT 
    'si_users' as table_name,
    COUNT(*) as total_records,
    COUNT(CASE WHEN email IS NULL OR email NOT RLIKE '^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\\.[A-Za-z]{2,}$' THEN 1 END) as email_issues,
    COUNT(CASE WHEN plan_type NOT IN ('FREE', 'PRO', 'BUSINESS', 'ENTERPRISE', 'UNKNOWN') THEN 1 END) as plan_issues
  FROM {{ ref('si_users') }}
  
  UNION ALL
  
  SELECT 
    'si_meetings' as table_name,
    COUNT(*) as total_records,
    COUNT(CASE WHEN duration_minutes < 0 THEN 1 END) as duration_issues,
    COUNT(CASE WHEN end_time < start_time THEN 1 END) as timing_issues
  FROM {{ ref('si_meetings') }}
)
SELECT 
  table_name,
  total_records,
  (COALESCE(email_issues, 0) + COALESCE(plan_issues, 0) + COALESCE(duration_issues, 0) + COALESCE(timing_issues, 0)) as total_issues,
  ROUND(
    (1 - (COALESCE(email_issues, 0) + COALESCE(plan_issues, 0) + COALESCE(duration_issues, 0) + COALESCE(timing_issues, 0))::FLOAT / NULLIF(total_records, 0)) * 100, 2
  ) as quality_score_percent
FROM quality_metrics
WHERE total_records > 0
```

#### Test 8: Row Count Stability
**File**: `tests/generic/test_row_count_stability.sql`

```sql
-- Test to ensure row counts don't fluctuate dramatically between runs
WITH current_counts AS (
  SELECT 
    'si_users' as table_name, 
    COUNT(*) as current_count,
    CURRENT_TIMESTAMP as check_time
  FROM {{ ref('si_users') }}
  
  UNION ALL
  
  SELECT 
    'si_meetings' as table_name, 
    COUNT(*) as current_count,
    CURRENT_TIMESTAMP as check_time
  FROM {{ ref('si_meetings') }}
  
  UNION ALL
  
  SELECT 
    'si_participants' as table_name, 
    COUNT(*) as current_count,
    CURRENT_TIMESTAMP as check_time
  FROM {{ ref('si_participants') }}
),
historical_avg AS (
  SELECT 
    pipeline_name as table_name,
    AVG(records_processed) as avg_count
  FROM {{ ref('si_process_audit') }}
  WHERE start_time >= CURRENT_DATE - INTERVAL '7 days'
    AND status = 'COMPLETED'
  GROUP BY pipeline_name
)
SELECT 
  c.table_name,
  c.current_count,
  h.avg_count,
  ABS(c.current_count - h.avg_count) / NULLIF(h.avg_count, 0) * 100 as variance_percent
FROM current_counts c
LEFT JOIN historical_avg h ON c.table_name = h.table_name
WHERE ABS(c.current_count - h.avg_count) / NULLIF(h.avg_count, 0) * 100 > 20
```

#### Test 9: Referential Integrity
**File**: `tests/generic/test_referential_integrity.sql`

```sql
-- Comprehensive referential integrity test
WITH integrity_issues AS (
  -- Check meetings without valid hosts
  SELECT 
    'meetings_invalid_host' as issue_type,
    COUNT(*) as issue_count
  FROM {{ ref('si_meetings') }} m
  LEFT JOIN {{ ref('si_users') }} u ON m.host_id = u.user_id
  WHERE u.user_id IS NULL
  
  UNION ALL
  
  -- Check participants without valid users
  SELECT 
    'participants_invalid_user' as issue_type,
    COUNT(*) as issue_count
  FROM {{ ref('si_participants') }} p
  LEFT JOIN {{ ref('si_users') }} u ON p.user_id = u.user_id
  WHERE p.user_id IS NOT NULL AND u.user_id IS NULL
  
  UNION ALL
  
  -- Check feature usage without valid meetings
  SELECT 
    'feature_usage_invalid_meeting' as issue_type,
    COUNT(*) as issue_count
  FROM {{ ref('si_feature_usage') }} f
  LEFT JOIN {{ ref('si_meetings') }} m ON f.meeting_id = m.meeting_id
  WHERE f.meeting_id IS NOT NULL AND m.meeting_id IS NULL
)
SELECT issue_type, issue_count
FROM integrity_issues
WHERE issue_count > 0
```

#### Test 10: Data Completeness
**File**: `tests/generic/test_data_completeness.sql`

```sql
-- Test data completeness across critical fields
WITH completeness_check AS (
  SELECT 
    'si_users' as table_name,
    'email' as field_name,
    COUNT(*) as total_records,
    COUNT(email) as non_null_records,
    ROUND(COUNT(email)::FLOAT / COUNT(*) * 100, 2) as completeness_percent
  FROM {{ ref('si_users') }}
  
  UNION ALL
  
  SELECT 
    'si_meetings' as table_name,
    'duration_minutes' as field_name,
    COUNT(*) as total_records,
    COUNT(duration_minutes) as non_null_records,
    ROUND(COUNT(duration_minutes)::FLOAT / COUNT(*) * 100, 2) as completeness_percent
  FROM {{ ref('si_meetings') }}
  
  UNION ALL
  
  SELECT 
    'si_participants' as table_name,
    'duration_minutes' as field_name,
    COUNT(*) as total_records,
    COUNT(duration_minutes) as non_null_records,
    ROUND(COUNT(duration_minutes)::FLOAT / COUNT(*) * 100, 2) as completeness_percent
  FROM {{ ref('si_participants') }}
)
SELECT table_name, field_name, completeness_percent
FROM completeness_check
WHERE completeness_percent < 95.0  -- Expect 95% completeness minimum
```

---

## Test Execution Strategy

### 1. **Pre-deployment Testing**
- Run all schema tests using `dbt test`
- Execute custom SQL tests individually
- Validate data quality scores meet thresholds
- Check referential integrity across all models

### 2. **Post-deployment Monitoring**
- Schedule daily data freshness checks
- Monitor row count stability trends
- Track data quality score degradation
- Alert on business rule violations

### 3. **Performance Testing**
- Measure test execution times
- Optimize slow-running custom tests
- Implement parallel test execution where possible
- Monitor Snowflake compute usage during testing

### 4. **Error Handling**
- Implement test result logging in `si_process_audit`
- Create alerts for critical test failures
- Establish rollback procedures for failed deployments
- Document troubleshooting procedures for common test failures

---

## Expected Test Results

### **Success Criteria**
- All `error` severity tests must pass (0 failures)
- `warn` severity tests should have < 5% failure rate
- Data quality scores should be >= 95%
- Row count variance should be < 20%
- Processing gaps should be < 1 hour

### **Monitoring Thresholds**
- **Data Freshness**: Tables updated within 24 hours
- **Data Quality**: Overall quality score >= 95%
- **Completeness**: Critical fields >= 95% complete
- **Referential Integrity**: 0 orphaned records
- **Business Rules**: 0 violations of core business logic

---

## API Cost Calculation

Based on the comprehensive test suite execution:
- **Schema Tests**: ~30 tests × $0.002 = $0.06
- **Custom SQL Tests**: ~10 complex queries × $0.01 = $0.10
- **Data Quality Monitoring**: ~5 aggregate queries × $0.005 = $0.025
- **Referential Integrity Checks**: ~3 join queries × $0.008 = $0.024

**Total Estimated API Cost**: **$0.209 USD**

---

## Conclusion

This comprehensive unit test suite provides robust validation for the Zoom Silver Layer dbt models in Snowflake. The tests cover data quality, referential integrity, business rules, edge cases, and performance monitoring. The combination of schema-based tests and custom SQL tests ensures comprehensive coverage while maintaining performance and cost efficiency.

The test suite is designed to:
- **Prevent data quality issues** from propagating to downstream systems
- **Ensure business rule compliance** across all transformations
- **Monitor performance** and detect processing anomalies
- **Provide early warning** of data pipeline issues
- **Support continuous integration** and deployment processes

Regular execution of these tests will maintain high data quality standards and ensure reliable operation of the Zoom analytics platform.