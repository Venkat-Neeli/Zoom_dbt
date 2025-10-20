# Snowflake dbt Unit Test Cases - Gold Layer Fact Tables

## Metadata
- **Project**: Zoom dbt Data Pipeline
- **Layer**: Gold Layer
- **Version**: 1.0
- **Created Date**: 2024
- **Models Covered**: 6 Fact Tables
- **Test Framework**: dbt tests with Snowflake

## Overview
This document contains comprehensive unit test cases for all 6 Gold Layer fact tables that transform data from Silver Layer into optimized fact tables with data quality filtering, audit trails, and performance optimization.

## Models Under Test
1. `fact_user_activity.sql`
2. `fact_meeting_activity.sql`
3. `fact_participant_activity.sql`
4. `fact_feature_usage.sql`
5. `fact_webinar_activity.sql`
6. `fact_billing_events.sql`

---

## 1. fact_user_activity Model Tests

### Test Case Summary
| Test ID | Test Type | Description | Priority |
|---------|-----------|-------------|----------|
| FUA_001 | unique | Verify user_activity_id uniqueness | High |
| FUA_002 | not_null | Validate required fields are not null | High |
| FUA_003 | relationships | Check foreign key relationships | High |
| FUA_004 | accepted_values | Validate activity_type values | Medium |
| FUA_005 | custom_sql | Data quality and business rules | High |
| FUA_006 | custom_sql | Edge case handling | Medium |

### dbt Test Scripts

```yaml
# tests/fact_user_activity_tests.yml
version: 2

models:
  - name: fact_user_activity
    description: "Gold layer fact table for user activities"
    tests:
      - dbt_utils.unique_combination_of_columns:
          combination_of_columns:
            - user_activity_id
    columns:
      - name: user_activity_id
        description: "Unique identifier for user activity"
        tests:
          - unique
          - not_null
      
      - name: user_id
        description: "Foreign key to user dimension"
        tests:
          - not_null
          - relationships:
              to: ref('si_users')
              field: user_id
      
      - name: activity_type
        description: "Type of user activity"
        tests:
          - not_null
          - accepted_values:
              values: ['login', 'logout', 'profile_update', 'settings_change', 'password_reset']
      
      - name: activity_timestamp
        description: "Timestamp of the activity"
        tests:
          - not_null
          - dbt_utils.expression_is_true:
              expression: "activity_timestamp <= current_timestamp()"
      
      - name: created_at
        description: "Record creation timestamp"
        tests:
          - not_null
```

```sql
-- tests/custom/test_fact_user_activity_data_quality.sql
-- Test for data quality rules
SELECT *
FROM {{ ref('fact_user_activity') }}
WHERE 
    -- Check for future dates
    activity_timestamp > CURRENT_TIMESTAMP()
    -- Check for invalid duration
    OR (activity_duration_seconds < 0 OR activity_duration_seconds > 86400)
    -- Check for missing audit fields
    OR created_at IS NULL
    OR updated_at IS NULL
```

```sql
-- tests/custom/test_fact_user_activity_edge_cases.sql
-- Test for edge cases and exception scenarios
SELECT 
    'duplicate_activities' as test_case,
    COUNT(*) as failure_count
FROM (
    SELECT user_id, activity_type, activity_timestamp, COUNT(*) as cnt
    FROM {{ ref('fact_user_activity') }}
    GROUP BY user_id, activity_type, activity_timestamp
    HAVING COUNT(*) > 1
)
UNION ALL
SELECT 
    'orphaned_records' as test_case,
    COUNT(*) as failure_count
FROM {{ ref('fact_user_activity') }} f
LEFT JOIN {{ ref('si_users') }} u ON f.user_id = u.user_id
WHERE u.user_id IS NULL
```

---

## 2. fact_meeting_activity Model Tests

### Test Case Summary
| Test ID | Test Type | Description | Priority |
|---------|-----------|-------------|----------|
| FMA_001 | unique | Verify meeting_activity_id uniqueness | High |
| FMA_002 | not_null | Validate required fields are not null | High |
| FMA_003 | relationships | Check foreign key relationships | High |
| FMA_004 | accepted_values | Validate meeting_status values | Medium |
| FMA_005 | custom_sql | Meeting duration validation | High |
| FMA_006 | custom_sql | Participant count validation | Medium |

### dbt Test Scripts

```yaml
# tests/fact_meeting_activity_tests.yml
version: 2

models:
  - name: fact_meeting_activity
    description: "Gold layer fact table for meeting activities"
    columns:
      - name: meeting_activity_id
        description: "Unique identifier for meeting activity"
        tests:
          - unique
          - not_null
      
      - name: meeting_id
        description: "Foreign key to meeting dimension"
        tests:
          - not_null
          - relationships:
              to: ref('si_meetings')
              field: meeting_id
      
      - name: host_user_id
        description: "Foreign key to host user"
        tests:
          - not_null
          - relationships:
              to: ref('si_users')
              field: user_id
      
      - name: meeting_status
        description: "Status of the meeting"
        tests:
          - not_null
          - accepted_values:
              values: ['scheduled', 'started', 'ended', 'cancelled', 'no_show']
      
      - name: start_time
        description: "Meeting start timestamp"
        tests:
          - not_null
      
      - name: end_time
        description: "Meeting end timestamp"
        tests:
          - dbt_utils.expression_is_true:
              expression: "end_time IS NULL OR end_time >= start_time"
      
      - name: participant_count
        description: "Number of participants"
        tests:
          - dbt_utils.expression_is_true:
              expression: "participant_count >= 0"
```

```sql
-- tests/custom/test_fact_meeting_activity_duration.sql
-- Test meeting duration logic
SELECT *
FROM {{ ref('fact_meeting_activity') }}
WHERE 
    -- Invalid duration calculations
    (end_time IS NOT NULL AND start_time IS NOT NULL 
     AND DATEDIFF('second', start_time, end_time) < 0)
    -- Unrealistic meeting durations (over 24 hours)
    OR (end_time IS NOT NULL AND start_time IS NOT NULL 
        AND DATEDIFF('hour', start_time, end_time) > 24)
    -- Meetings marked as ended but no end_time
    OR (meeting_status = 'ended' AND end_time IS NULL)
```

```sql
-- tests/custom/test_fact_meeting_activity_participants.sql
-- Test participant count validation
WITH meeting_participant_counts AS (
    SELECT 
        meeting_id,
        COUNT(DISTINCT participant_id) as actual_participant_count
    FROM {{ ref('si_participants') }}
    GROUP BY meeting_id
)
SELECT 
    f.meeting_id,
    f.participant_count as reported_count,
    COALESCE(mpc.actual_participant_count, 0) as actual_count
FROM {{ ref('fact_meeting_activity') }} f
LEFT JOIN meeting_participant_counts mpc ON f.meeting_id = mpc.meeting_id
WHERE ABS(f.participant_count - COALESCE(mpc.actual_participant_count, 0)) > 0
```

---

## 3. fact_participant_activity Model Tests

### Test Case Summary
| Test ID | Test Type | Description | Priority |
|---------|-----------|-------------|----------|
| FPA_001 | unique | Verify participant_activity_id uniqueness | High |
| FPA_002 | not_null | Validate required fields are not null | High |
| FPA_003 | relationships | Check foreign key relationships | High |
| FPA_004 | accepted_values | Validate participant_status values | Medium |
| FPA_005 | custom_sql | Join/leave time validation | High |
| FPA_006 | custom_sql | Duration calculations | Medium |

### dbt Test Scripts

```yaml
# tests/fact_participant_activity_tests.yml
version: 2

models:
  - name: fact_participant_activity
    description: "Gold layer fact table for participant activities"
    columns:
      - name: participant_activity_id
        description: "Unique identifier for participant activity"
        tests:
          - unique
          - not_null
      
      - name: participant_id
        description: "Foreign key to participant"
        tests:
          - not_null
          - relationships:
              to: ref('si_participants')
              field: participant_id
      
      - name: meeting_id
        description: "Foreign key to meeting"
        tests:
          - not_null
          - relationships:
              to: ref('si_meetings')
              field: meeting_id
      
      - name: participant_status
        description: "Status of participant"
        tests:
          - not_null
          - accepted_values:
              values: ['joined', 'left', 'waiting', 'admitted', 'removed']
      
      - name: join_time
        description: "Participant join timestamp"
        tests:
          - not_null
      
      - name: leave_time
        description: "Participant leave timestamp"
        tests:
          - dbt_utils.expression_is_true:
              expression: "leave_time IS NULL OR leave_time >= join_time"
      
      - name: duration_minutes
        description: "Participation duration in minutes"
        tests:
          - dbt_utils.expression_is_true:
              expression: "duration_minutes IS NULL OR duration_minutes >= 0"
```

```sql
-- tests/custom/test_fact_participant_activity_timing.sql
-- Test participant timing logic
SELECT *
FROM {{ ref('fact_participant_activity') }}
WHERE 
    -- Leave time before join time
    (leave_time IS NOT NULL AND leave_time < join_time)
    -- Duration calculation mismatch
    OR (leave_time IS NOT NULL AND join_time IS NOT NULL 
        AND ABS(duration_minutes - DATEDIFF('minute', join_time, leave_time)) > 1)
    -- Participant marked as left but no leave_time
    OR (participant_status = 'left' AND leave_time IS NULL)
    -- Unrealistic duration (over 24 hours)
    OR (duration_minutes > 1440)
```

---

## 4. fact_feature_usage Model Tests

### Test Case Summary
| Test ID | Test Type | Description | Priority |
|---------|-----------|-------------|----------|
| FFU_001 | unique | Verify feature_usage_id uniqueness | High |
| FFU_002 | not_null | Validate required fields are not null | High |
| FFU_003 | relationships | Check foreign key relationships | High |
| FFU_004 | accepted_values | Validate feature_name values | Medium |
| FFU_005 | custom_sql | Usage metrics validation | High |
| FFU_006 | custom_sql | Feature availability validation | Medium |

### dbt Test Scripts

```yaml
# tests/fact_feature_usage_tests.yml
version: 2

models:
  - name: fact_feature_usage
    description: "Gold layer fact table for feature usage"
    columns:
      - name: feature_usage_id
        description: "Unique identifier for feature usage"
        tests:
          - unique
          - not_null
      
      - name: user_id
        description: "Foreign key to user"
        tests:
          - not_null
          - relationships:
              to: ref('si_users')
              field: user_id
      
      - name: feature_name
        description: "Name of the feature used"
        tests:
          - not_null
          - accepted_values:
              values: ['screen_share', 'recording', 'chat', 'breakout_rooms', 'whiteboard', 'polls', 'reactions']
      
      - name: usage_timestamp
        description: "Timestamp of feature usage"
        tests:
          - not_null
          - dbt_utils.expression_is_true:
              expression: "usage_timestamp <= current_timestamp()"
      
      - name: usage_count
        description: "Number of times feature was used"
        tests:
          - not_null
          - dbt_utils.expression_is_true:
              expression: "usage_count > 0"
      
      - name: usage_duration_seconds
        description: "Duration of feature usage"
        tests:
          - dbt_utils.expression_is_true:
              expression: "usage_duration_seconds IS NULL OR usage_duration_seconds >= 0"
```

```sql
-- tests/custom/test_fact_feature_usage_metrics.sql
-- Test feature usage metrics
SELECT *
FROM {{ ref('fact_feature_usage') }}
WHERE 
    -- Invalid usage count
    usage_count <= 0
    -- Negative duration
    OR (usage_duration_seconds IS NOT NULL AND usage_duration_seconds < 0)
    -- Unrealistic duration (over 24 hours for single usage)
    OR (usage_duration_seconds > 86400)
    -- Usage timestamp in future
    OR usage_timestamp > CURRENT_TIMESTAMP()
```

---

## 5. fact_webinar_activity Model Tests

### Test Case Summary
| Test ID | Test Type | Description | Priority |
|---------|-----------|-------------|----------|
| FWA_001 | unique | Verify webinar_activity_id uniqueness | High |
| FWA_002 | not_null | Validate required fields are not null | High |
| FWA_003 | relationships | Check foreign key relationships | High |
| FWA_004 | accepted_values | Validate webinar_status values | Medium |
| FWA_005 | custom_sql | Attendee count validation | High |
| FWA_006 | custom_sql | Registration vs attendance | Medium |

### dbt Test Scripts

```yaml
# tests/fact_webinar_activity_tests.yml
version: 2

models:
  - name: fact_webinar_activity
    description: "Gold layer fact table for webinar activities"
    columns:
      - name: webinar_activity_id
        description: "Unique identifier for webinar activity"
        tests:
          - unique
          - not_null
      
      - name: webinar_id
        description: "Foreign key to webinar"
        tests:
          - not_null
          - relationships:
              to: ref('si_webinars')
              field: webinar_id
      
      - name: host_user_id
        description: "Foreign key to host user"
        tests:
          - not_null
          - relationships:
              to: ref('si_users')
              field: user_id
      
      - name: webinar_status
        description: "Status of the webinar"
        tests:
          - not_null
          - accepted_values:
              values: ['scheduled', 'live', 'ended', 'cancelled']
      
      - name: scheduled_start_time
        description: "Scheduled start time"
        tests:
          - not_null
      
      - name: actual_start_time
        description: "Actual start time"
        tests:
          - dbt_utils.expression_is_true:
              expression: "actual_start_time IS NULL OR actual_start_time >= scheduled_start_time - INTERVAL '1 hour'"
      
      - name: registered_count
        description: "Number of registered attendees"
        tests:
          - dbt_utils.expression_is_true:
              expression: "registered_count >= 0"
      
      - name: attended_count
        description: "Number of actual attendees"
        tests:
          - dbt_utils.expression_is_true:
              expression: "attended_count >= 0 AND attended_count <= registered_count + 100"
```

```sql
-- tests/custom/test_fact_webinar_activity_attendance.sql
-- Test webinar attendance logic
SELECT *
FROM {{ ref('fact_webinar_activity') }}
WHERE 
    -- Attended count exceeds registered by unrealistic margin
    attended_count > registered_count * 2
    -- Webinar marked as ended but no actual start time
    OR (webinar_status = 'ended' AND actual_start_time IS NULL)
    -- Negative attendance numbers
    OR registered_count < 0
    OR attended_count < 0
```

---

## 6. fact_billing_events Model Tests

### Test Case Summary
| Test ID | Test Type | Description | Priority |
|---------|-----------|-------------|----------|
| FBE_001 | unique | Verify billing_event_id uniqueness | High |
| FBE_002 | not_null | Validate required fields are not null | High |
| FBE_003 | relationships | Check foreign key relationships | High |
| FBE_004 | accepted_values | Validate event_type values | Medium |
| FBE_005 | custom_sql | Amount and currency validation | High |
| FBE_006 | custom_sql | Billing period validation | Medium |

### dbt Test Scripts

```yaml
# tests/fact_billing_events_tests.yml
version: 2

models:
  - name: fact_billing_events
    description: "Gold layer fact table for billing events"
    columns:
      - name: billing_event_id
        description: "Unique identifier for billing event"
        tests:
          - unique
          - not_null
      
      - name: account_id
        description: "Foreign key to account"
        tests:
          - not_null
      
      - name: event_type
        description: "Type of billing event"
        tests:
          - not_null
          - accepted_values:
              values: ['subscription', 'usage_charge', 'refund', 'credit', 'invoice', 'payment']
      
      - name: event_timestamp
        description: "Timestamp of billing event"
        tests:
          - not_null
          - dbt_utils.expression_is_true:
              expression: "event_timestamp <= current_timestamp()"
      
      - name: amount
        description: "Billing amount"
        tests:
          - not_null
          - dbt_utils.expression_is_true:
              expression: "amount >= 0 OR event_type IN ('refund', 'credit')"
      
      - name: currency
        description: "Currency code"
        tests:
          - not_null
          - accepted_values:
              values: ['USD', 'EUR', 'GBP', 'CAD', 'AUD', 'JPY']
      
      - name: billing_period_start
        description: "Start of billing period"
        tests:
          - dbt_utils.expression_is_true:
              expression: "billing_period_start IS NULL OR billing_period_start <= billing_period_end"
```

```sql
-- tests/custom/test_fact_billing_events_amounts.sql
-- Test billing amounts and currency logic
SELECT *
FROM {{ ref('fact_billing_events') }}
WHERE 
    -- Negative amounts for non-refund/credit events
    (amount < 0 AND event_type NOT IN ('refund', 'credit'))
    -- Positive amounts for refund events
    OR (amount > 0 AND event_type = 'refund')
    -- Unrealistic amounts (over $100,000)
    OR ABS(amount) > 100000
    -- Invalid billing period
    OR (billing_period_start IS NOT NULL AND billing_period_end IS NOT NULL 
        AND billing_period_start > billing_period_end)
```

```sql
-- tests/custom/test_fact_billing_events_consistency.sql
-- Test billing event consistency
WITH billing_summary AS (
    SELECT 
        account_id,
        DATE_TRUNC('month', event_timestamp) as billing_month,
        SUM(CASE WHEN event_type IN ('subscription', 'usage_charge') THEN amount ELSE 0 END) as charges,
        SUM(CASE WHEN event_type IN ('refund', 'credit') THEN ABS(amount) ELSE 0 END) as credits,
        COUNT(*) as event_count
    FROM {{ ref('fact_billing_events') }}
    GROUP BY account_id, DATE_TRUNC('month', event_timestamp)
)
SELECT *
FROM billing_summary
WHERE 
    -- Accounts with only credits/refunds (suspicious)
    (charges = 0 AND credits > 0)
    -- Excessive number of events per month
    OR event_count > 1000
```

---

## Cross-Model Integration Tests

### Test Case Summary
| Test ID | Test Type | Description | Priority |
|---------|-----------|-------------|----------|
| INT_001 | custom_sql | User activity consistency across models | High |
| INT_002 | custom_sql | Meeting and participant data alignment | High |
| INT_003 | custom_sql | Feature usage during meetings | Medium |
| INT_004 | custom_sql | Billing events and user activity correlation | Medium |

### Integration Test Scripts

```sql
-- tests/integration/test_user_activity_consistency.sql
-- Test user activity consistency across fact tables
WITH user_activity_summary AS (
    SELECT user_id, COUNT(*) as activity_count
    FROM {{ ref('fact_user_activity') }}
    GROUP BY user_id
),
meeting_host_summary AS (
    SELECT host_user_id as user_id, COUNT(*) as hosted_meetings
    FROM {{ ref('fact_meeting_activity') }}
    GROUP BY host_user_id
),
feature_usage_summary AS (
    SELECT user_id, COUNT(*) as feature_usage_count
    FROM {{ ref('fact_feature_usage') }}
    GROUP BY user_id
)
SELECT 
    u.user_id,
    COALESCE(uas.activity_count, 0) as user_activities,
    COALESCE(mhs.hosted_meetings, 0) as hosted_meetings,
    COALESCE(fus.feature_usage_count, 0) as feature_usages
FROM {{ ref('si_users') }} u
LEFT JOIN user_activity_summary uas ON u.user_id = uas.user_id
LEFT JOIN meeting_host_summary mhs ON u.user_id = mhs.user_id
LEFT JOIN feature_usage_summary fus ON u.user_id = fus.user_id
WHERE 
    -- Users with feature usage but no user activity (suspicious)
    (fus.feature_usage_count > 0 AND uas.activity_count = 0)
    -- Users hosting meetings but no user activity
    OR (mhs.hosted_meetings > 0 AND uas.activity_count = 0)
```

```sql
-- tests/integration/test_meeting_participant_alignment.sql
-- Test alignment between meeting and participant data
WITH meeting_participants AS (
    SELECT 
        meeting_id,
        COUNT(DISTINCT participant_id) as unique_participants,
        SUM(duration_minutes) as total_participant_minutes
    FROM {{ ref('fact_participant_activity') }}
    GROUP BY meeting_id
)
SELECT 
    m.meeting_id,
    m.participant_count as reported_participants,
    COALESCE(mp.unique_participants, 0) as actual_participants,
    m.start_time,
    m.end_time,
    DATEDIFF('minute', m.start_time, m.end_time) as meeting_duration_minutes,
    COALESCE(mp.total_participant_minutes, 0) as total_participant_minutes
FROM {{ ref('fact_meeting_activity') }} m
LEFT JOIN meeting_participants mp ON m.meeting_id = mp.meeting_id
WHERE 
    -- Participant count mismatch
    ABS(m.participant_count - COALESCE(mp.unique_participants, 0)) > 0
    -- Impossible participant minutes (more than meeting duration * participants)
    OR (m.end_time IS NOT NULL AND m.start_time IS NOT NULL 
        AND mp.total_participant_minutes > 
        DATEDIFF('minute', m.start_time, m.end_time) * m.participant_count * 1.1)
```

---

## Test Execution Guidelines

### Running Tests
```bash
# Run all tests
dbt test

# Run tests for specific model
dbt test --select fact_user_activity

# Run only custom tests
dbt test --select test_type:custom

# Run tests with specific tag
dbt test --select tag:data_quality
```

### Test Categories
1. **Data Quality Tests**: Validate data integrity and business rules
2. **Relationship Tests**: Ensure referential integrity
3. **Performance Tests**: Validate query performance and optimization
4. **Edge Case Tests**: Handle boundary conditions and exceptions
5. **Integration Tests**: Cross-model consistency validation

### Success Criteria
- All unique and not_null tests must pass (0 failures)
- Relationship tests should have < 1% failure rate
- Custom business rule tests must pass completely
- Performance tests should complete within acceptable time limits
- Integration tests should show consistent data across models

### Monitoring and Alerting
- Set up automated test runs in CI/CD pipeline
- Configure alerts for test failures
- Monitor test execution times
- Track test coverage metrics
- Regular review of test results and patterns

---

## Version History
- **v1.0**: Initial comprehensive test suite for all 6 Gold Layer fact tables
- Created comprehensive unit tests covering happy path, edge cases, and integration scenarios
- Included dbt YAML configurations and custom SQL tests
- Added cross-model integration tests for data consistency validation