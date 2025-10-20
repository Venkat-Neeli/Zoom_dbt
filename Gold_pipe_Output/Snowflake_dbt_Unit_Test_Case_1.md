_____________________________________________
## *Author*: AAVA
## *Created on*: 2024-12-19
## *Description*: Comprehensive unit test cases for Gold layer fact tables including go_meeting_facts, go_participant_facts, go_webinar_facts, go_billing_facts, go_usage_facts, and go_quality_facts models
## *Version*: 1 
## *Updated on*: 2024-12-19
_____________________________________________

# Comprehensive Snowflake dbt Unit Test Cases for Gold Layer Fact Tables

## Metadata
- **Author**: AAVA
- **Created on**: 2024-12-19
- **Description**: Comprehensive unit test cases for Gold layer fact tables including go_meeting_facts, go_participant_facts, go_webinar_facts, go_billing_facts, go_usage_facts, and go_quality_facts models
- **Version**: 1.0
- **Updated on**: 2024-12-19
- **API Cost**: $0.15 USD (estimated for test execution and validation)

## Overview

This document provides comprehensive unit test cases for the Gold layer fact tables in the Zoom Customer Analytics platform. The test cases validate data transformations, business rules, edge cases, and error handling across all six fact table models.

## Test Case List

### GO_MEETING_FACTS Tests

| Test Case ID | Description | Expected Outcome |
|--------------|-------------|------------------|
| GMF-001 | Validate meeting_key uniqueness | All meeting_key values are unique |
| GMF-002 | Check non-null required fields | meeting_id, start_time, host_id are not null |
| GMF-003 | Validate engagement_score calculation | Engagement score between 0-100 |
| GMF-004 | Test participant_count accuracy | Participant count matches actual participants |
| GMF-005 | Validate duration calculations | Meeting duration is positive and realistic |
| GMF-006 | Test incremental load logic | Only new/updated records processed |
| GMF-007 | Edge case: Zero participants | Handle meetings with no participants |
| GMF-008 | Edge case: Null engagement metrics | Handle null engagement values gracefully |

### GO_PARTICIPANT_FACTS Tests

| Test Case ID | Description | Expected Outcome |
|--------------|-------------|------------------|
| GPF-001 | Validate participant_key uniqueness | All participant_key values are unique |
| GPF-002 | Check attendance duration logic | Attendance duration <= meeting duration |
| GPF-003 | Validate feature usage flags | Feature usage flags are boolean |
| GPF-004 | Test join_time vs leave_time | Leave time >= join time |
| GPF-005 | Validate participant-meeting relationship | All participants linked to valid meetings |
| GPF-006 | Edge case: Multiple join/leave events | Handle reconnections properly |
| GPF-007 | Edge case: Null user information | Handle anonymous participants |
| GPF-008 | Test audio/video quality metrics | Quality scores within valid ranges |

### GO_WEBINAR_FACTS Tests

| Test Case ID | Description | Expected Outcome |
|--------------|-------------|------------------|
| GWF-001 | Validate webinar_key uniqueness | All webinar_key values are unique |
| GWF-002 | Check attendance rate calculation | Attendance rate between 0-100% |
| GWF-003 | Validate registration vs attendance | Attendance <= registration count |
| GWF-004 | Test engagement metrics | Engagement metrics are non-negative |
| GWF-005 | Validate webinar duration | Duration matches scheduled time |
| GWF-006 | Edge case: Zero registrations | Handle webinars with no registrations |
| GWF-007 | Edge case: Cancelled webinars | Handle cancelled webinar status |
| GWF-008 | Test Q&A and poll metrics | Interactive feature metrics are accurate |

### GO_BILLING_FACTS Tests

| Test Case ID | Description | Expected Outcome |
|--------------|-------------|------------------|
| GBF-001 | Validate billing_key uniqueness | All billing_key values are unique |
| GBF-002 | Check revenue amount validation | Revenue amounts are non-negative |
| GBF-003 | Validate billing event types | Event types from accepted values list |
| GBF-004 | Test currency consistency | All amounts in consistent currency |
| GBF-005 | Validate billing date logic | Billing dates are realistic |
| GBF-006 | Edge case: Refund transactions | Handle negative revenue for refunds |
| GBF-007 | Edge case: Zero amount transactions | Handle promotional/free transactions |
| GBF-008 | Test subscription lifecycle | Validate subscription start/end dates |

### GO_USAGE_FACTS Tests

| Test Case ID | Description | Expected Outcome |
|--------------|-------------|------------------|
| GUF-001 | Validate usage_key uniqueness | All usage_key values are unique |
| GUF-002 | Check feature usage metrics | Usage counts are non-negative |
| GUF-003 | Validate platform metrics | Platform usage within expected ranges |
| GUF-004 | Test time-based aggregations | Usage aggregated correctly by time |
| GUF-005 | Validate user-feature relationships | All usage linked to valid users/features |
| GUF-006 | Edge case: Inactive users | Handle users with zero usage |
| GUF-007 | Edge case: Feature deprecation | Handle deprecated feature usage |
| GUF-008 | Test usage trend calculations | Usage trends calculated accurately |

### GO_QUALITY_FACTS Tests

| Test Case ID | Description | Expected Outcome |
|--------------|-------------|------------------|
| GQF-001 | Validate quality_key uniqueness | All quality_key values are unique |
| GQF-002 | Check connection quality scores | Quality scores between 1-5 |
| GQF-003 | Validate performance metrics | Performance metrics are realistic |
| GQF-004 | Test latency measurements | Latency values are positive |
| GQF-005 | Validate quality-session relationship | All quality records linked to sessions |
| GQF-006 | Edge case: Connection failures | Handle failed connection attempts |
| GQF-007 | Edge case: Missing quality data | Handle sessions without quality metrics |
| GQF-008 | Test quality trend analysis | Quality trends calculated properly |

## Cross-Model Integration Tests

| Test Case ID | Description | Expected Outcome |
|--------------|-------------|------------------|
| INT-001 | Meeting-Participant relationship | All participants belong to valid meetings |
| INT-002 | Usage-Billing correlation | Usage patterns align with billing events |
| INT-003 | Quality-Meeting correlation | Quality metrics exist for active meetings |
| INT-004 | Webinar-Participant relationship | Webinar attendees are valid participants |
| INT-005 | Cross-model date consistency | Date fields consistent across models |

## Performance and Data Quality Tests

| Test Case ID | Description | Expected Outcome |
|--------------|-------------|------------------|
| PER-001 | Incremental load performance | Models complete within SLA timeframes |
| PER-002 | Data freshness validation | Data updated within expected intervals |
| PER-003 | Row count validation | Row counts within expected ranges |
| PER-004 | Duplicate detection | No unexpected duplicate records |
| PER-005 | Audit trail completeness | All records have audit information |

## dbt Test Scripts

### YAML-based Schema Tests

```yaml
# dbt Schema Tests for Gold Layer Fact Tables
# Author: AAVA
# Version: 1.0
# Created: 2024-12-19

version: 2

models:
  # GO_MEETING_FACTS Tests
  - name: go_meeting_facts
    description: "Gold layer fact table for meeting analytics"
    tests:
      - dbt_utils.unique_combination_of_columns:
          combination_of_columns:
            - meeting_key
      - dbt_utils.expression_is_true:
          expression: "participant_count >= 0"
      - dbt_utils.expression_is_true:
          expression: "engagement_score BETWEEN 0 AND 100 OR engagement_score IS NULL"
      - dbt_utils.expression_is_true:
          expression: "duration_minutes > 0 OR duration_minutes IS NULL"
    columns:
      - name: meeting_key
        description: "Unique identifier for meeting"
        tests:
          - unique
          - not_null
      - name: meeting_id
        description: "Source meeting ID"
        tests:
          - not_null
      - name: host_id
        description: "Meeting host identifier"
        tests:
          - not_null
      - name: start_time
        description: "Meeting start timestamp"
        tests:
          - not_null
      - name: participant_count
        description: "Number of participants"
        tests:
          - dbt_utils.expression_is_true:
              expression: ">= 0"
      - name: engagement_score
        description: "Meeting engagement score"
        tests:
          - dbt_utils.expression_is_true:
              expression: "BETWEEN 0 AND 100 OR IS NULL"

  # GO_PARTICIPANT_FACTS Tests
  - name: go_participant_facts
    description: "Gold layer fact table for participant analytics"
    tests:
      - dbt_utils.unique_combination_of_columns:
          combination_of_columns:
            - participant_key
      - dbt_utils.expression_is_true:
          expression: "attendance_duration_minutes >= 0"
      - dbt_utils.expression_is_true:
          expression: "leave_time >= join_time OR leave_time IS NULL"
    columns:
      - name: participant_key
        description: "Unique identifier for participant session"
        tests:
          - unique
          - not_null
      - name: meeting_key
        description: "Foreign key to meeting"
        tests:
          - not_null
          - relationships:
              to: ref('go_meeting_facts')
              field: meeting_key
      - name: participant_id
        description: "Participant identifier"
        tests:
          - not_null
      - name: join_time
        description: "Participant join timestamp"
        tests:
          - not_null
      - name: attendance_duration_minutes
        description: "Duration of attendance"
        tests:
          - dbt_utils.expression_is_true:
              expression: ">= 0"

  # GO_WEBINAR_FACTS Tests
  - name: go_webinar_facts
    description: "Gold layer fact table for webinar analytics"
    tests:
      - dbt_utils.unique_combination_of_columns:
          combination_of_columns:
            - webinar_key
      - dbt_utils.expression_is_true:
          expression: "attendance_rate BETWEEN 0 AND 100 OR attendance_rate IS NULL"
      - dbt_utils.expression_is_true:
          expression: "actual_attendees <= registered_count OR registered_count IS NULL"
    columns:
      - name: webinar_key
        description: "Unique identifier for webinar"
        tests:
          - unique
          - not_null
      - name: webinar_id
        description: "Source webinar ID"
        tests:
          - not_null
      - name: host_id
        description: "Webinar host identifier"
        tests:
          - not_null
      - name: scheduled_start_time
        description: "Scheduled start timestamp"
        tests:
          - not_null
      - name: registered_count
        description: "Number of registrations"
        tests:
          - dbt_utils.expression_is_true:
              expression: ">= 0"
      - name: actual_attendees
        description: "Actual number of attendees"
        tests:
          - dbt_utils.expression_is_true:
              expression: ">= 0"
      - name: attendance_rate
        description: "Attendance rate percentage"
        tests:
          - dbt_utils.expression_is_true:
              expression: "BETWEEN 0 AND 100 OR IS NULL"

  # GO_BILLING_FACTS Tests
  - name: go_billing_facts
    description: "Gold layer fact table for billing analytics"
    tests:
      - dbt_utils.unique_combination_of_columns:
          combination_of_columns:
            - billing_key
      - dbt_utils.expression_is_true:
          expression: "amount >= 0 OR event_type = 'REFUND'"
    columns:
      - name: billing_key
        description: "Unique identifier for billing event"
        tests:
          - unique
          - not_null
      - name: account_id
        description: "Account identifier"
        tests:
          - not_null
      - name: event_type
        description: "Type of billing event"
        tests:
          - not_null
          - accepted_values:
              values: ['CHARGE', 'REFUND', 'SUBSCRIPTION', 'UPGRADE', 'DOWNGRADE', 'CANCELLATION']
      - name: amount
        description: "Billing amount"
        tests:
          - not_null
      - name: currency
        description: "Currency code"
        tests:
          - not_null
          - accepted_values:
              values: ['USD', 'EUR', 'GBP', 'JPY', 'CAD', 'AUD']
      - name: billing_date
        description: "Billing event date"
        tests:
          - not_null

  # GO_USAGE_FACTS Tests
  - name: go_usage_facts
    description: "Gold layer fact table for usage analytics"
    tests:
      - dbt_utils.unique_combination_of_columns:
          combination_of_columns:
            - usage_key
      - dbt_utils.expression_is_true:
          expression: "usage_count >= 0"
    columns:
      - name: usage_key
        description: "Unique identifier for usage event"
        tests:
          - unique
          - not_null
      - name: user_id
        description: "User identifier"
        tests:
          - not_null
      - name: feature_name
        description: "Feature being used"
        tests:
          - not_null
          - accepted_values:
              values: ['MEETING', 'WEBINAR', 'CHAT', 'PHONE', 'ROOMS', 'CLOUD_RECORDING', 'BREAKOUT_ROOMS']
      - name: usage_count
        description: "Usage frequency"
        tests:
          - not_null
          - dbt_utils.expression_is_true:
              expression: ">= 0"
      - name: usage_date
        description: "Usage date"
        tests:
          - not_null

  # GO_QUALITY_FACTS Tests
  - name: go_quality_facts
    description: "Gold layer fact table for quality analytics"
    tests:
      - dbt_utils.unique_combination_of_columns:
          combination_of_columns:
            - quality_key
      - dbt_utils.expression_is_true:
          expression: "connection_quality_score BETWEEN 1 AND 5 OR connection_quality_score IS NULL"
      - dbt_utils.expression_is_true:
          expression: "latency_ms >= 0 OR latency_ms IS NULL"
    columns:
      - name: quality_key
        description: "Unique identifier for quality measurement"
        tests:
          - unique
          - not_null
      - name: session_id
        description: "Session identifier"
        tests:
          - not_null
      - name: user_id
        description: "User identifier"
        tests:
          - not_null
      - name: connection_quality_score
        description: "Connection quality score (1-5)"
        tests:
          - dbt_utils.expression_is_true:
              expression: "BETWEEN 1 AND 5 OR IS NULL"
      - name: latency_ms
        description: "Connection latency in milliseconds"
        tests:
          - dbt_utils.expression_is_true:
              expression: ">= 0 OR IS NULL"
      - name: packet_loss_percentage
        description: "Packet loss percentage"
        tests:
          - dbt_utils.expression_is_true:
              expression: "BETWEEN 0 AND 100 OR IS NULL"
      - name: measurement_timestamp
        description: "Quality measurement timestamp"
        tests:
          - not_null
```

### Custom SQL-based dbt Tests

```sql
-- Custom SQL-based dbt Tests for Gold Layer Fact Tables
-- Author: AAVA
-- Version: 1.0
-- Created: 2024-12-19

-- Test: GMF-006 - Validate incremental load logic for meetings
-- tests/test_meeting_facts_incremental_load.sql
SELECT 
    meeting_key,
    updated_at,
    COUNT(*) as duplicate_count
FROM {{ ref('go_meeting_facts') }}
WHERE updated_at >= CURRENT_DATE - 7  -- Last 7 days
GROUP BY meeting_key, updated_at
HAVING COUNT(*) > 1;

-- Test: GMF-007 - Edge case: Meetings with zero participants
-- tests/test_meeting_facts_zero_participants.sql
SELECT 
    meeting_key,
    participant_count,
    duration_minutes
FROM {{ ref('go_meeting_facts') }}
WHERE participant_count = 0 
  AND duration_minutes > 60  -- Suspicious: long meeting with no participants
;

-- Test: GPF-006 - Handle multiple join/leave events properly
-- tests/test_participant_facts_multiple_sessions.sql
WITH participant_sessions AS (
    SELECT 
        meeting_key,
        participant_id,
        COUNT(*) as session_count,
        SUM(attendance_duration_minutes) as total_duration
    FROM {{ ref('go_participant_facts') }}
    GROUP BY meeting_key, participant_id
),
meeting_duration AS (
    SELECT 
        meeting_key,
        duration_minutes
    FROM {{ ref('go_meeting_facts') }}
)
SELECT 
    ps.meeting_key,
    ps.participant_id,
    ps.total_duration,
    md.duration_minutes,
    ps.session_count
FROM participant_sessions ps
JOIN meeting_duration md ON ps.meeting_key = md.meeting_key
WHERE ps.total_duration > md.duration_minutes * 1.1  -- Allow 10% tolerance
;

-- Test: GWF-003 - Validate registration vs attendance logic
-- tests/test_webinar_facts_attendance_validation.sql
SELECT 
    webinar_key,
    registered_count,
    actual_attendees,
    attendance_rate,
    CASE 
        WHEN registered_count > 0 THEN 
            ROUND((actual_attendees::FLOAT / registered_count) * 100, 2)
        ELSE NULL 
    END as calculated_attendance_rate
FROM {{ ref('go_webinar_facts') }}
WHERE ABS(attendance_rate - calculated_attendance_rate) > 1  -- Allow 1% tolerance
   OR (actual_attendees > registered_count AND registered_count > 0)
;

-- Test: GBF-006 - Validate refund transaction logic
-- tests/test_billing_facts_refund_validation.sql
SELECT 
    billing_key,
    account_id,
    event_type,
    amount,
    currency,
    billing_date
FROM {{ ref('go_billing_facts') }}
WHERE event_type = 'REFUND' 
  AND amount > 0  -- Refunds should have negative or zero amounts
;

-- Test: GBF-008 - Validate subscription lifecycle
-- tests/test_billing_facts_subscription_lifecycle.sql
WITH subscription_events AS (
    SELECT 
        account_id,
        event_type,
        billing_date,
        LAG(event_type) OVER (PARTITION BY account_id ORDER BY billing_date) as prev_event,
        LAG(billing_date) OVER (PARTITION BY account_id ORDER BY billing_date) as prev_date
    FROM {{ ref('go_billing_facts') }}
    WHERE event_type IN ('SUBSCRIPTION', 'UPGRADE', 'DOWNGRADE', 'CANCELLATION')
)
SELECT 
    account_id,
    event_type,
    prev_event,
    billing_date,
    prev_date
FROM subscription_events
WHERE 
    -- Invalid transitions
    (event_type = 'SUBSCRIPTION' AND prev_event = 'SUBSCRIPTION' AND DATEDIFF('day', prev_date, billing_date) < 30)
    OR (event_type IN ('UPGRADE', 'DOWNGRADE') AND prev_event = 'CANCELLATION')
    OR (event_type = 'CANCELLATION' AND prev_event = 'CANCELLATION')
;

-- Test: GUF-007 - Handle deprecated feature usage
-- tests/test_usage_facts_deprecated_features.sql
WITH deprecated_features AS (
    SELECT feature_name 
    FROM (
        VALUES 
            ('LEGACY_CHAT'),
            ('OLD_PHONE_SYSTEM'),
            ('DEPRECATED_API')
    ) AS t(feature_name)
)
SELECT 
    uf.usage_key,
    uf.feature_name,
    uf.usage_date,
    uf.usage_count
FROM {{ ref('go_usage_facts') }} uf
JOIN deprecated_features df ON uf.feature_name = df.feature_name
WHERE uf.usage_date >= CURRENT_DATE - 30  -- Recent usage of deprecated features
;

-- Test: GQF-006 - Handle connection failures properly
-- tests/test_quality_facts_connection_failures.sql
SELECT 
    quality_key,
    session_id,
    connection_quality_score,
    latency_ms,
    packet_loss_percentage,
    measurement_timestamp
FROM {{ ref('go_quality_facts') }}
WHERE 
    (connection_quality_score IS NULL AND latency_ms IS NOT NULL)  -- Inconsistent null handling
    OR (packet_loss_percentage > 50 AND connection_quality_score > 3)  -- High packet loss but good quality score
    OR (latency_ms > 1000 AND connection_quality_score > 3)  -- High latency but good quality score
;

-- Test: INT-001 - Cross-model relationship validation
-- tests/test_cross_model_meeting_participant_relationship.sql
SELECT 
    pf.participant_key,
    pf.meeting_key,
    pf.participant_id
FROM {{ ref('go_participant_facts') }} pf
LEFT JOIN {{ ref('go_meeting_facts') }} mf ON pf.meeting_key = mf.meeting_key
WHERE mf.meeting_key IS NULL  -- Orphaned participants
;

-- Test: INT-002 - Usage-Billing correlation
-- tests/test_cross_model_usage_billing_correlation.sql
WITH high_usage_accounts AS (
    SELECT 
        user_id,
        COUNT(*) as usage_events,
        COUNT(DISTINCT feature_name) as features_used
    FROM {{ ref('go_usage_facts') }}
    WHERE usage_date >= CURRENT_DATE - 30
    GROUP BY user_id
    HAVING COUNT(*) > 1000  -- High usage threshold
),
billing_accounts AS (
    SELECT DISTINCT account_id
    FROM {{ ref('go_billing_facts') }}
    WHERE billing_date >= CURRENT_DATE - 30
      AND event_type IN ('CHARGE', 'SUBSCRIPTION')
)
SELECT 
    hua.user_id,
    hua.usage_events,
    hua.features_used
FROM high_usage_accounts hua
LEFT JOIN billing_accounts ba ON hua.user_id = ba.account_id
WHERE ba.account_id IS NULL  -- High usage but no billing events
;

-- Test: PER-002 - Data freshness validation
-- tests/test_data_freshness_validation.sql
WITH model_freshness AS (
    SELECT 
        'go_meeting_facts' as model_name,
        MAX(created_at) as last_update,
        COUNT(*) as record_count
    FROM {{ ref('go_meeting_facts') }}
    WHERE created_at >= CURRENT_DATE - 1
    
    UNION ALL
    
    SELECT 
        'go_participant_facts' as model_name,
        MAX(created_at) as last_update,
        COUNT(*) as record_count
    FROM {{ ref('go_participant_facts') }}
    WHERE created_at >= CURRENT_DATE - 1
    
    UNION ALL
    
    SELECT 
        'go_webinar_facts' as model_name,
        MAX(created_at) as last_update,
        COUNT(*) as record_count
    FROM {{ ref('go_webinar_facts') }}
    WHERE created_at >= CURRENT_DATE - 1
    
    UNION ALL
    
    SELECT 
        'go_billing_facts' as model_name,
        MAX(created_at) as last_update,
        COUNT(*) as record_count
    FROM {{ ref('go_billing_facts') }}
    WHERE created_at >= CURRENT_DATE - 1
    
    UNION ALL
    
    SELECT 
        'go_usage_facts' as model_name,
        MAX(created_at) as last_update,
        COUNT(*) as record_count
    FROM {{ ref('go_usage_facts') }}
    WHERE created_at >= CURRENT_DATE - 1
    
    UNION ALL
    
    SELECT 
        'go_quality_facts' as model_name,
        MAX(created_at) as last_update,
        COUNT(*) as record_count
    FROM {{ ref('go_quality_facts') }}
    WHERE created_at >= CURRENT_DATE - 1
)
SELECT 
    model_name,
    last_update,
    record_count,
    DATEDIFF('hour', last_update, CURRENT_TIMESTAMP) as hours_since_update
FROM model_freshness
WHERE 
    DATEDIFF('hour', last_update, CURRENT_TIMESTAMP) > 24  -- Data older than 24 hours
    OR record_count = 0  -- No recent records
;

-- Test: PER-004 - Duplicate detection across models
-- tests/test_duplicate_detection.sql
WITH duplicate_meetings AS (
    SELECT meeting_id, COUNT(*) as dup_count
    FROM {{ ref('go_meeting_facts') }}
    GROUP BY meeting_id
    HAVING COUNT(*) > 1
),
duplicate_participants AS (
    SELECT meeting_key, participant_id, join_time, COUNT(*) as dup_count
    FROM {{ ref('go_participant_facts') }}
    GROUP BY meeting_key, participant_id, join_time
    HAVING COUNT(*) > 1
),
duplicate_webinars AS (
    SELECT webinar_id, COUNT(*) as dup_count
    FROM {{ ref('go_webinar_facts') }}
    GROUP BY webinar_id
    HAVING COUNT(*) > 1
)
SELECT 'go_meeting_facts' as model_name, meeting_id as duplicate_key, dup_count
FROM duplicate_meetings
UNION ALL
SELECT 'go_participant_facts' as model_name, 
       CONCAT(meeting_key, '-', participant_id, '-', join_time) as duplicate_key, 
       dup_count
FROM duplicate_participants
UNION ALL
SELECT 'go_webinar_facts' as model_name, webinar_id as duplicate_key, dup_count
FROM duplicate_webinars
;
```

## Test Execution Instructions

### Running Schema Tests
```bash
# Run all schema tests
dbt test

# Run tests for specific model
dbt test --models go_meeting_facts

# Run specific test types
dbt test --select test_type:unique
dbt test --select test_type:not_null
dbt test --select test_type:relationships
```

### Running Custom SQL Tests
```bash
# Run all tests including custom SQL tests
dbt test

# Run specific custom test
dbt test --select test_meeting_facts_incremental_load

# Run tests with specific tags
dbt test --select tag:data_quality
dbt test --select tag:cross_model
```

### Test Results Tracking

Test results are automatically tracked in:
- **dbt's run_results.json**: Contains detailed test execution results
- **Snowflake audit schema**: Stores test execution history and outcomes
- **dbt Cloud/Core logs**: Provides detailed test execution logs

### Monitoring and Alerting

Set up monitoring for:
- Test failure rates
- Data freshness violations
- Cross-model relationship failures
- Performance degradation alerts

---

*This comprehensive test suite ensures robust validation of all Gold layer fact tables with focus on data quality, business rule compliance, edge case handling, and cross-model integration testing.*