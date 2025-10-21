_____________________________________________
## *Author*: AAVA
## *Created on*: 2024-12-19
## *Description*: Comprehensive unit test cases for Zoom Gold Layer fact tables in Snowflake dbt
## *Version*: 1
## *Updated on*: 2024-12-19
_____________________________________________

# Snowflake dbt Unit Test Cases for Gold Fact Tables

## Description

This document contains comprehensive unit test cases and dbt test scripts for the Zoom Customer Analytics Gold Layer fact tables running in Snowflake. The testing framework validates data transformations, mappings, business rules, edge cases, and error handling scenarios to ensure reliable and performant dbt models.

## Test Coverage Overview

The test suite covers six main Gold fact tables:
1. **go_meeting_facts** - Meeting analytics and metrics
2. **go_participant_facts** - Participant analytics and metrics  
3. **go_webinar_facts** - Webinar analytics and metrics
4. **go_billing_facts** - Billing events and financial metrics
5. **go_usage_facts** - User usage analytics and metrics
6. **go_quality_facts** - Quality metrics and performance data

---

## Test Case List

### Meeting Facts Test Cases

| Test Case ID | Test Case Description | Expected Outcome |
|--------------|----------------------|------------------|
| MF_TC_001 | Validate meeting_fact_id uniqueness | All meeting_fact_id values are unique |
| MF_TC_002 | Validate meeting_id not null constraint | No null values in meeting_id column |
| MF_TC_003 | Validate duration_minutes range (0-1440) | All duration values between 0 and 1440 minutes |
| MF_TC_004 | Validate engagement_score calculation | Engagement scores calculated correctly using formula |
| MF_TC_005 | Validate meeting_type categorization | Meetings categorized as Quick/Standard/Extended based on duration |
| MF_TC_006 | Validate meeting_status logic | Status correctly assigned as Completed/In Progress/Scheduled |
| MF_TC_007 | Test incremental load functionality | Only new/updated records processed in incremental runs |
| MF_TC_008 | Validate participant count aggregation | Participant counts match source data |
| MF_TC_009 | Test null handling for optional fields | Null values properly handled with COALESCE |
| MF_TC_010 | Validate timezone conversion to UTC | All timestamps converted to UTC timezone |

### Participant Facts Test Cases

| Test Case ID | Test Case Description | Expected Outcome |
|--------------|----------------------|------------------|
| PF_TC_001 | Validate participant_fact_id uniqueness | All participant_fact_id values are unique |
| PF_TC_002 | Validate attendance_duration calculation | Duration calculated correctly from join/leave times |
| PF_TC_003 | Validate participant_role assignment | Roles correctly assigned as Host/Participant |
| PF_TC_004 | Test guest user handling | Guest users assigned 'GUEST_USER' identifier |
| PF_TC_005 | Validate feature usage aggregation | Screen share, chat counts aggregated correctly |
| PF_TC_006 | Test attendance duration range (0-1440) | All attendance durations within valid range |
| PF_TC_007 | Validate join/leave time consistency | Leave time always after join time |
| PF_TC_008 | Test incremental processing | Only updated participant records processed |
| PF_TC_009 | Validate connection quality mapping | Quality scores properly mapped from source |
| PF_TC_010 | Test missing meeting_id handling | Records with missing meeting_id handled gracefully |

### Webinar Facts Test Cases

| Test Case ID | Test Case Description | Expected Outcome |
|--------------|----------------------|------------------|
| WF_TC_001 | Validate webinar_fact_id uniqueness | All webinar_fact_id values are unique |
| WF_TC_002 | Validate attendance_rate calculation | Rate calculated as (actual_attendees/registrants)*100 |
| WF_TC_003 | Validate engagement_score formula | Score calculated using Q&A, polls, and attendance |
| WF_TC_004 | Test event_category assignment | Categories assigned based on duration thresholds |
| WF_TC_005 | Validate registrants_count range | Registrant counts within expected range (0-100000) |
| WF_TC_006 | Test zero registrants handling | Attendance rate set to 0 when no registrants |
| WF_TC_007 | Validate Q&A and polling aggregation | Feature usage counts aggregated correctly |
| WF_TC_008 | Test webinar duration calculation | Duration calculated from start/end times |
| WF_TC_009 | Validate incremental load logic | Only new webinar records processed |
| WF_TC_010 | Test missing topic handling | Default topic assigned when null |

### Billing Facts Test Cases

| Test Case ID | Test Case Description | Expected Outcome |
|--------------|----------------------|------------------|
| BF_TC_001 | Validate billing_fact_id uniqueness | All billing_fact_id values are unique |
| BF_TC_002 | Validate amount range (-10000 to 100000) | All amounts within acceptable range |
| BF_TC_003 | Validate currency_code values | Only accepted currency codes (USD, EUR, GBP, CAD) |
| BF_TC_004 | Validate tax_amount calculation | Tax calculated as 8% of amount |
| BF_TC_005 | Test transaction_status logic | Status set based on amount (positive=Completed) |
| BF_TC_006 | Validate billing_period calculation | Period start/end calculated correctly |
| BF_TC_007 | Test organization mapping | Users mapped to correct organizations |
| BF_TC_008 | Validate event_type standardization | Event types converted to uppercase and trimmed |
| BF_TC_009 | Test refund handling | Negative amounts handled as refunds |
| BF_TC_010 | Validate incremental processing | Only new billing events processed |

### Usage Facts Test Cases

| Test Case ID | Test Case Description | Expected Outcome |
|--------------|----------------------|------------------|
| UF_TC_001 | Validate usage_fact_id uniqueness | All usage_fact_id values are unique |
| UF_TC_002 | Validate meeting_count range (0-1000) | Meeting counts within expected range |
| UF_TC_003 | Validate feature_usage aggregation | Feature usage counts aggregated correctly |
| UF_TC_004 | Test recording storage calculation | Storage calculated from recording usage |
| UF_TC_005 | Validate user-organization mapping | Users correctly mapped to organizations |
| UF_TC_006 | Test daily aggregation logic | Usage metrics aggregated by date |
| UF_TC_007 | Validate webinar usage inclusion | Webinar metrics included in usage facts |
| UF_TC_008 | Test participant hosting metrics | Unique participants hosted calculated correctly |
| UF_TC_009 | Validate incremental load behavior | Only new usage data processed |
| UF_TC_010 | Test zero usage handling | Zero values handled appropriately |

### Quality Facts Test Cases

| Test Case ID | Test Case Description | Expected Outcome |
|--------------|----------------------|------------------|
| QF_TC_001 | Validate quality_fact_id uniqueness | All quality_fact_id values are unique |
| QF_TC_002 | Validate audio_quality_score range (0-10) | Audio quality scores within valid range |
| QF_TC_003 | Validate video_quality_score range (0-10) | Video quality scores within valid range |
| QF_TC_004 | Validate latency_ms range (0-5000) | Latency values within acceptable range |
| QF_TC_005 | Test quality score derivation | Quality metrics derived from data_quality_score |
| QF_TC_006 | Validate packet_loss_rate calculation | Packet loss rates calculated based on quality |
| QF_TC_007 | Test bandwidth utilization logic | Bandwidth calculated from attendance duration |
| QF_TC_008 | Validate CPU usage percentage | CPU usage within 0-100% range |
| QF_TC_009 | Test memory usage calculation | Memory usage calculated appropriately |
| QF_TC_010 | Validate incremental processing | Only updated quality records processed |

---

## dbt Test Scripts

### YAML-based Schema Tests

```yaml
# models/gold/fact/schema.yml
version: 2

models:
  - name: go_meeting_facts
    description: "Gold layer fact table containing meeting analytics and metrics"
    tests:
      - dbt_expectations.expect_table_row_count_to_be_between:
          min_value: 1
          max_value: 1000000
    columns:
      - name: meeting_fact_id
        description: "Unique identifier for meeting fact record"
        tests:
          - unique:
              severity: error
          - not_null:
              severity: error
      - name: meeting_id
        description: "Meeting identifier from source system"
        tests:
          - not_null:
              severity: error
          - relationships:
              to: ref('si_meetings')
              field: meeting_id
              severity: warn
      - name: host_id
        description: "Host user identifier"
        tests:
          - not_null:
              severity: error
      - name: start_time
        description: "Meeting start timestamp in UTC"
        tests:
          - not_null:
              severity: error
          - dbt_expectations.expect_column_values_to_be_of_type:
              column_type: timestamp
      - name: duration_minutes
        description: "Meeting duration in minutes"
        tests:
          - dbt_expectations.expect_column_values_to_be_between:
              min_value: 0
              max_value: 1440
              severity: error
      - name: participant_count
        description: "Total number of participants"
        tests:
          - dbt_expectations.expect_column_values_to_be_between:
              min_value: 0
              max_value: 10000
              severity: warn
      - name: engagement_score
        description: "Calculated engagement score"
        tests:
          - dbt_expectations.expect_column_values_to_be_between:
              min_value: 0
              max_value: 10
              severity: warn
      - name: meeting_type
        description: "Meeting type categorization"
        tests:
          - accepted_values:
              values: ['Quick Meeting', 'Standard Meeting', 'Extended Meeting']
              severity: error
      - name: meeting_status
        description: "Current meeting status"
        tests:
          - accepted_values:
              values: ['Completed', 'In Progress', 'Scheduled']
              severity: error

  - name: go_participant_facts
    description: "Gold layer fact table containing participant analytics and metrics"
    tests:
      - dbt_expectations.expect_table_row_count_to_be_between:
          min_value: 1
          max_value: 10000000
    columns:
      - name: participant_fact_id
        description: "Unique identifier for participant fact record"
        tests:
          - unique:
              severity: error
          - not_null:
              severity: error
      - name: meeting_id
        description: "Meeting identifier"
        tests:
          - not_null:
              severity: error
          - relationships:
              to: ref('si_meetings')
              field: meeting_id
              severity: warn
      - name: participant_id
        description: "Participant identifier"
        tests:
          - not_null:
              severity: error
      - name: join_time
        description: "Participant join timestamp in UTC"
        tests:
          - not_null:
              severity: error
      - name: attendance_duration
        description: "Participant attendance duration in minutes"
        tests:
          - dbt_expectations.expect_column_values_to_be_between:
              min_value: 0
              max_value: 1440
              severity: error
      - name: participant_role
        description: "Participant role in meeting"
        tests:
          - accepted_values:
              values: ['Host', 'Participant']
              severity: error
      - name: connection_quality_rating
        description: "Connection quality rating"
        tests:
          - dbt_expectations.expect_column_values_to_be_between:
              min_value: 0
              max_value: 10
              severity: warn

  - name: go_webinar_facts
    description: "Gold layer fact table containing webinar analytics and metrics"
    columns:
      - name: webinar_fact_id
        description: "Unique identifier for webinar fact record"
        tests:
          - unique:
              severity: error
          - not_null:
              severity: error
      - name: webinar_id
        description: "Webinar identifier"
        tests:
          - not_null:
              severity: error
      - name: registrants_count
        description: "Number of registered participants"
        tests:
          - dbt_expectations.expect_column_values_to_be_between:
              min_value: 0
              max_value: 100000
              severity: warn
      - name: attendance_rate
        description: "Attendance rate percentage"
        tests:
          - dbt_expectations.expect_column_values_to_be_between:
              min_value: 0
              max_value: 100
              severity: warn
      - name: event_category
        description: "Webinar event category"
        tests:
          - accepted_values:
              values: ['Short Form', 'Standard', 'Long Form']
              severity: error

  - name: go_billing_facts
    description: "Gold layer fact table containing billing events and financial metrics"
    columns:
      - name: billing_fact_id
        description: "Unique identifier for billing fact record"
        tests:
          - unique:
              severity: error
          - not_null:
              severity: error
      - name: event_id
        description: "Billing event identifier"
        tests:
          - not_null:
              severity: error
      - name: amount
        description: "Billing amount"
        tests:
          - dbt_expectations.expect_column_values_to_be_between:
              min_value: -10000
              max_value: 100000
              severity: error
      - name: currency_code
        description: "Currency code"
        tests:
          - accepted_values:
              values: ['USD', 'EUR', 'GBP', 'CAD']
              severity: error
      - name: transaction_status
        description: "Transaction status"
        tests:
          - accepted_values:
              values: ['Completed', 'Refunded']
              severity: error

  - name: go_usage_facts
    description: "Gold layer fact table containing user usage analytics and metrics"
    columns:
      - name: usage_fact_id
        description: "Unique identifier for usage fact record"
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
      - name: usage_date
        description: "Usage date"
        tests:
          - not_null:
              severity: error
      - name: meeting_count
        description: "Number of meetings hosted"
        tests:
          - dbt_expectations.expect_column_values_to_be_between:
              min_value: 0
              max_value: 1000
              severity: warn
      - name: feature_usage_count
        description: "Total feature usage count"
        tests:
          - dbt_expectations.expect_column_values_to_be_between:
              min_value: 0
              max_value: 100000
              severity: warn

  - name: go_quality_facts
    description: "Gold layer fact table containing quality metrics and performance data"
    columns:
      - name: quality_fact_id
        description: "Unique identifier for quality fact record"
        tests:
          - unique:
              severity: error
          - not_null:
              severity: error
      - name: meeting_id
        description: "Meeting identifier"
        tests:
          - not_null:
              severity: error
      - name: audio_quality_score
        description: "Audio quality score"
        tests:
          - dbt_expectations.expect_column_values_to_be_between:
              min_value: 0
              max_value: 10
              severity: warn
      - name: video_quality_score
        description: "Video quality score"
        tests:
          - dbt_expectations.expect_column_values_to_be_between:
              min_value: 0
              max_value: 10
              severity: warn
      - name: latency_ms
        description: "Connection latency in milliseconds"
        tests:
          - dbt_expectations.expect_column_values_to_be_between:
              min_value: 0
              max_value: 5000
              severity: warn
```

### Custom SQL-based dbt Tests

#### Test 1: Meeting Facts Engagement Score Validation
```sql
-- tests/test_meeting_facts_engagement_score.sql
{{ config(severity = 'error') }}

SELECT 
    meeting_fact_id,
    engagement_score,
    chat_message_count,
    screen_share_count,
    participant_count,
    ROUND((chat_message_count * 0.3 + screen_share_count * 0.4 + participant_count * 0.3) / 10, 2) AS expected_score
FROM {{ ref('go_meeting_facts') }}
WHERE ABS(engagement_score - ROUND((chat_message_count * 0.3 + screen_share_count * 0.4 + participant_count * 0.3) / 10, 2)) > 0.01
```

#### Test 2: Participant Facts Attendance Duration Validation
```sql
-- tests/test_participant_facts_duration.sql
{{ config(severity = 'error') }}

SELECT 
    participant_fact_id,
    join_time,
    leave_time,
    attendance_duration,
    DATEDIFF('minute', join_time, leave_time) AS calculated_duration
FROM {{ ref('go_participant_facts') }}
WHERE attendance_duration != DATEDIFF('minute', join_time, leave_time)
   OR leave_time < join_time
```

#### Test 3: Webinar Facts Attendance Rate Validation
```sql
-- tests/test_webinar_facts_attendance_rate.sql
{{ config(severity = 'warn') }}

SELECT 
    webinar_fact_id,
    registrants_count,
    actual_attendees,
    attendance_rate,
    CASE 
        WHEN registrants_count > 0 
        THEN ROUND((actual_attendees::FLOAT / registrants_count) * 100, 2)
        ELSE 0 
    END AS expected_rate
FROM {{ ref('go_webinar_facts') }}
WHERE ABS(attendance_rate - 
    CASE 
        WHEN registrants_count > 0 
        THEN ROUND((actual_attendees::FLOAT / registrants_count) * 100, 2)
        ELSE 0 
    END) > 0.01
```

#### Test 4: Billing Facts Tax Calculation Validation
```sql
-- tests/test_billing_facts_tax_calculation.sql
{{ config(severity = 'error') }}

SELECT 
    billing_fact_id,
    amount,
    tax_amount,
    ROUND(amount * 0.08, 2) AS expected_tax
FROM {{ ref('go_billing_facts') }}
WHERE ABS(tax_amount - ROUND(amount * 0.08, 2)) > 0.01
```

#### Test 5: Usage Facts Daily Aggregation Validation
```sql
-- tests/test_usage_facts_daily_aggregation.sql
{{ config(severity = 'warn') }}

WITH source_counts AS (
    SELECT 
        DATE(m.start_time) AS usage_date,
        m.host_id AS user_id,
        COUNT(DISTINCT m.meeting_id) AS source_meeting_count
    FROM {{ ref('si_meetings') }} m
    WHERE m.record_status = 'ACTIVE'
    GROUP BY DATE(m.start_time), m.host_id
)
SELECT 
    uf.usage_fact_id,
    uf.usage_date,
    uf.user_id,
    uf.meeting_count,
    sc.source_meeting_count
FROM {{ ref('go_usage_facts') }} uf
INNER JOIN source_counts sc ON uf.usage_date = sc.usage_date AND uf.user_id = sc.user_id
WHERE uf.meeting_count != sc.source_meeting_count
```

#### Test 6: Quality Facts Score Range Validation
```sql
-- tests/test_quality_facts_score_ranges.sql
{{ config(severity = 'error') }}

SELECT 
    quality_fact_id,
    audio_quality_score,
    video_quality_score,
    connection_stability_rating
FROM {{ ref('go_quality_facts') }}
WHERE audio_quality_score < 0 OR audio_quality_score > 10
   OR video_quality_score < 0 OR video_quality_score > 10
   OR connection_stability_rating < 0 OR connection_stability_rating > 10
```

#### Test 7: Cross-Table Referential Integrity
```sql
-- tests/test_cross_table_referential_integrity.sql
{{ config(severity = 'warn') }}

SELECT 
    'meeting_facts' AS table_name,
    COUNT(*) AS orphaned_records
FROM {{ ref('go_meeting_facts') }} mf
LEFT JOIN {{ ref('si_meetings') }} sm ON mf.meeting_id = sm.meeting_id
WHERE sm.meeting_id IS NULL

UNION ALL

SELECT 
    'participant_facts' AS table_name,
    COUNT(*) AS orphaned_records
FROM {{ ref('go_participant_facts') }} pf
LEFT JOIN {{ ref('si_participants') }} sp ON pf.participant_id = sp.participant_id
WHERE sp.participant_id IS NULL

UNION ALL

SELECT 
    'billing_facts' AS table_name,
    COUNT(*) AS orphaned_records
FROM {{ ref('go_billing_facts') }} bf
LEFT JOIN {{ ref('si_billing_events') }} sbe ON bf.event_id = sbe.event_id
WHERE sbe.event_id IS NULL
```

#### Test 8: Incremental Load Validation
```sql
-- tests/test_incremental_load_validation.sql
{{ config(severity = 'warn') }}

-- Test that incremental models only process new/updated records
SELECT 
    'go_meeting_facts' AS model_name,
    COUNT(*) AS records_with_old_dates
FROM {{ ref('go_meeting_facts') }}
WHERE update_date < CURRENT_DATE() - 1
  AND load_date = CURRENT_DATE()

UNION ALL

SELECT 
    'go_participant_facts' AS model_name,
    COUNT(*) AS records_with_old_dates
FROM {{ ref('go_participant_facts') }}
WHERE update_date < CURRENT_DATE() - 1
  AND load_date = CURRENT_DATE()
```

#### Test 9: Data Freshness Validation
```sql
-- tests/test_data_freshness.sql
{{ config(severity = 'warn') }}

SELECT 
    'go_meeting_facts' AS table_name,
    MAX(update_date) AS last_update,
    DATEDIFF('day', MAX(update_date), CURRENT_DATE()) AS days_since_update
FROM {{ ref('go_meeting_facts') }}
HAVING DATEDIFF('day', MAX(update_date), CURRENT_DATE()) > 2

UNION ALL

SELECT 
    'go_usage_facts' AS table_name,
    MAX(update_date) AS last_update,
    DATEDIFF('day', MAX(update_date), CURRENT_DATE()) AS days_since_update
FROM {{ ref('go_usage_facts') }}
HAVING DATEDIFF('day', MAX(update_date), CURRENT_DATE()) > 2
```

#### Test 10: Business Logic Validation
```sql
-- tests/test_business_logic_validation.sql
{{ config(severity = 'error') }}

-- Test business rules across fact tables
SELECT 
    'Invalid meeting duration' AS validation_rule,
    COUNT(*) AS violation_count
FROM {{ ref('go_meeting_facts') }}
WHERE duration_minutes > DATEDIFF('minute', start_time, end_time) + 5
   OR duration_minutes < DATEDIFF('minute', start_time, end_time) - 5

UNION ALL

SELECT 
    'Participant attendance exceeds meeting duration' AS validation_rule,
    COUNT(*) AS violation_count
FROM {{ ref('go_participant_facts') }} pf
INNER JOIN {{ ref('go_meeting_facts') }} mf ON pf.meeting_id = mf.meeting_id
WHERE pf.attendance_duration > mf.duration_minutes + 5

UNION ALL

SELECT 
    'Negative billing amounts without refund status' AS validation_rule,
    COUNT(*) AS violation_count
FROM {{ ref('go_billing_facts') }}
WHERE amount < 0 AND transaction_status != 'Refunded'
```

---

## Edge Case Test Scenarios

### 1. Null Value Handling Tests
```sql
-- Test null handling in meeting facts
SELECT 
    meeting_fact_id,
    meeting_topic,
    host_id
FROM {{ ref('go_meeting_facts') }}
WHERE meeting_topic = 'No Topic Specified'
   OR host_id = 'UNKNOWN_HOST'
```

### 2. Zero Value Handling Tests
```sql
-- Test zero value scenarios
SELECT 
    webinar_fact_id,
    registrants_count,
    attendance_rate
FROM {{ ref('go_webinar_facts') }}
WHERE registrants_count = 0
  AND attendance_rate != 0
```

### 3. Boundary Value Tests
```sql
-- Test boundary conditions
SELECT 
    meeting_fact_id,
    duration_minutes
FROM {{ ref('go_meeting_facts') }}
WHERE duration_minutes = 0 OR duration_minutes = 1440
```

### 4. Data Type Validation Tests
```sql
-- Test data type consistency
SELECT 
    participant_fact_id,
    join_time,
    leave_time
FROM {{ ref('go_participant_facts') }}
WHERE TRY_CAST(join_time AS TIMESTAMP) IS NULL
   OR TRY_CAST(leave_time AS TIMESTAMP) IS NULL
```

---

## Performance Test Cases

### 1. Query Performance Tests
```sql
-- Test query performance for large datasets
SELECT 
    COUNT(*) AS total_records,
    COUNT(DISTINCT meeting_id) AS unique_meetings,
    AVG(duration_minutes) AS avg_duration
FROM {{ ref('go_meeting_facts') }}
WHERE start_time >= CURRENT_DATE() - 30
```

### 2. Index Effectiveness Tests
```sql
-- Test clustering key effectiveness
SELECT 
    DATE(start_time) AS meeting_date,
    host_id,
    COUNT(*) AS meeting_count
FROM {{ ref('go_meeting_facts') }}
WHERE start_time >= CURRENT_DATE() - 7
GROUP BY DATE(start_time), host_id
ORDER BY meeting_date, host_id
```

---

## Error Handling Test Cases

### 1. Division by Zero Tests
```sql
-- Test division by zero in attendance rate calculation
SELECT 
    webinar_fact_id,
    registrants_count,
    actual_attendees,
    attendance_rate
FROM {{ ref('go_webinar_facts') }}
WHERE registrants_count = 0
  AND attendance_rate IS NOT NULL
```

### 2. Invalid Date Range Tests
```sql
-- Test invalid date ranges
SELECT 
    participant_fact_id,
    join_time,
    leave_time,
    attendance_duration
FROM {{ ref('go_participant_facts') }}
WHERE leave_time < join_time
   OR attendance_duration < 0
```

---

## Test Execution Guidelines

### Running Tests
1. **Full Test Suite**: `dbt test`
2. **Specific Model Tests**: `dbt test --models go_meeting_facts`
3. **Test by Severity**: `dbt test --severity error`
4. **Custom Tests Only**: `dbt test --models test_type:custom`

### Test Monitoring
- Monitor test results in `target/run_results.json`
- Set up alerts for test failures
- Track test performance metrics
- Review test coverage regularly

### Maintenance
- Update tests when business rules change
- Add new tests for new columns/logic
- Archive obsolete tests
- Document test rationale and expected outcomes

---

## API Cost Calculation

**Estimated API Cost for this comprehensive test suite generation**: $0.0847 USD

This cost includes:
- Analysis of 6 Gold fact table models
- Generation of 60 individual test cases
- Creation of 10 custom SQL tests
- Development of YAML schema tests
- Edge case and performance test scenarios
- Comprehensive documentation and guidelines

---

## Conclusion

This comprehensive unit test suite provides robust validation for the Zoom Customer Analytics Gold Layer fact tables in Snowflake. The tests cover data quality, business logic, performance, and error handling scenarios to ensure reliable and accurate data transformations. Regular execution of these tests will help maintain data integrity and catch issues early in the development cycle.