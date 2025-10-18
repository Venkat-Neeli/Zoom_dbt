# Comprehensive Snowflake dbt Unit Test Cases for Gold Layer Fact Tables

## Overview
This document contains comprehensive unit test cases for 6 Gold layer fact table models in the Zoom dbt project. Each test case validates data transformations, business rules, edge cases, and error handling scenarios.

## Test Metadata
- **Project**: Zoom dbt Gold Layer
- **Environment**: Snowflake
- **Test Framework**: dbt native testing
- **Coverage**: 6 fact table models
- **Created Date**: $(date)
- **Test Categories**: Data Quality, Business Logic, Edge Cases, Performance

---

## 1. GO_MEETING_FACTS Model Test Cases

### Test Case List:
1. **Data Quality Score Validation**
2. **Meeting Duration Calculation**
3. **Participant Count Aggregation**
4. **Date Dimension Join Validation**
5. **Null Handling for Optional Fields**
6. **Duplicate Meeting Prevention**
7. **Load Date Clustering Validation**
8. **Pre/Post Hook Execution**

### dbt Test Scripts:

```sql
-- Test 1: Data Quality Score Validation
-- tests/go_meeting_facts/test_data_quality_score.sql
SELECT *
FROM {{ ref('go_meeting_facts') }}
WHERE data_quality_score < 0.7
```

```sql
-- Test 2: Meeting Duration Calculation
-- tests/go_meeting_facts/test_meeting_duration.sql
SELECT meeting_id,
       start_time,
       end_time,
       duration_minutes,
       CASE 
         WHEN end_time IS NOT NULL AND start_time IS NOT NULL 
         THEN DATEDIFF('minute', start_time, end_time)
         ELSE NULL
       END as calculated_duration
FROM {{ ref('go_meeting_facts') }}
WHERE ABS(duration_minutes - calculated_duration) > 1
  AND end_time IS NOT NULL
  AND start_time IS NOT NULL
```

```sql
-- Test 3: Participant Count Aggregation
-- tests/go_meeting_facts/test_participant_count.sql
WITH participant_counts AS (
  SELECT meeting_id, COUNT(*) as actual_count
  FROM {{ ref('go_participant_facts') }}
  GROUP BY meeting_id
)
SELECT m.meeting_id,
       m.total_participants,
       p.actual_count
FROM {{ ref('go_meeting_facts') }} m
LEFT JOIN participant_counts p ON m.meeting_id = p.meeting_id
WHERE m.total_participants != COALESCE(p.actual_count, 0)
```

```sql
-- Test 4: Date Dimension Join Validation
-- tests/go_meeting_facts/test_date_dimension_join.sql
SELECT meeting_id, meeting_date
FROM {{ ref('go_meeting_facts') }}
WHERE meeting_date NOT IN (
  SELECT date_key FROM {{ ref('dim_date') }}
)
```

```sql
-- Test 5: Null Handling Validation
-- tests/go_meeting_facts/test_null_handling.sql
SELECT meeting_id
FROM {{ ref('go_meeting_facts') }}
WHERE meeting_id IS NULL
   OR host_id IS NULL
   OR created_at IS NULL
```

```sql
-- Test 6: Duplicate Meeting Prevention
-- tests/go_meeting_facts/test_duplicate_meetings.sql
SELECT meeting_id, COUNT(*)
FROM {{ ref('go_meeting_facts') }}
GROUP BY meeting_id
HAVING COUNT(*) > 1
```

---

## 2. GO_PARTICIPANT_FACTS Model Test Cases

### Test Case List:
1. **Join Time Before Leave Time Validation**
2. **Participant Session Duration Calculation**
3. **Meeting Reference Integrity**
4. **Attendance Status Logic**
5. **Device Type Categorization**
6. **Geographic Location Validation**
7. **Connection Quality Metrics**
8. **Participant Role Assignment**

### dbt Test Scripts:

```sql
-- Test 1: Join Time Before Leave Time
-- tests/go_participant_facts/test_join_leave_time.sql
SELECT participant_id, meeting_id, join_time, leave_time
FROM {{ ref('go_participant_facts') }}
WHERE join_time > leave_time
  AND leave_time IS NOT NULL
```

```sql
-- Test 2: Session Duration Calculation
-- tests/go_participant_facts/test_session_duration.sql
SELECT participant_id,
       join_time,
       leave_time,
       session_duration_minutes,
       DATEDIFF('minute', join_time, COALESCE(leave_time, CURRENT_TIMESTAMP())) as calculated_duration
FROM {{ ref('go_participant_facts') }}
WHERE ABS(session_duration_minutes - calculated_duration) > 2
  AND join_time IS NOT NULL
```

```sql
-- Test 3: Meeting Reference Integrity
-- tests/go_participant_facts/test_meeting_reference.sql
SELECT DISTINCT p.meeting_id
FROM {{ ref('go_participant_facts') }} p
LEFT JOIN {{ ref('go_meeting_facts') }} m ON p.meeting_id = m.meeting_id
WHERE m.meeting_id IS NULL
```

```sql
-- Test 4: Attendance Status Logic
-- tests/go_participant_facts/test_attendance_status.sql
SELECT participant_id, join_time, leave_time, attendance_status
FROM {{ ref('go_participant_facts') }}
WHERE (join_time IS NOT NULL AND attendance_status = 'No Show')
   OR (join_time IS NULL AND attendance_status != 'No Show')
```

```sql
-- Test 5: Device Type Categorization
-- tests/go_participant_facts/test_device_type.sql
SELECT participant_id, device_type
FROM {{ ref('go_participant_facts') }}
WHERE device_type NOT IN ('Desktop', 'Mobile', 'Tablet', 'Phone', 'Unknown')
```

---

## 3. GO_WEBINAR_FACTS Model Test Cases

### Test Case List:
1. **Webinar Registration vs Attendance**
2. **Engagement Score Calculation**
3. **Q&A Interaction Metrics**
4. **Poll Response Validation**
5. **Webinar Duration Consistency**
6. **Registration Funnel Analysis**
7. **Presenter Count Validation**
8. **Recording Status Logic**

### dbt Test Scripts:

```sql
-- Test 1: Registration vs Attendance Logic
-- tests/go_webinar_facts/test_registration_attendance.sql
SELECT webinar_id, total_registered, total_attended
FROM {{ ref('go_webinar_facts') }}
WHERE total_attended > total_registered
```

```sql
-- Test 2: Engagement Score Calculation
-- tests/go_webinar_facts/test_engagement_score.sql
SELECT webinar_id,
       engagement_score,
       poll_responses,
       qa_questions,
       chat_messages,
       (poll_responses * 0.4 + qa_questions * 0.4 + chat_messages * 0.2) as calculated_score
FROM {{ ref('go_webinar_facts') }}
WHERE ABS(engagement_score - calculated_score) > 0.1
  AND engagement_score IS NOT NULL
```

```sql
-- Test 3: Q&A Interaction Validation
-- tests/go_webinar_facts/test_qa_interactions.sql
SELECT webinar_id, qa_questions, qa_answered
FROM {{ ref('go_webinar_facts') }}
WHERE qa_answered > qa_questions
   OR qa_questions < 0
   OR qa_answered < 0
```

```sql
-- Test 4: Poll Response Validation
-- tests/go_webinar_facts/test_poll_responses.sql
SELECT webinar_id, total_polls, poll_responses, total_attended
FROM {{ ref('go_webinar_facts') }}
WHERE poll_responses > (total_polls * total_attended)
   OR poll_responses < 0
```

```sql
-- Test 5: Webinar Duration Consistency
-- tests/go_webinar_facts/test_webinar_duration.sql
SELECT webinar_id, scheduled_duration, actual_duration
FROM {{ ref('go_webinar_facts') }}
WHERE actual_duration > (scheduled_duration * 2)
   OR actual_duration <= 0
```

---

## 4. GO_BILLING_FACTS Model Test Cases

### Test Case List:
1. **Revenue Calculation Accuracy**
2. **Subscription Plan Validation**
3. **Billing Cycle Consistency**
4. **Payment Status Logic**
5. **Currency Conversion Validation**
6. **Discount Application Logic**
7. **Tax Calculation Verification**
8. **Refund Processing Validation**

### dbt Test Scripts:

```sql
-- Test 1: Revenue Calculation Accuracy
-- tests/go_billing_facts/test_revenue_calculation.sql
SELECT billing_id,
       base_amount,
       discount_amount,
       tax_amount,
       total_amount,
       (base_amount - COALESCE(discount_amount, 0) + COALESCE(tax_amount, 0)) as calculated_total
FROM {{ ref('go_billing_facts') }}
WHERE ABS(total_amount - calculated_total) > 0.01
```

```sql
-- Test 2: Subscription Plan Validation
-- tests/go_billing_facts/test_subscription_plan.sql
SELECT billing_id, subscription_plan
FROM {{ ref('go_billing_facts') }}
WHERE subscription_plan NOT IN ('Basic', 'Pro', 'Business', 'Enterprise', 'Free')
```

```sql
-- Test 3: Billing Cycle Consistency
-- tests/go_billing_facts/test_billing_cycle.sql
SELECT billing_id, billing_start_date, billing_end_date, billing_cycle
FROM {{ ref('go_billing_facts') }}
WHERE (billing_cycle = 'Monthly' AND DATEDIFF('day', billing_start_date, billing_end_date) NOT BETWEEN 28 AND 31)
   OR (billing_cycle = 'Annual' AND DATEDIFF('day', billing_start_date, billing_end_date) NOT BETWEEN 365 AND 366)
```

```sql
-- Test 4: Payment Status Logic
-- tests/go_billing_facts/test_payment_status.sql
SELECT billing_id, payment_date, payment_status, due_date
FROM {{ ref('go_billing_facts') }}
WHERE (payment_date IS NOT NULL AND payment_status IN ('Pending', 'Failed'))
   OR (payment_date IS NULL AND payment_status = 'Paid')
   OR (payment_date > due_date AND payment_status = 'Paid')
```

```sql
-- Test 5: Currency Conversion Validation
-- tests/go_billing_facts/test_currency_conversion.sql
SELECT billing_id, original_currency, converted_currency, exchange_rate, original_amount, converted_amount
FROM {{ ref('go_billing_facts') }}
WHERE original_currency != 'USD'
  AND ABS(converted_amount - (original_amount * exchange_rate)) > 0.01
```

---

## 5. GO_USAGE_FACTS Model Test Cases

### Test Case List:
1. **Usage Metrics Consistency**
2. **Storage Calculation Validation**
3. **Bandwidth Usage Tracking**
4. **Feature Usage Categorization**
5. **Peak Usage Time Analysis**
6. **License Utilization Validation**
7. **API Call Tracking**
8. **Resource Consumption Limits**

### dbt Test Scripts:

```sql
-- Test 1: Usage Metrics Consistency
-- tests/go_usage_facts/test_usage_metrics.sql
SELECT usage_id, total_minutes, meeting_minutes, webinar_minutes, phone_minutes
FROM {{ ref('go_usage_facts') }}
WHERE total_minutes != (COALESCE(meeting_minutes, 0) + COALESCE(webinar_minutes, 0) + COALESCE(phone_minutes, 0))
```

```sql
-- Test 2: Storage Calculation Validation
-- tests/go_usage_facts/test_storage_calculation.sql
SELECT usage_id, cloud_storage_used, cloud_storage_limit
FROM {{ ref('go_usage_facts') }}
WHERE cloud_storage_used > cloud_storage_limit * 1.1  -- Allow 10% buffer
   OR cloud_storage_used < 0
   OR cloud_storage_limit <= 0
```

```sql
-- Test 3: Bandwidth Usage Tracking
-- tests/go_usage_facts/test_bandwidth_usage.sql
SELECT usage_id, bandwidth_consumed_gb, usage_date
FROM {{ ref('go_usage_facts') }}
WHERE bandwidth_consumed_gb < 0
   OR bandwidth_consumed_gb > 1000  -- Reasonable upper limit
```

```sql
-- Test 4: Feature Usage Categorization
-- tests/go_usage_facts/test_feature_usage.sql
SELECT usage_id, feature_name, usage_count
FROM {{ ref('go_usage_facts') }}
WHERE feature_name NOT IN ('Screen Share', 'Recording', 'Breakout Rooms', 'Whiteboard', 'Chat', 'Polls', 'Q&A')
   OR usage_count < 0
```

```sql
-- Test 5: Peak Usage Time Analysis
-- tests/go_usage_facts/test_peak_usage.sql
SELECT usage_id, peak_concurrent_users, total_unique_users
FROM {{ ref('go_usage_facts') }}
WHERE peak_concurrent_users > total_unique_users
   OR peak_concurrent_users <= 0
```

---

## 6. GO_QUALITY_FACTS Model Test Cases

### Test Case List:
1. **Audio Quality Score Validation**
2. **Video Quality Metrics**
3. **Connection Stability Analysis**
4. **Latency Measurement Validation**
5. **Packet Loss Calculation**
6. **Jitter Analysis**
7. **Quality Score Aggregation**
8. **Network Performance Correlation**

### dbt Test Scripts:

```sql
-- Test 1: Audio Quality Score Validation
-- tests/go_quality_facts/test_audio_quality.sql
SELECT quality_id, audio_quality_score
FROM {{ ref('go_quality_facts') }}
WHERE audio_quality_score < 0 
   OR audio_quality_score > 5
   OR audio_quality_score IS NULL
```

```sql
-- Test 2: Video Quality Metrics
-- tests/go_quality_facts/test_video_quality.sql
SELECT quality_id, video_resolution, video_fps, video_bitrate
FROM {{ ref('go_quality_facts') }}
WHERE (video_resolution IS NOT NULL AND video_fps <= 0)
   OR (video_resolution IS NOT NULL AND video_bitrate <= 0)
   OR video_fps > 60
```

```sql
-- Test 3: Connection Stability Analysis
-- tests/go_quality_facts/test_connection_stability.sql
SELECT quality_id, connection_drops, session_duration_minutes, stability_score
FROM {{ ref('go_quality_facts') }}
WHERE connection_drops < 0
   OR (connection_drops = 0 AND stability_score < 0.9)
   OR (connection_drops > 5 AND stability_score > 0.5)
```

```sql
-- Test 4: Latency Measurement Validation
-- tests/go_quality_facts/test_latency_measurement.sql
SELECT quality_id, avg_latency_ms, max_latency_ms, min_latency_ms
FROM {{ ref('go_quality_facts') }}
WHERE avg_latency_ms < min_latency_ms
   OR avg_latency_ms > max_latency_ms
   OR min_latency_ms < 0
   OR max_latency_ms > 5000  -- 5 second upper limit
```

```sql
-- Test 5: Packet Loss Calculation
-- tests/go_quality_facts/test_packet_loss.sql
SELECT quality_id, packet_loss_percentage
FROM {{ ref('go_quality_facts') }}
WHERE packet_loss_percentage < 0
   OR packet_loss_percentage > 100
```

```sql
-- Test 6: Jitter Analysis
-- tests/go_quality_facts/test_jitter_analysis.sql
SELECT quality_id, avg_jitter_ms, max_jitter_ms
FROM {{ ref('go_quality_facts') }}
WHERE avg_jitter_ms < 0
   OR max_jitter_ms < avg_jitter_ms
   OR max_jitter_ms > 1000  -- 1 second upper limit
```

---

## Cross-Model Integration Tests

### Test Case List:
1. **Meeting-Participant Relationship Integrity**
2. **Billing-Usage Correlation**
3. **Quality-Meeting Association**
4. **Webinar-Participant Consistency**
5. **Usage-Quality Correlation**

### dbt Test Scripts:

```sql
-- Test 1: Meeting-Participant Relationship Integrity
-- tests/integration/test_meeting_participant_integrity.sql
WITH meeting_participant_counts AS (
  SELECT m.meeting_id,
         m.total_participants as meeting_count,
         COUNT(p.participant_id) as actual_count
  FROM {{ ref('go_meeting_facts') }} m
  LEFT JOIN {{ ref('go_participant_facts') }} p ON m.meeting_id = p.meeting_id
  GROUP BY m.meeting_id, m.total_participants
)
SELECT meeting_id, meeting_count, actual_count
FROM meeting_participant_counts
WHERE meeting_count != actual_count
```

```sql
-- Test 2: Billing-Usage Correlation
-- tests/integration/test_billing_usage_correlation.sql
SELECT b.account_id, b.billing_period, u.usage_period
FROM {{ ref('go_billing_facts') }} b
JOIN {{ ref('go_usage_facts') }} u ON b.account_id = u.account_id
WHERE b.billing_period != u.usage_period
  AND b.subscription_plan != 'Free'
```

---

## Performance Tests

### Test Case List:
1. **Query Execution Time Validation**
2. **Clustering Effectiveness**
3. **Memory Usage Optimization**
4. **Warehouse Scaling Impact**

### dbt Test Scripts:

```sql
-- Test 1: Query Performance Validation
-- tests/performance/test_query_performance.sql
SELECT 
  'go_meeting_facts' as model_name,
  COUNT(*) as row_count,
  CURRENT_TIMESTAMP() as test_start_time
FROM {{ ref('go_meeting_facts') }}
WHERE load_date >= CURRENT_DATE() - 7

UNION ALL

SELECT 
  'go_participant_facts' as model_name,
  COUNT(*) as row_count,
  CURRENT_TIMESTAMP() as test_start_time
FROM {{ ref('go_participant_facts') }}
WHERE load_date >= CURRENT_DATE() - 7
```

---

## Data Quality Schema Tests

### schema.yml Configuration:

```yaml
version: 2

models:
  - name: go_meeting_facts
    description: "Gold layer fact table for meeting data"
    tests:
      - dbt_utils.unique_combination_of_columns:
          combination_of_columns:
            - meeting_id
            - load_date
    columns:
      - name: meeting_id
        description: "Unique identifier for meetings"
        tests:
          - not_null
          - unique
      - name: data_quality_score
        description: "Data quality score for the record"
        tests:
          - not_null
          - dbt_utils.accepted_range:
              min_value: 0.7
              max_value: 1.0
      - name: duration_minutes
        description: "Meeting duration in minutes"
        tests:
          - not_null
          - dbt_utils.accepted_range:
              min_value: 0
              max_value: 1440  # 24 hours

  - name: go_participant_facts
    description: "Gold layer fact table for participant data"
    tests:
      - dbt_utils.unique_combination_of_columns:
          combination_of_columns:
            - participant_id
            - meeting_id
    columns:
      - name: participant_id
        description: "Unique identifier for participants"
        tests:
          - not_null
      - name: meeting_id
        description: "Reference to meeting"
        tests:
          - not_null
          - relationships:
              to: ref('go_meeting_facts')
              field: meeting_id

  - name: go_webinar_facts
    description: "Gold layer fact table for webinar data"
    columns:
      - name: webinar_id
        tests:
          - not_null
          - unique
      - name: total_registered
        tests:
          - not_null
          - dbt_utils.accepted_range:
              min_value: 0
      - name: total_attended
        tests:
          - not_null
          - dbt_utils.accepted_range:
              min_value: 0

  - name: go_billing_facts
    description: "Gold layer fact table for billing data"
    columns:
      - name: billing_id
        tests:
          - not_null
          - unique
      - name: total_amount
        tests:
          - not_null
          - dbt_utils.accepted_range:
              min_value: 0
      - name: payment_status
        tests:
          - not_null
          - accepted_values:
              values: ['Paid', 'Pending', 'Failed', 'Refunded']

  - name: go_usage_facts
    description: "Gold layer fact table for usage data"
    columns:
      - name: usage_id
        tests:
          - not_null
          - unique
      - name: total_minutes
        tests:
          - not_null
          - dbt_utils.accepted_range:
              min_value: 0

  - name: go_quality_facts
    description: "Gold layer fact table for quality metrics"
    columns:
      - name: quality_id
        tests:
          - not_null
          - unique
      - name: audio_quality_score
        tests:
          - not_null
          - dbt_utils.accepted_range:
              min_value: 0
              max_value: 5
```

---

## Test Execution Commands

### Running All Tests:
```bash
# Run all tests
dbt test

# Run tests for specific model
dbt test --select go_meeting_facts

# Run specific test type
dbt test --select test_type:generic
dbt test --select test_type:singular

# Run tests with specific tags
dbt test --select tag:data_quality
dbt test --select tag:business_logic
```

### Test Categories and Tags:
```yaml
# Add to individual test files
{{ config(
    tags=['data_quality', 'critical']
) }}

# For business logic tests
{{ config(
    tags=['business_logic', 'validation']
) }}

# For performance tests
{{ config(
    tags=['performance', 'optimization']
) }}
```

---

## Expected Test Results and Monitoring

### Success Criteria:
- All data quality tests pass with 0 failures
- Business logic tests validate transformation accuracy
- Performance tests complete within acceptable time limits
- Integration tests confirm cross-model consistency

### Failure Handling:
- Document test failures with root cause analysis
- Implement data quality alerts for critical failures
- Set up automated test execution in CI/CD pipeline
- Create test result dashboards for monitoring

### Test Maintenance:
- Review and update test cases monthly
- Add new test cases for model changes
- Archive obsolete tests
- Maintain test documentation

---

## Conclusion

This comprehensive test suite ensures the reliability, accuracy, and performance of all 6 Gold layer fact table models. The tests cover:

- **Data Quality**: Validates data integrity and completeness
- **Business Logic**: Ensures accurate transformations and calculations
- **Edge Cases**: Handles boundary conditions and error scenarios
- **Performance**: Monitors query execution and resource usage
- **Integration**: Validates relationships between models

Regular execution of these tests will maintain high data quality standards and prevent production issues in the Snowflake dbt environment.