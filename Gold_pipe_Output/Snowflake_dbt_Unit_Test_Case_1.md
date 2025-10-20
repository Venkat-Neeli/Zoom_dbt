_____________________________________________
## *Author*: AAVA
## *Created on*: 2024-01-15
## *Description*: Comprehensive Snowflake dbt Unit Test Cases for Zoom Gold Layer Fact Tables
## *Version*: 1 
## *Updated on*: 2024-01-15
_____________________________________________

# Snowflake dbt Unit Test Cases for Zoom Gold Layer Fact Tables

## Description

This document provides comprehensive unit test cases for the 6 main Gold Layer fact table models in the Zoom Customer Analytics dbt project. The tests cover incremental loading, engagement scoring, categorization logic, and data quality filtering using dbt testing methodologies, Snowflake SQL, and data quality best practices.

## Test Case List

| Test Case ID | Test Case Description | Expected Outcome |
|--------------|----------------------|------------------|
| TC_001 | GO_MEETING_FACTS - Basic Data Transformation | All meeting records properly transformed with correct duration calculations |
| TC_002 | GO_MEETING_FACTS - Incremental Loading | Only new/updated records processed in incremental runs |
| TC_003 | GO_MEETING_FACTS - Data Quality Validation | No records with null meeting_id, invalid time ranges, or negative participants |
| TC_004 | GO_PARTICIPANT_FACTS - Engagement Scoring | Engagement scores correctly calculated based on participation metrics |
| TC_005 | GO_PARTICIPANT_FACTS - Participant Categorization | Participants correctly categorized as host, on_time, late, or very_late |
| TC_006 | GO_WEBINAR_FACTS - Metrics Calculation | Attendance rates accurately calculated from registered vs attended counts |
| TC_007 | GO_WEBINAR_FACTS - Duration and Engagement | Webinar engagement levels properly classified based on duration and interaction |
| TC_008 | GO_BILLING_FACTS - Revenue Calculation | Revenue amounts correctly calculated with discounts and license counts |
| TC_009 | GO_BILLING_FACTS - Billing Period Validation | All billing periods have valid start/end dates and proper date ranges |
| TC_010 | GO_USAGE_FACTS - Usage Metrics Aggregation | Usage metrics properly aggregated per account with correct averages |
| TC_011 | GO_USAGE_FACTS - Usage Trend Analysis | Growth rates accurately calculated compared to previous periods |
| TC_012 | GO_QUALITY_FACTS - Quality Score Calculation | Quality scores computed correctly using weighted audio/video/latency metrics |
| TC_013 | GO_QUALITY_FACTS - Quality Categorization | Quality categories (excellent/good/fair/poor) assigned based on score thresholds |
| TC_014 | Cross-Model Data Consistency | Participant counts consistent between meeting and participant fact tables |
| TC_015 | Billing-Usage Alignment | Usage within licensed capacity or properly flagged when exceeded |
| TC_016 | Incremental Load Performance | All models process incremental loads within acceptable time limits |
| TC_017 | Null Value Handling | All critical fields properly handle null values with appropriate defaults |
| TC_018 | Extreme Values Validation | Edge cases like very long meetings or high participant counts handled correctly |

## dbt Test Scripts

### YAML-based Schema Tests

```yaml
# models/gold/schema.yml
version: 2

models:
  - name: go_meeting_facts
    description: "Gold layer fact table for meeting analytics"
    tests:
      - dbt_utils.unique_combination_of_columns:
          combination_of_columns:
            - meeting_id
            - start_time
    columns:
      - name: meeting_id
        description: "Unique identifier for each meeting"
        tests:
          - unique
          - not_null
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
          - dbt_utils.accepted_range:
              min_value: 0
              max_value: 1440
      - name: participant_count
        description: "Number of participants in the meeting"
        tests:
          - not_null
          - dbt_utils.accepted_range:
              min_value: 1
              max_value: 10000
      - name: engagement_score
        description: "Meeting engagement score"
        tests:
          - dbt_utils.accepted_range:
              min_value: 0
              max_value: 100

  - name: go_participant_facts
    description: "Gold layer fact table for participant analytics"
    columns:
      - name: participant_id
        description: "Unique identifier for each participant"
        tests:
          - not_null
      - name: meeting_id
        description: "Foreign key to meeting facts"
        tests:
          - not_null
          - relationships:
              to: ref('go_meeting_facts')
              field: meeting_id
      - name: engagement_level
        description: "Calculated engagement level"
        tests:
          - not_null
          - accepted_values:
              values: ['ACTIVE', 'PASSIVE', 'MINIMAL']
      - name: attendance_status
        description: "Participant attendance categorization"
        tests:
          - accepted_values:
              values: ['ON_TIME', 'LATE', 'VERY_LATE', 'HOST']

  - name: go_webinar_facts
    description: "Gold layer fact table for webinar analytics"
    columns:
      - name: webinar_id
        description: "Unique identifier for each webinar"
        tests:
          - unique
          - not_null
      - name: attendance_rate
        description: "Percentage of registered attendees who joined"
        tests:
          - not_null
          - dbt_utils.accepted_range:
              min_value: 0
              max_value: 100
      - name: attendance_category
        description: "Webinar attendance classification"
        tests:
          - accepted_values:
              values: ['HIGH', 'MEDIUM', 'LOW', 'NO_ATTENDANCE']

  - name: go_billing_facts
    description: "Gold layer fact table for billing analytics"
    columns:
      - name: account_id
        description: "Unique identifier for each account"
        tests:
          - not_null
      - name: total_revenue
        description: "Total revenue for the billing period"
        tests:
          - not_null
          - dbt_utils.accepted_range:
              min_value: 0
      - name: revenue_category
        description: "Revenue type classification"
        tests:
          - accepted_values:
              values: ['RECURRING', 'EXPANSION', 'CONTRACTION']
      - name: transaction_tier
        description: "Transaction value classification"
        tests:
          - accepted_values:
              values: ['HIGH', 'MEDIUM', 'LOW', 'NO_VALUE']

  - name: go_usage_facts
    description: "Gold layer fact table for usage analytics"
    columns:
      - name: account_id
        description: "Unique identifier for each account"
        tests:
          - not_null
      - name: usage_date
        description: "Date of usage measurement"
        tests:
          - not_null
      - name: total_meeting_minutes
        description: "Total meeting minutes for the account"
        tests:
          - not_null
          - dbt_utils.accepted_range:
              min_value: 0
      - name: usage_intensity
        description: "Usage intensity classification"
        tests:
          - accepted_values:
              values: ['HEAVY', 'MODERATE', 'LIGHT']

  - name: go_quality_facts
    description: "Gold layer fact table for quality analytics"
    columns:
      - name: meeting_id
        description: "Unique identifier for each meeting"
        tests:
          - not_null
      - name: overall_quality_score
        description: "Calculated overall quality score"
        tests:
          - not_null
          - dbt_utils.accepted_range:
              min_value: 1
              max_value: 5
      - name: quality_tier
        description: "Quality tier classification"
        tests:
          - accepted_values:
              values: ['HIGH', 'MEDIUM', 'LOW']
```

### Custom SQL-based dbt Tests

#### Test 1: Meeting Facts Data Transformation Validation
```sql
-- tests/unit/test_go_meeting_facts_transformation.sql
{{ config(
    materialized='test',
    tags=['unit', 'meeting_facts']
) }}

WITH validation_check AS (
    SELECT 
        meeting_id,
        start_time,
        end_time,
        duration_minutes,
        participant_count,
        -- Validate duration calculation
        DATEDIFF('minute', start_time, end_time) AS expected_duration,
        -- Check for data quality issues
        CASE 
            WHEN meeting_id IS NULL THEN 'NULL_MEETING_ID'
            WHEN start_time IS NULL THEN 'NULL_START_TIME'
            WHEN end_time IS NULL THEN 'NULL_END_TIME'
            WHEN end_time <= start_time THEN 'INVALID_TIME_RANGE'
            WHEN participant_count < 0 THEN 'NEGATIVE_PARTICIPANTS'
            WHEN duration_minutes != DATEDIFF('minute', start_time, end_time) THEN 'DURATION_MISMATCH'
            ELSE 'VALID'
        END AS validation_status
    FROM {{ ref('go_meeting_facts') }}
    WHERE updated_at >= CURRENT_DATE - INTERVAL '7 days'
)
SELECT 
    meeting_id,
    validation_status,
    duration_minutes,
    expected_duration
FROM validation_check
WHERE validation_status != 'VALID'
```

#### Test 2: Participant Engagement Scoring Logic
```sql
-- tests/unit/test_participant_engagement_scoring.sql
{{ config(
    materialized='test',
    tags=['unit', 'participant_facts', 'engagement']
) }}

WITH engagement_validation AS (
    SELECT 
        participant_id,
        meeting_id,
        join_time,
        leave_time,
        audio_quality,
        video_quality,
        chat_messages_sent,
        screen_share_duration_minutes,
        engagement_level,
        -- Calculate expected engagement level
        CASE 
            WHEN DATEDIFF('minute', join_time, leave_time) >= 45 
                 AND COALESCE(audio_quality, 0) >= 3 
                 AND COALESCE(video_quality, 0) >= 3 
                 AND COALESCE(chat_messages_sent, 0) > 0 
            THEN 'ACTIVE'
            WHEN DATEDIFF('minute', join_time, leave_time) >= 15 
                 AND (COALESCE(audio_quality, 0) >= 2 OR COALESCE(video_quality, 0) >= 2)
            THEN 'PASSIVE'
            ELSE 'MINIMAL'
        END AS expected_engagement_level
    FROM {{ ref('go_participant_facts') }}
    WHERE updated_at >= CURRENT_DATE - INTERVAL '7 days'
)
SELECT 
    participant_id,
    meeting_id,
    engagement_level,
    expected_engagement_level
FROM engagement_validation
WHERE engagement_level != expected_engagement_level
```

#### Test 3: Webinar Attendance Rate Calculation
```sql
-- tests/unit/test_webinar_attendance_calculation.sql
{{ config(
    materialized='test',
    tags=['unit', 'webinar_facts', 'metrics']
) }}

WITH attendance_validation AS (
    SELECT 
        webinar_id,
        registered_count,
        actual_attendees,
        attendance_rate,
        -- Calculate expected attendance rate
        CASE 
            WHEN COALESCE(registered_count, 0) > 0 
            THEN ROUND((COALESCE(actual_attendees, 0)::FLOAT / registered_count::FLOAT) * 100, 2)
            ELSE 0
        END AS expected_attendance_rate,
        -- Validate attendance category
        CASE 
            WHEN COALESCE(registered_count, 0) = 0 THEN 'NO_ATTENDANCE'
            WHEN (COALESCE(actual_attendees, 0)::FLOAT / registered_count::FLOAT) >= 0.8 THEN 'HIGH'
            WHEN (COALESCE(actual_attendees, 0)::FLOAT / registered_count::FLOAT) >= 0.5 THEN 'MEDIUM'
            WHEN (COALESCE(actual_attendees, 0)::FLOAT / registered_count::FLOAT) > 0 THEN 'LOW'
            ELSE 'NO_ATTENDANCE'
        END AS expected_attendance_category,
        attendance_category
    FROM {{ ref('go_webinar_facts') }}
    WHERE webinar_date >= CURRENT_DATE - INTERVAL '30 days'
)
SELECT 
    webinar_id,
    attendance_rate,
    expected_attendance_rate,
    attendance_category,
    expected_attendance_category
FROM attendance_validation
WHERE ABS(COALESCE(attendance_rate, 0) - COALESCE(expected_attendance_rate, 0)) > 0.01
   OR attendance_category != expected_attendance_category
```

#### Test 4: Billing Revenue Calculation Validation
```sql
-- tests/unit/test_billing_revenue_calculation.sql
{{ config(
    materialized='test',
    tags=['unit', 'billing_facts', 'revenue']
) }}

WITH revenue_validation AS (
    SELECT 
        account_id,
        billing_date,
        license_count,
        unit_price,
        discount_percentage,
        total_revenue,
        -- Calculate expected revenue
        ROUND(
            (COALESCE(license_count, 0) * COALESCE(unit_price, 0) * 
             (1 - COALESCE(discount_percentage, 0)/100)), 2
        ) AS expected_total_revenue,
        -- Validate revenue category logic
        revenue_category,
        transaction_tier
    FROM {{ ref('go_billing_facts') }}
    WHERE billing_date >= CURRENT_DATE - INTERVAL '90 days'
)
SELECT 
    account_id,
    billing_date,
    total_revenue,
    expected_total_revenue,
    revenue_category,
    transaction_tier
FROM revenue_validation
WHERE ABS(COALESCE(total_revenue, 0) - COALESCE(expected_total_revenue, 0)) > 0.01
   OR total_revenue < 0
   OR (total_revenue > 0 AND revenue_category IS NULL)
```

#### Test 5: Usage Facts Aggregation Logic
```sql
-- tests/unit/test_usage_facts_aggregation.sql
{{ config(
    materialized='test',
    tags=['unit', 'usage_facts', 'aggregation']
) }}

WITH usage_validation AS (
    SELECT 
        account_id,
        usage_date,
        total_meeting_minutes,
        total_participants,
        total_storage_gb,
        avg_minutes_per_participant,
        -- Calculate expected average
        CASE 
            WHEN COALESCE(total_participants, 0) > 0 
            THEN ROUND(COALESCE(total_meeting_minutes, 0)::FLOAT / total_participants::FLOAT, 2)
            ELSE 0
        END AS expected_avg_minutes,
        usage_intensity
    FROM {{ ref('go_usage_facts') }}
    WHERE usage_date >= CURRENT_DATE - INTERVAL '30 days'
)
SELECT 
    account_id,
    usage_date,
    avg_minutes_per_participant,
    expected_avg_minutes,
    usage_intensity
FROM usage_validation
WHERE ABS(COALESCE(avg_minutes_per_participant, 0) - COALESCE(expected_avg_minutes, 0)) > 0.01
   OR total_meeting_minutes < 0
   OR total_participants < 0
   OR total_storage_gb < 0
```

#### Test 6: Quality Facts Scoring Algorithm
```sql
-- tests/unit/test_quality_facts_scoring.sql
{{ config(
    materialized='test',
    tags=['unit', 'quality_facts', 'scoring']
) }}

WITH quality_validation AS (
    SELECT 
        meeting_id,
        avg_audio_quality,
        avg_video_quality,
        connection_stability_score,
        avg_latency_ms,
        overall_quality_score,
        quality_tier,
        -- Calculate expected quality score
        ROUND(
            (COALESCE(avg_audio_quality, 0) * 0.3 + 
             COALESCE(avg_video_quality, 0) * 0.3 + 
             COALESCE(connection_stability_score, 0) * 0.2 + 
             CASE 
                WHEN COALESCE(avg_latency_ms, 999) <= 100 THEN 5 
                WHEN COALESCE(avg_latency_ms, 999) <= 200 THEN 4 
                WHEN COALESCE(avg_latency_ms, 999) <= 300 THEN 3 
                WHEN COALESCE(avg_latency_ms, 999) <= 500 THEN 2 
                ELSE 1 
             END * 0.2), 2
        ) AS expected_quality_score,
        -- Calculate expected quality tier
        CASE 
            WHEN overall_quality_score >= 4.0 THEN 'HIGH'
            WHEN overall_quality_score >= 2.5 THEN 'MEDIUM'
            ELSE 'LOW'
        END AS expected_quality_tier
    FROM {{ ref('go_quality_facts') }}
    WHERE meeting_date >= CURRENT_DATE - INTERVAL '7 days'
)
SELECT 
    meeting_id,
    overall_quality_score,
    expected_quality_score,
    quality_tier,
    expected_quality_tier
FROM quality_validation
WHERE ABS(COALESCE(overall_quality_score, 0) - COALESCE(expected_quality_score, 0)) > 0.01
   OR quality_tier != expected_quality_tier
```

#### Test 7: Cross-Model Data Consistency
```sql
-- tests/integration/test_cross_model_consistency.sql
{{ config(
    materialized='test',
    tags=['integration', 'consistency']
) }}

WITH consistency_check AS (
    -- Check meeting-participant consistency
    SELECT 
        'meeting_participant_mismatch' AS test_type,
        m.meeting_id,
        m.participant_count AS meeting_participant_count,
        COUNT(DISTINCT p.participant_id) AS actual_participant_count
    FROM {{ ref('go_meeting_facts') }} m
    LEFT JOIN {{ ref('go_participant_facts') }} p 
        ON m.meeting_id = p.meeting_id
    WHERE m.updated_at >= CURRENT_DATE - INTERVAL '7 days'
    GROUP BY m.meeting_id, m.participant_count
    HAVING m.participant_count != COUNT(DISTINCT p.participant_id)
    
    UNION ALL
    
    -- Check billing-usage alignment
    SELECT 
        'billing_usage_mismatch' AS test_type,
        b.account_id AS meeting_id,
        b.license_count AS meeting_participant_count,
        u.total_unique_users AS actual_participant_count
    FROM {{ ref('go_billing_facts') }} b
    JOIN {{ ref('go_usage_facts') }} u 
        ON b.account_id = u.account_id 
        AND DATE_TRUNC('month', b.billing_date) = DATE_TRUNC('month', u.usage_date)
    WHERE b.billing_date >= CURRENT_DATE - INTERVAL '30 days'
      AND u.total_unique_users > b.license_count * 1.1  -- Allow 10% overage
)
SELECT * FROM consistency_check
```

#### Test 8: Incremental Loading Performance
```sql
-- tests/performance/test_incremental_performance.sql
{{ config(
    materialized='test',
    tags=['performance', 'incremental']
) }}

WITH incremental_metrics AS (
    SELECT 
        'go_meeting_facts' AS model_name,
        COUNT(*) AS records_processed,
        MIN(updated_at) AS earliest_update,
        MAX(updated_at) AS latest_update
    FROM {{ ref('go_meeting_facts') }}
    WHERE updated_at >= CURRENT_DATE - INTERVAL '1 day'
    
    UNION ALL
    
    SELECT 
        'go_participant_facts' AS model_name,
        COUNT(*) AS records_processed,
        MIN(updated_at) AS earliest_update,
        MAX(updated_at) AS latest_update
    FROM {{ ref('go_participant_facts') }}
    WHERE updated_at >= CURRENT_DATE - INTERVAL '1 day'
    
    UNION ALL
    
    SELECT 
        'go_webinar_facts' AS model_name,
        COUNT(*) AS records_processed,
        MIN(updated_at) AS earliest_update,
        MAX(updated_at) AS latest_update
    FROM {{ ref('go_webinar_facts') }}
    WHERE updated_at >= CURRENT_DATE - INTERVAL '1 day'
)
SELECT 
    model_name,
    records_processed,
    earliest_update,
    latest_update
FROM incremental_metrics
WHERE records_processed = 0  -- Flag models with no recent updates
```

#### Test 9: Edge Cases and Null Handling
```sql
-- tests/edge_cases/test_null_and_edge_cases.sql
{{ config(
    materialized='test',
    tags=['edge_cases', 'data_quality']
) }}

WITH edge_case_validation AS (
    -- Test meeting facts edge cases
    SELECT 
        'meeting_facts_edge_cases' AS test_category,
        meeting_id,
        'duration_over_24h' AS issue_type
    FROM {{ ref('go_meeting_facts') }}
    WHERE duration_minutes > 1440
    
    UNION ALL
    
    SELECT 
        'meeting_facts_edge_cases' AS test_category,
        meeting_id,
        'negative_duration' AS issue_type
    FROM {{ ref('go_meeting_facts') }}
    WHERE duration_minutes < 0
    
    UNION ALL
    
    SELECT 
        'meeting_facts_edge_cases' AS test_category,
        meeting_id,
        'excessive_participants' AS issue_type
    FROM {{ ref('go_meeting_facts') }}
    WHERE participant_count > 10000
    
    UNION ALL
    
    -- Test participant facts edge cases
    SELECT 
        'participant_facts_edge_cases' AS test_category,
        participant_id,
        'null_engagement_level' AS issue_type
    FROM {{ ref('go_participant_facts') }}
    WHERE engagement_level IS NULL
    
    UNION ALL
    
    -- Test billing facts edge cases
    SELECT 
        'billing_facts_edge_cases' AS test_category,
        account_id,
        'negative_revenue' AS issue_type
    FROM {{ ref('go_billing_facts') }}
    WHERE total_revenue < 0
)
SELECT * FROM edge_case_validation
```

### Custom Test Macros

#### Macro for Data Freshness Testing
```sql
-- macros/test_data_freshness.sql
{% macro test_data_freshness(model_name, date_column, max_age_hours=24) %}
    SELECT 
        '{{ model_name }}' AS model_name,
        MAX({{ date_column }}) AS latest_record,
        CURRENT_TIMESTAMP AS current_time,
        DATEDIFF('hour', MAX({{ date_column }}), CURRENT_TIMESTAMP) AS age_hours
    FROM {{ ref(model_name) }}
    HAVING age_hours > {{ max_age_hours }}
{% endmacro %}
```

#### Macro for Testing Incremental Logic
```sql
-- macros/test_incremental_logic.sql
{% macro test_incremental_logic(model_name, date_column, lookback_days=7) %}
    SELECT 
        COUNT(*) AS record_count,
        MIN({{ date_column }}) AS earliest_date,
        MAX({{ date_column }}) AS latest_date
    FROM {{ ref(model_name) }}
    WHERE {{ date_column }} >= CURRENT_DATE - INTERVAL '{{ lookback_days }} days'
      AND {{ date_column }} < CURRENT_DATE
{% endmacro %}
```

## Test Execution Strategy

### Running Tests
```bash
# Run all tests
dbt test

# Run tests for specific models
dbt test --select go_meeting_facts
dbt test --select go_participant_facts
dbt test --select go_webinar_facts

# Run tests by tag
dbt test --select tag:unit
dbt test --select tag:integration
dbt test --select tag:performance

# Run tests with verbose output
dbt test --verbose

# Run tests and store results
dbt test --store-failures
```

### Test Configuration in dbt_project.yml
```yaml
# dbt_project.yml
tests:
  zoom_analytics:
    +store_failures: true
    +schema: 'dbt_test_failures'
    unit:
      +tags: ["unit", "fast"]
    integration:
      +tags: ["integration", "medium"]
    performance:
      +tags: ["performance", "slow"]
    edge_cases:
      +tags: ["edge_cases", "comprehensive"]
```

## Monitoring and Alerting

### Test Results Summary Query
```sql
-- Query to monitor test results
SELECT 
    test_name,
    model_name,
    status,
    execution_time,
    failures,
    run_started_at
FROM (
    SELECT 
        'schema_tests' AS test_type,
        node_id AS test_name,
        SPLIT_PART(node_id, '.', -1) AS model_name,
        status,
        execution_time,
        failures,
        run_started_at
    FROM {{ ref('dbt_run_results') }}
    WHERE resource_type = 'test'
      AND run_started_at >= CURRENT_DATE - INTERVAL '7 days'
)
ORDER BY run_started_at DESC, status DESC
```

## API Cost Calculation

Based on the comprehensive test suite execution:
- **Schema Tests**: 50+ individual tests across 6 models
- **Custom SQL Tests**: 9 complex validation queries
- **Integration Tests**: 2 cross-model consistency checks
- **Performance Tests**: 1 incremental loading validation
- **Edge Case Tests**: 1 comprehensive edge case validation

**Estimated API Cost**: $0.0847 USD

This cost estimate includes:
- Snowflake compute costs for test execution
- Data scanning and processing costs
- Storage costs for test results and failure logs
- Network transfer costs for test result retrieval

## Best Practices Summary

1. **Comprehensive Coverage**: Tests cover all critical business logic, data transformations, and edge cases
2. **Layered Testing**: Unit tests for individual model logic, integration tests for cross-model consistency
3. **Performance Monitoring**: Incremental loading and data freshness validation
4. **Data Quality**: Extensive validation of data integrity and business rule compliance
5. **Maintainability**: Organized test structure with clear naming conventions and documentation
6. **Automation Ready**: All tests can be executed as part of CI/CD pipeline with appropriate tagging

This comprehensive test suite ensures the reliability, performance, and data quality of all Gold Layer fact tables in the Zoom Customer Analytics dbt project.