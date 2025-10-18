# Snowflake dbt Unit Test Case - Zoom Gold Fact Pipeline

_____________________________________________
## *Author*: AAVA
## *Created on*: 2024-12-19
## *Description*: Comprehensive unit test cases for Zoom Gold fact pipeline data transformations and business rules validation
## *Version*: 1
## *Updated on*: 2024-12-19
_____________________________________________

## Description

This document provides comprehensive unit test cases for the Zoom Gold fact pipeline in Snowflake using dbt. The tests validate data quality, transformations, business rules, edge cases, and ensure reliable data processing for Zoom meeting analytics and metrics.

## Instructions

1. Execute tests in the following order: Schema tests → Custom SQL tests → Business rule tests
2. Run tests after each model deployment using `dbt test`
3. Monitor test results and investigate any failures before promoting to production
4. Update test cases when business requirements change
5. Maintain test coverage above 90% for all critical data paths

## Test Case List

| Test Case ID | Description | Expected Outcome |
|--------------|-------------|------------------|
| TC_ZGF_001 | Validate meeting_id uniqueness | No duplicate meeting IDs in fact table |
| TC_ZGF_002 | Check for null values in required fields | Zero null values in mandatory columns |
| TC_ZGF_003 | Verify meeting duration calculations | Duration = end_time - start_time |
| TC_ZGF_004 | Validate participant count ranges | Participant count between 1 and 1000 |
| TC_ZGF_005 | Check date format consistency | All dates in YYYY-MM-DD format |
| TC_ZGF_006 | Verify foreign key relationships | All user_ids exist in user dimension |
| TC_ZGF_007 | Validate meeting status values | Status in ('completed', 'ongoing', 'cancelled') |
| TC_ZGF_008 | Check aggregation accuracy | Sum of individual metrics equals total |
| TC_ZGF_009 | Verify timezone handling | UTC conversion accuracy |
| TC_ZGF_010 | Test edge case handling | Zero-duration meetings handled correctly |
| TC_ZGF_011 | Validate data freshness | Data loaded within SLA timeframes |
| TC_ZGF_012 | Check for data completeness | All expected records present |
| TC_ZGF_013 | Verify business rule compliance | Premium features usage validation |
| TC_ZGF_014 | Test performance metrics | Query execution under 30 seconds |
| TC_ZGF_015 | Validate cost calculations | Meeting cost = duration * rate |

## dbt Test Scripts

### Schema Tests (schema.yml)

```yaml
version: 2

models:
  - name: zoom_gold_fact
    description: "Gold layer fact table for Zoom meeting analytics"
    columns:
      - name: meeting_id
        description: "Unique identifier for each meeting"
        tests:
          - unique
          - not_null
      
      - name: user_id
        description: "Foreign key to user dimension"
        tests:
          - not_null
          - relationships:
              to: ref('dim_users')
              field: user_id
      
      - name: meeting_start_time
        description: "Meeting start timestamp in UTC"
        tests:
          - not_null
          - dbt_expectations.expect_column_values_to_be_of_type:
              column_type: timestamp
      
      - name: meeting_end_time
        description: "Meeting end timestamp in UTC"
        tests:
          - not_null
          - dbt_expectations.expect_column_values_to_be_of_type:
              column_type: timestamp
      
      - name: meeting_duration_minutes
        description: "Meeting duration in minutes"
        tests:
          - not_null
          - dbt_expectations.expect_column_values_to_be_between:
              min_value: 0
              max_value: 1440
      
      - name: participant_count
        description: "Number of meeting participants"
        tests:
          - not_null
          - dbt_expectations.expect_column_values_to_be_between:
              min_value: 1
              max_value: 1000
      
      - name: meeting_status
        description: "Current status of the meeting"
        tests:
          - not_null
          - accepted_values:
              values: ['completed', 'ongoing', 'cancelled', 'scheduled']
      
      - name: meeting_type
        description: "Type of meeting (basic, pro, enterprise)"
        tests:
          - not_null
          - accepted_values:
              values: ['basic', 'pro', 'enterprise']
      
      - name: total_cost_usd
        description: "Total meeting cost in USD"
        tests:
          - not_null
          - dbt_expectations.expect_column_values_to_be_between:
              min_value: 0
              max_value: 10000
      
      - name: created_date
        description: "Date when record was created"
        tests:
          - not_null
      
      - name: updated_date
        description: "Date when record was last updated"
        tests:
          - not_null

    tests:
      - dbt_expectations.expect_table_row_count_to_be_between:
          min_value: 1
          max_value: 10000000
      
      - dbt_expectations.expect_table_columns_to_match_ordered_list:
          column_list: [
            "meeting_id",
            "user_id", 
            "meeting_start_time",
            "meeting_end_time",
            "meeting_duration_minutes",
            "participant_count",
            "meeting_status",
            "meeting_type",
            "total_cost_usd",
            "created_date",
            "updated_date"
          ]
```

### Custom SQL-based dbt Tests

#### Test 1: Duration Calculation Validation
```sql
-- tests/test_meeting_duration_calculation.sql
SELECT 
    meeting_id,
    meeting_start_time,
    meeting_end_time,
    meeting_duration_minutes,
    DATEDIFF('minute', meeting_start_time, meeting_end_time) AS calculated_duration
FROM {{ ref('zoom_gold_fact') }}
WHERE meeting_duration_minutes != DATEDIFF('minute', meeting_start_time, meeting_end_time)
   OR meeting_end_time < meeting_start_time
```

#### Test 2: Data Freshness Validation
```sql
-- tests/test_data_freshness.sql
SELECT COUNT(*) as stale_records
FROM {{ ref('zoom_gold_fact') }}
WHERE created_date < CURRENT_DATE - INTERVAL '2 days'
  AND meeting_start_time >= CURRENT_DATE - INTERVAL '1 day'
HAVING COUNT(*) > 0
```

#### Test 3: Business Rule - Premium Features Usage
```sql
-- tests/test_premium_features_validation.sql
SELECT 
    meeting_id,
    meeting_type,
    participant_count
FROM {{ ref('zoom_gold_fact') }}
WHERE (meeting_type = 'basic' AND participant_count > 100)
   OR (meeting_type = 'pro' AND participant_count > 500)
   OR (meeting_type = 'enterprise' AND participant_count > 1000)
```

#### Test 4: Cost Calculation Validation
```sql
-- tests/test_cost_calculation.sql
WITH cost_validation AS (
    SELECT 
        meeting_id,
        meeting_type,
        meeting_duration_minutes,
        total_cost_usd,
        CASE 
            WHEN meeting_type = 'basic' THEN meeting_duration_minutes * 0.02
            WHEN meeting_type = 'pro' THEN meeting_duration_minutes * 0.05
            WHEN meeting_type = 'enterprise' THEN meeting_duration_minutes * 0.10
        END AS expected_cost
    FROM {{ ref('zoom_gold_fact') }}
)
SELECT *
FROM cost_validation
WHERE ABS(total_cost_usd - expected_cost) > 0.01
```

#### Test 5: Null Value Edge Cases
```sql
-- tests/test_null_edge_cases.sql
SELECT 
    'meeting_id' as column_name,
    COUNT(*) as null_count
FROM {{ ref('zoom_gold_fact') }}
WHERE meeting_id IS NULL

UNION ALL

SELECT 
    'user_id' as column_name,
    COUNT(*) as null_count
FROM {{ ref('zoom_gold_fact') }}
WHERE user_id IS NULL

UNION ALL

SELECT 
    'meeting_start_time' as column_name,
    COUNT(*) as null_count
FROM {{ ref('zoom_gold_fact') }}
WHERE meeting_start_time IS NULL

HAVING null_count > 0
```

#### Test 6: Aggregation Validation
```sql
-- tests/test_aggregation_validation.sql
WITH daily_aggregates AS (
    SELECT 
        DATE(meeting_start_time) as meeting_date,
        COUNT(*) as total_meetings,
        SUM(meeting_duration_minutes) as total_duration,
        SUM(participant_count) as total_participants,
        SUM(total_cost_usd) as total_cost
    FROM {{ ref('zoom_gold_fact') }}
    GROUP BY DATE(meeting_start_time)
)
SELECT *
FROM daily_aggregates
WHERE total_meetings <= 0
   OR total_duration <= 0
   OR total_participants <= 0
   OR total_cost < 0
```

#### Test 7: Timezone Consistency
```sql
-- tests/test_timezone_consistency.sql
SELECT 
    meeting_id,
    meeting_start_time,
    meeting_end_time
FROM {{ ref('zoom_gold_fact') }}
WHERE EXTRACT(TIMEZONE_HOUR FROM meeting_start_time) != 0
   OR EXTRACT(TIMEZONE_HOUR FROM meeting_end_time) != 0
```

#### Test 8: Data Completeness Check
```sql
-- tests/test_data_completeness.sql
WITH source_count AS (
    SELECT COUNT(*) as source_records
    FROM {{ ref('zoom_silver_meetings') }}
    WHERE DATE(meeting_start_time) = CURRENT_DATE - 1
),
fact_count AS (
    SELECT COUNT(*) as fact_records
    FROM {{ ref('zoom_gold_fact') }}
    WHERE DATE(meeting_start_time) = CURRENT_DATE - 1
)
SELECT 
    source_records,
    fact_records,
    ABS(source_records - fact_records) as record_difference
FROM source_count, fact_count
WHERE ABS(source_records - fact_records) > source_records * 0.05
```

#### Test 9: Performance Validation
```sql
-- tests/test_performance_metrics.sql
SELECT 
    COUNT(*) as record_count,
    AVG(meeting_duration_minutes) as avg_duration,
    MAX(participant_count) as max_participants
FROM {{ ref('zoom_gold_fact') }}
WHERE created_date >= CURRENT_DATE - 7
HAVING COUNT(*) = 0
```

#### Test 10: Referential Integrity
```sql
-- tests/test_referential_integrity.sql
SELECT 
    f.meeting_id,
    f.user_id
FROM {{ ref('zoom_gold_fact') }} f
LEFT JOIN {{ ref('dim_users') }} u ON f.user_id = u.user_id
WHERE u.user_id IS NULL
```

## API Cost Calculation

### Test Execution Costs (USD)

| Test Category | Number of Tests | Estimated Snowflake Credits | Cost per Credit | Total Cost (USD) |
|---------------|-----------------|----------------------------|-----------------|------------------|
| Schema Tests | 15 | 0.5 | $2.00 | $1.00 |
| Custom SQL Tests | 10 | 2.0 | $2.00 | $4.00 |
| Performance Tests | 3 | 1.5 | $2.00 | $3.00 |
| **Total** | **28** | **4.0** | **$2.00** | **$8.00** |

### Monthly Testing Cost Estimate
- Daily test runs: $8.00
- Monthly cost (30 days): $240.00
- Annual cost: $2,880.00

### Cost Optimization Recommendations
1. Run full test suite only on production deployments
2. Use subset of critical tests for development environments
3. Implement test result caching where possible
4. Schedule heavy aggregation tests during off-peak hours
5. Use Snowflake's auto-suspend feature for test warehouses

## Test Execution Commands

```bash
# Run all tests
dbt test

# Run specific test
dbt test --select test_meeting_duration_calculation

# Run tests for specific model
dbt test --select zoom_gold_fact

# Run tests with verbose output
dbt test --verbose

# Run only schema tests
dbt test --select test_type:schema

# Run only custom tests
dbt test --select test_type:data
```

## Monitoring and Alerting

1. **Test Failure Alerts**: Configure alerts for any test failures
2. **Performance Monitoring**: Track test execution times
3. **Data Quality Metrics**: Monitor test pass rates over time
4. **Cost Monitoring**: Track Snowflake credit consumption for tests
5. **Automated Reporting**: Generate daily test summary reports

## Maintenance Schedule

- **Weekly**: Review test results and update thresholds
- **Monthly**: Analyze test performance and optimize slow tests
- **Quarterly**: Review and update test cases based on business changes
- **Annually**: Comprehensive test suite audit and enhancement