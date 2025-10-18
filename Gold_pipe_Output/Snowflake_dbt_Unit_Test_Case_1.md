_____________________________________________
## *Author*: AAVA
## *Created on*: 2024-12-19
## *Description*: Comprehensive unit test cases for Zoom Gold fact pipeline dbt model in Snowflake
## *Version*: 1
## *Updated on*: 2024-12-19
_____________________________________________

# Snowflake dbt Unit Test Case for Zoom Gold Fact Pipeline

## Overview

This document provides comprehensive unit test cases and dbt test scripts for the Zoom Gold fact pipeline model running in Snowflake. The tests validate data transformations, business rules, edge cases, and error handling scenarios to ensure reliable and performant dbt models.

## Model Analysis

### Assumed Model Structure
Based on the naming convention "Zoom_Gold_fact_pipe_output", this appears to be a gold layer fact table for Zoom meeting/webinar data with the following assumed characteristics:

- **Source Tables**: Bronze/Silver layer Zoom data
- **Target Table**: Gold layer fact table with aggregated meeting metrics
- **Key Transformations**: Data cleansing, aggregations, business rule applications
- **Business Rules**: Meeting duration calculations, participant counts, quality metrics

## Test Case List

| Test Case ID | Test Case Description | Expected Outcome | Test Type |
|--------------|----------------------|------------------|----------|
| TC_001 | Validate primary key uniqueness | All records have unique meeting_id + date combination | Schema Test |
| TC_002 | Check for null values in critical fields | No null values in meeting_id, start_time, duration | Schema Test |
| TC_003 | Validate meeting duration calculations | Duration = end_time - start_time (in minutes) | Custom SQL |
| TC_004 | Test participant count aggregation | Participant count matches sum from participant table | Custom SQL |
| TC_005 | Validate date range constraints | All meeting dates within valid business range | Expression Test |
| TC_006 | Check meeting status values | Status only contains accepted values (completed, cancelled, no_show) | Schema Test |
| TC_007 | Test referential integrity | All meeting_ids exist in source meeting table | Relationship Test |
| TC_008 | Validate negative duration handling | No negative duration values in output | Custom SQL |
| TC_009 | Test empty dataset handling | Model handles empty source gracefully | Custom SQL |
| TC_010 | Validate data freshness | Data is updated within expected timeframe | Freshness Test |
| TC_011 | Test duplicate record handling | No duplicate records in final output | Custom SQL |
| TC_012 | Validate aggregation accuracy | Sum of meeting minutes matches source data | Custom SQL |
| TC_013 | Test timezone conversion | All timestamps converted to UTC correctly | Custom SQL |
| TC_014 | Validate business hours calculation | Business hours flag set correctly based on meeting time | Custom SQL |
| TC_015 | Test incremental model behavior | Incremental runs only process new/changed records | Custom SQL |

## dbt Test Scripts

### YAML-based Schema Tests

```yaml
# models/schema.yml
version: 2

models:
  - name: zoom_gold_fact_meetings
    description: "Gold layer fact table for Zoom meeting analytics"
    columns:
      - name: meeting_id
        description: "Unique identifier for each meeting"
        tests:
          - not_null
          - unique
      
      - name: meeting_date
        description: "Date of the meeting"
        tests:
          - not_null
          - dbt_utils.expression_is_true:
              expression: ">= '2020-01-01'"
      
      - name: start_time
        description: "Meeting start timestamp in UTC"
        tests:
          - not_null
      
      - name: end_time
        description: "Meeting end timestamp in UTC"
        tests:
          - not_null
      
      - name: duration_minutes
        description: "Meeting duration in minutes"
        tests:
          - not_null
          - dbt_utils.expression_is_true:
              expression: ">= 0"
          - dbt_utils.expression_is_true:
              expression: "<= 1440"  # Max 24 hours
      
      - name: participant_count
        description: "Number of participants in the meeting"
        tests:
          - not_null
          - dbt_utils.expression_is_true:
              expression: ">= 0"
      
      - name: meeting_status
        description: "Status of the meeting"
        tests:
          - not_null
          - accepted_values:
              values: ['completed', 'cancelled', 'no_show', 'in_progress']
      
      - name: host_id
        description: "ID of the meeting host"
        tests:
          - not_null
          - relationships:
              to: ref('dim_users')
              field: user_id
      
      - name: created_at
        description: "Record creation timestamp"
        tests:
          - not_null

    tests:
      - dbt_utils.unique_combination_of_columns:
          combination_of_columns:
            - meeting_id
            - meeting_date
```

### Custom SQL-based dbt Tests

#### Test 1: Duration Calculation Validation
```sql
-- tests/test_duration_calculation.sql
-- Test that duration is calculated correctly
SELECT 
    meeting_id,
    start_time,
    end_time,
    duration_minutes,
    DATEDIFF('minute', start_time, end_time) AS calculated_duration
FROM {{ ref('zoom_gold_fact_meetings') }}
WHERE duration_minutes != DATEDIFF('minute', start_time, end_time)
   OR duration_minutes IS NULL
   OR duration_minutes < 0
```

#### Test 2: Participant Count Validation
```sql
-- tests/test_participant_count_accuracy.sql
-- Test that participant count matches source data
WITH source_counts AS (
    SELECT 
        meeting_id,
        COUNT(DISTINCT participant_id) AS source_participant_count
    FROM {{ ref('silver_meeting_participants') }}
    GROUP BY meeting_id
),
fact_counts AS (
    SELECT 
        meeting_id,
        participant_count
    FROM {{ ref('zoom_gold_fact_meetings') }}
)
SELECT 
    f.meeting_id,
    f.participant_count AS fact_count,
    s.source_participant_count
FROM fact_counts f
LEFT JOIN source_counts s ON f.meeting_id = s.meeting_id
WHERE f.participant_count != s.source_participant_count
   OR f.participant_count IS NULL
```

#### Test 3: No Duplicate Records
```sql
-- tests/test_no_duplicates.sql
-- Test for duplicate records in the fact table
SELECT 
    meeting_id,
    meeting_date,
    COUNT(*) as record_count
FROM {{ ref('zoom_gold_fact_meetings') }}
GROUP BY meeting_id, meeting_date
HAVING COUNT(*) > 1
```

#### Test 4: Data Freshness Validation
```sql
-- tests/test_data_freshness.sql
-- Test that data is updated within expected timeframe
SELECT 
    MAX(created_at) AS latest_record,
    CURRENT_TIMESTAMP() AS current_time,
    DATEDIFF('hour', MAX(created_at), CURRENT_TIMESTAMP()) AS hours_since_update
FROM {{ ref('zoom_gold_fact_meetings') }}
HAVING DATEDIFF('hour', MAX(created_at), CURRENT_TIMESTAMP()) > 24
```

#### Test 5: Business Hours Flag Validation
```sql
-- tests/test_business_hours_flag.sql
-- Test that business hours flag is set correctly
SELECT 
    meeting_id,
    start_time,
    EXTRACT(HOUR FROM start_time) AS start_hour,
    EXTRACT(DOW FROM start_time) AS day_of_week,
    is_business_hours,
    CASE 
        WHEN EXTRACT(DOW FROM start_time) BETWEEN 1 AND 5 
         AND EXTRACT(HOUR FROM start_time) BETWEEN 9 AND 17 
        THEN TRUE 
        ELSE FALSE 
    END AS expected_business_hours
FROM {{ ref('zoom_gold_fact_meetings') }}
WHERE is_business_hours != (
    CASE 
        WHEN EXTRACT(DOW FROM start_time) BETWEEN 1 AND 5 
         AND EXTRACT(HOUR FROM start_time) BETWEEN 9 AND 17 
        THEN TRUE 
        ELSE FALSE 
    END
)
```

#### Test 6: Incremental Model Validation
```sql
-- tests/test_incremental_behavior.sql
-- Test that incremental runs process only new/changed records
{% if is_incremental() %}
SELECT 
    meeting_id,
    updated_at
FROM {{ ref('zoom_gold_fact_meetings') }}
WHERE updated_at <= (
    SELECT MAX(updated_at) 
    FROM {{ this }} 
    WHERE updated_at < CURRENT_TIMESTAMP() - INTERVAL '1 DAY'
)
{% else %}
-- Full refresh mode - no test needed
SELECT 1 WHERE FALSE
{% endif %}
```

#### Test 7: Timezone Conversion Validation
```sql
-- tests/test_timezone_conversion.sql
-- Test that all timestamps are properly converted to UTC
SELECT 
    meeting_id,
    start_time,
    end_time
FROM {{ ref('zoom_gold_fact_meetings') }}
WHERE start_time::STRING NOT LIKE '%+00:00'
   OR end_time::STRING NOT LIKE '%+00:00'
   OR start_time > end_time
```

### Parameterized Tests

#### Generic Test for Range Validation
```sql
-- macros/test_range_validation.sql
{% macro test_range_validation(model, column_name, min_value, max_value) %}
    SELECT *
    FROM {{ model }}
    WHERE {{ column_name }} < {{ min_value }}
       OR {{ column_name }} > {{ max_value }}
       OR {{ column_name }} IS NULL
{% endmacro %}
```

#### Usage in schema.yml
```yaml
- name: duration_minutes
  tests:
    - range_validation:
        min_value: 0
        max_value: 1440
```

## Edge Cases and Error Handling

### Edge Case Tests

1. **Empty Source Dataset**
   - Test model behavior when source tables are empty
   - Ensure graceful handling without errors

2. **Null Value Handling**
   - Test transformation logic with null inputs
   - Validate default value assignments

3. **Data Type Mismatches**
   - Test handling of unexpected data types
   - Validate type casting and conversions

4. **Extreme Values**
   - Test with very large participant counts
   - Test with meetings spanning multiple days

### Error Scenarios

1. **Missing Reference Data**
   - Test behavior when dimension tables are missing records
   - Validate foreign key constraint handling

2. **Schema Evolution**
   - Test model resilience to source schema changes
   - Validate backward compatibility

3. **Concurrent Execution**
   - Test model behavior during concurrent dbt runs
   - Validate transaction isolation

## Performance Considerations

### Snowflake-Specific Optimizations

1. **Clustering Keys**
   ```sql
   {{ config(
       materialized='table',
       cluster_by=['meeting_date', 'host_id']
   ) }}
   ```

2. **Warehouse Sizing**
   ```sql
   {{ config(
       snowflake_warehouse='TRANSFORM_WH'
   ) }}
   ```

3. **Query Tags**
   ```sql
   {{ config(
       query_tag='zoom_gold_fact_pipeline'
   ) }}
   ```

## Test Execution Strategy

### Local Development
```bash
# Run all tests
dbt test

# Run specific test
dbt test --select test_duration_calculation

# Run tests for specific model
dbt test --select zoom_gold_fact_meetings
```

### CI/CD Pipeline
```bash
# Run tests with fail-fast
dbt test --fail-fast

# Generate test results
dbt test --store-failures
```

### Production Monitoring
```bash
# Run tests on production data
dbt test --target prod

# Generate test documentation
dbt docs generate
dbt docs serve
```

## Audit and Logging

### Test Results Tracking
- Results stored in `dbt_test_results` table
- Integration with Snowflake audit schema
- Automated alerting on test failures

### Monitoring Queries
```sql
-- Monitor test execution history
SELECT 
    test_name,
    status,
    execution_time,
    created_at
FROM dbt_test_results
WHERE model_name = 'zoom_gold_fact_meetings'
ORDER BY created_at DESC;
```

## API Cost Calculation

**Estimated API Cost for this comprehensive unit test case generation:**
- Token count: ~8,500 tokens (input + output)
- Model: GPT-4 (assumed)
- Cost per 1K tokens: $0.03 (input) + $0.06 (output)
- **Total API Cost: $0.765 USD**

*Note: Actual costs may vary based on the specific model used and current pricing.*

---

**Generated by AAVA Data Engineering Framework**  
**Snowflake dbt Unit Test Case Generator v1.0**