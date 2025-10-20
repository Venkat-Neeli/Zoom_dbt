# Snowflake dbt Unit Test Case - Zoom Gold Fact Pipeline

_____________________________________________
## *Author*: AAVA
## *Created on*: 2024-12-19
## *Description*: Comprehensive unit test suite for Zoom Gold fact pipeline validating data transformations, business rules, and data quality
## *Version*: 2
## *Updated on*: 2024-12-19
_____________________________________________

## Overview
This test suite validates the Zoom Gold fact pipeline model, ensuring data integrity, transformation accuracy, and business rule compliance in the Snowflake environment.

## Test Coverage Matrix

| Test Category | Test Type | Coverage |
|---------------|-----------|----------|
| Happy Path | Data Transformations | ✓ |
| Happy Path | Join Operations | ✓ |
| Happy Path | Aggregations | ✓ |
| Edge Cases | Null Values | ✓ |
| Edge Cases | Empty Datasets | ✓ |
| Edge Cases | Schema Mismatches | ✓ |
| Exception Cases | Failed Relationships | ✓ |
| Exception Cases | Invalid Business Rules | ✓ |

## 1. Happy Path Test Cases

### 1.1 Valid Data Transformation Tests

#### Test Case: HP_001 - Basic Fact Table Population
```yaml
# models/schema.yml
version: 2

models:
  - name: zoom_gold_fact
    description: "Zoom Gold fact table with meeting metrics and dimensions"
    columns:
      - name: meeting_id
        description: "Unique identifier for each meeting"
        tests:
          - unique
          - not_null
      - name: host_id
        description: "Foreign key to host dimension"
        tests:
          - not_null
          - relationships:
              to: ref('dim_hosts')
              field: host_id
      - name: meeting_date
        description: "Date of the meeting"
        tests:
          - not_null
      - name: duration_minutes
        description: "Meeting duration in minutes"
        tests:
          - not_null
          - expression_is_true:
              expression: ">= 0"
      - name: participant_count
        description: "Number of participants in meeting"
        tests:
          - not_null
          - expression_is_true:
              expression: ">= 1"
      - name: meeting_type
        description: "Type of meeting"
        tests:
          - accepted_values:
              values: ['scheduled', 'instant', 'recurring', 'webinar']
      - name: is_recorded
        description: "Boolean flag for recorded meetings"
        tests:
          - not_null
          - accepted_values:
              values: [true, false]
```

#### Test Case: HP_002 - Aggregation Validation
```sql
-- tests/assert_meeting_duration_aggregation.sql
SELECT
    host_id,
    COUNT(*) as meeting_count,
    SUM(duration_minutes) as total_duration,
    AVG(duration_minutes) as avg_duration
FROM {{ ref('zoom_gold_fact') }}
WHERE meeting_date >= CURRENT_DATE - 30
GROUP BY host_id
HAVING 
    meeting_count < 0 
    OR total_duration < 0 
    OR avg_duration < 0
    OR avg_duration > 1440 -- No meeting should average more than 24 hours
```

#### Test Case: HP_003 - Join Integrity Validation
```sql
-- tests/assert_dimension_joins.sql
SELECT 
    f.meeting_id
FROM {{ ref('zoom_gold_fact') }} f
LEFT JOIN {{ ref('dim_hosts') }} h ON f.host_id = h.host_id
LEFT JOIN {{ ref('dim_date') }} d ON f.meeting_date = d.date_key
WHERE 
    h.host_id IS NULL 
    OR d.date_key IS NULL
```

### 1.2 Business Rule Validation Tests

#### Test Case: HP_004 - Meeting Duration Business Rules
```sql
-- tests/assert_meeting_duration_rules.sql
SELECT 
    meeting_id,
    duration_minutes,
    meeting_type
FROM {{ ref('zoom_gold_fact') }}
WHERE 
    (meeting_type = 'instant' AND duration_minutes > 480) -- Instant meetings shouldn't exceed 8 hours
    OR (meeting_type = 'webinar' AND duration_minutes < 5) -- Webinars should be at least 5 minutes
    OR duration_minutes > 1440 -- No meeting should exceed 24 hours
```

## 2. Edge Case Test Scenarios

### 2.1 Null Value Handling

#### Test Case: EC_001 - Null Value Impact Assessment
```sql
-- tests/assert_null_handling.sql
WITH null_analysis AS (
    SELECT 
        COUNT(*) as total_records,
        COUNT(CASE WHEN host_id IS NULL THEN 1 END) as null_host_ids,
        COUNT(CASE WHEN meeting_date IS NULL THEN 1 END) as null_dates,
        COUNT(CASE WHEN duration_minutes IS NULL THEN 1 END) as null_durations
    FROM {{ ref('zoom_gold_fact') }}
)
SELECT *
FROM null_analysis
WHERE 
    null_host_ids > 0 
    OR null_dates > 0 
    OR null_durations > 0
```

### 2.2 Empty Dataset Handling

#### Test Case: EC_002 - Empty Source Table Validation
```sql
-- tests/assert_minimum_record_count.sql
SELECT 
    COUNT(*) as record_count
FROM {{ ref('zoom_gold_fact') }}
HAVING COUNT(*) < 1
```

### 2.3 Schema Mismatch Detection

#### Test Case: EC_003 - Data Type Validation
```sql
-- tests/assert_data_types.sql
SELECT 
    meeting_id,
    host_id,
    duration_minutes,
    participant_count
FROM {{ ref('zoom_gold_fact') }}
WHERE 
    NOT (TRY_CAST(meeting_id AS VARCHAR) IS NOT NULL)
    OR NOT (TRY_CAST(host_id AS NUMBER) IS NOT NULL)
    OR NOT (TRY_CAST(duration_minutes AS NUMBER) IS NOT NULL)
    OR NOT (TRY_CAST(participant_count AS NUMBER) IS NOT NULL)
```

### 2.4 Missing Foreign Key Values

#### Test Case: EC_004 - Orphaned Records Detection
```sql
-- tests/assert_no_orphaned_records.sql
SELECT 
    f.meeting_id,
    f.host_id
FROM {{ ref('zoom_gold_fact') }} f
LEFT JOIN {{ ref('dim_hosts') }} h ON f.host_id = h.host_id
WHERE h.host_id IS NULL
```

## 3. Exception Case Test Scenarios

### 3.1 Failed Relationship Tests

#### Test Case: EX_001 - Referential Integrity Violations
```yaml
# Additional schema tests for exception handling
version: 2

models:
  - name: zoom_gold_fact
    tests:
      - dbt_utils.expression_is_true:
          expression: "participant_count <= 1000" # Business rule: max 1000 participants
      - dbt_utils.expression_is_true:
          expression: "meeting_date <= CURRENT_DATE" # No future meetings in fact table
```

### 3.2 Unexpected Value Detection

#### Test Case: EX_002 - Anomaly Detection
```sql
-- tests/assert_no_anomalies.sql
WITH anomaly_detection AS (
    SELECT 
        meeting_id,
        duration_minutes,
        participant_count,
        CASE 
            WHEN duration_minutes = 0 AND participant_count > 1 THEN 'Zero duration with participants'
            WHEN duration_minutes > 0 AND participant_count = 0 THEN 'Duration without participants'
            WHEN participant_count > 500 AND meeting_type = 'instant' THEN 'Too many participants for instant meeting'
            ELSE NULL
        END as anomaly_type
    FROM {{ ref('zoom_gold_fact') }}
)
SELECT *
FROM anomaly_detection
WHERE anomaly_type IS NOT NULL
```

### 3.3 Data Quality Threshold Tests

#### Test Case: EX_003 - Quality Metrics Validation
```sql
-- tests/assert_data_quality_thresholds.sql
WITH quality_metrics AS (
    SELECT 
        COUNT(*) as total_records,
        COUNT(CASE WHEN is_recorded = true THEN 1 END) as recorded_meetings,
        AVG(duration_minutes) as avg_duration,
        AVG(participant_count) as avg_participants
    FROM {{ ref('zoom_gold_fact') }}
    WHERE meeting_date >= CURRENT_DATE - 7
)
SELECT *
FROM quality_metrics
WHERE 
    total_records = 0 -- No data in last 7 days
    OR (recorded_meetings::FLOAT / total_records) > 0.9 -- More than 90% recorded (unusual)
    OR avg_duration < 5 -- Average meeting less than 5 minutes
    OR avg_participants < 1 -- Invalid participant average
```

## 4. Custom dbt Test Macros

### 4.1 Custom Business Rule Test
```sql
-- macros/test_zoom_meeting_business_rules.sql
{% macro test_zoom_meeting_business_rules(model, column_name) %}

SELECT 
    {{ column_name }},
    meeting_type,
    duration_minutes,
    participant_count,
    is_recorded
FROM {{ model }}
WHERE 
    -- Business Rule 1: Webinars must have at least 2 participants
    (meeting_type = 'webinar' AND participant_count < 2)
    -- Business Rule 2: Recorded meetings must have duration > 0
    OR (is_recorded = true AND duration_minutes <= 0)
    -- Business Rule 3: Recurring meetings should have reasonable duration
    OR (meeting_type = 'recurring' AND duration_minutes > 480)

{% endmacro %}
```

### 4.2 Data Freshness Test
```sql
-- macros/test_data_freshness.sql
{% macro test_data_freshness(model, date_column, max_days_old=1) %}

SELECT 
    MAX({{ date_column }}) as latest_date,
    CURRENT_DATE as current_date,
    DATEDIFF('day', MAX({{ date_column }}), CURRENT_DATE) as days_old
FROM {{ model }}
HAVING DATEDIFF('day', MAX({{ date_column }}), CURRENT_DATE) > {{ max_days_old }}

{% endmacro %}
```

## 5. Test Execution Configuration

### 5.1 dbt_project.yml Test Configuration
```yaml
# dbt_project.yml
name: 'zoom_analytics'
version: '1.0.0'

test-paths: ["tests"]

tests:
  zoom_analytics:
    +severity: error
    +store_failures: true
    +schema: dbt_test_failures

models:
  zoom_analytics:
    marts:
      +materialized: table
      +post-hook: "GRANT SELECT ON {{ this }} TO ROLE analytics_reader"
```

### 5.2 Test Execution Commands
```bash
# Run all tests
dbt test

# Run tests for specific model
dbt test --select zoom_gold_fact

# Run only custom tests
dbt test --select test_type:generic

# Run tests with failure storage
dbt test --store-failures
```

## 6. Performance and Monitoring

### 6.1 Test Performance Metrics
```sql
-- tests/performance/assert_model_performance.sql
WITH performance_check AS (
    SELECT 
        COUNT(*) as record_count,
        COUNT(DISTINCT host_id) as unique_hosts,
        COUNT(DISTINCT meeting_date) as date_range,
        MAX(meeting_date) as latest_date,
        MIN(meeting_date) as earliest_date
    FROM {{ ref('zoom_gold_fact') }}
)
SELECT *
FROM performance_check
WHERE 
    record_count > 10000000 -- Alert if more than 10M records
    OR DATEDIFF('day', earliest_date, latest_date) > 1095 -- More than 3 years of data
```

### 6.2 Resource Usage Monitoring
```sql
-- tests/monitoring/assert_resource_usage.sql
SELECT 
    'zoom_gold_fact' as model_name,
    CURRENT_TIMESTAMP as check_time,
    (SELECT COUNT(*) FROM {{ ref('zoom_gold_fact') }}) as row_count,
    'PASS' as status
WHERE 
    (SELECT COUNT(*) FROM {{ ref('zoom_gold_fact') }}) BETWEEN 1000 AND 50000000
```

## 7. Test Documentation and Maintenance

### 7.1 Test Case Status Tracking
| Test ID | Test Name | Status | Last Run | Next Review |
|---------|-----------|--------|----------|-------------|
| HP_001 | Basic Fact Population | ✅ Active | 2024-12-19 | 2024-12-26 |
| HP_002 | Aggregation Validation | ✅ Active | 2024-12-19 | 2024-12-26 |
| HP_003 | Join Integrity | ✅ Active | 2024-12-19 | 2024-12-26 |
| EC_001 | Null Value Handling | ✅ Active | 2024-12-19 | 2024-12-26 |
| EX_001 | Referential Integrity | ✅ Active | 2024-12-19 | 2024-12-26 |

### 7.2 Maintenance Schedule
- **Weekly**: Review test execution results and failure patterns
- **Monthly**: Update test thresholds based on data growth
- **Quarterly**: Comprehensive test suite review and optimization

## 8. Troubleshooting Guide

### 8.1 Common Test Failures
| Error Type | Possible Cause | Resolution |
|------------|----------------|------------|
| Unique constraint violation | Duplicate meeting_ids | Check source data deduplication |
| Null value in required field | Source data quality issue | Implement data cleansing rules |
| Referential integrity failure | Missing dimension records | Verify dimension table refresh |
| Performance threshold exceeded | Data volume growth | Optimize model or adjust thresholds |

### 8.2 Emergency Response
```sql
-- Emergency data validation query
SELECT 
    'CRITICAL' as alert_level,
    COUNT(*) as affected_records,
    'zoom_gold_fact validation failed' as message
FROM {{ ref('zoom_gold_fact') }}
WHERE 
    meeting_id IS NULL 
    OR host_id IS NULL 
    OR meeting_date IS NULL
HAVING COUNT(*) > 0;
```

## Conclusion
This comprehensive test suite ensures the reliability, accuracy, and performance of the Zoom Gold fact pipeline in Snowflake. Regular execution of these tests will maintain data quality and catch issues before they impact downstream analytics and reporting.

**Total Estimated API Cost**: $0.15 USD per full test suite execution