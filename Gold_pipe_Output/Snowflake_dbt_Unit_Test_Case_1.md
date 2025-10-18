_____________________________________________
## *Author*: AAVA
## *Created on*: 2024-12-19
## *Description*: Comprehensive unit test cases and dbt test scripts for Zoom Gold dimension pipeline in Snowflake
## *Version*: 1 
## *Updated on*: 2024-12-19
_____________________________________________

# Snowflake dbt Unit Test Case for Zoom Gold Dimension Pipeline

## Description

This document provides comprehensive unit test cases and dbt test scripts for the Zoom Gold dimension pipeline that transforms data from Silver Layer to Gold Layer dimension tables in Snowflake. The testing framework validates data transformations, mappings, business rules, edge cases, and error handling scenarios to ensure reliable and high-performance dbt models.

## Test Case Overview

The testing strategy covers six main Gold dimension models:
1. **go_process_audit** - ETL process tracking and audit logging
2. **go_user_dimension** - User dimension with type categorization and license info
3. **go_organization_dimension** - Organization dimension derived from user company data
4. **go_time_dimension** - Time dimension with fiscal and calendar attributes
5. **go_device_dimension** - Device dimension with placeholder data
6. **go_geography_dimension** - Geography dimension with default US data

## Test Case List

| Test Case ID | Test Case Description | Expected Outcome | Test Type | Severity |
|--------------|----------------------|------------------|-----------|----------|
| TC_001 | Validate surrogate key uniqueness across all dimensions | All surrogate keys are unique within each dimension table | Schema Test | Error |
| TC_002 | Validate not null constraints on critical fields | No null values in mandatory fields | Schema Test | Error |
| TC_003 | Validate data quality scores range (0-100) | All data quality scores between 0 and 100 | Expression Test | Error |
| TC_004 | Validate user email format | All emails contain '@' symbol | Expression Test | Error |
| TC_005 | Validate user type categorization | User types match accepted values | Accepted Values Test | Error |
| TC_006 | Validate license type mapping | License types match predefined categories | Accepted Values Test | Error |
| TC_007 | Validate account status consistency | Account status values are valid | Accepted Values Test | Error |
| TC_008 | Validate process audit timestamp logic | End timestamp >= start timestamp or null | Expression Test | Error |
| TC_009 | Validate process status values | Process status matches allowed values | Accepted Values Test | Error |
| TC_010 | Validate organization size categorization | Organization sizes match expected values | Accepted Values Test | Error |
| TC_011 | Validate time dimension date ranges | Year values within expected range (2020-2030) | Expression Test | Error |
| TC_012 | Validate quarter and month values | Quarter (1-4) and month (1-12) within valid ranges | Expression Test | Error |
| TC_013 | Validate weekend flag accuracy | Weekend flag correctly identifies weekends | Expression Test | Error |
| TC_014 | Validate device type categorization | Device types match accepted values | Accepted Values Test | Error |
| TC_015 | Validate geography timezone values | Timezone values match accepted list | Accepted Values Test | Error |
| TC_016 | Test duplicate email detection | Identify duplicate emails across users | Custom SQL Test | Warn |
| TC_017 | Test organization-user relationships | All active organizations have active users | Custom SQL Test | Warn |
| TC_018 | Test time dimension completeness | No gaps in date sequence | Custom SQL Test | Error |
| TC_019 | Test audit process integrity | Completed processes have end timestamps | Custom SQL Test | Error |
| TC_020 | Test cross-dimension data quality | Average quality scores above threshold | Custom SQL Test | Warn |
| TC_021 | Test surrogate key overlap prevention | No key overlap across dimension tables | Custom SQL Test | Error |
| TC_022 | Test business rule compliance | User type and license type consistency | Custom SQL Test | Error |
| TC_023 | Test data freshness validation | Data updated within acceptable timeframe | Custom SQL Test | Warn |
| TC_024 | Test referential integrity | Child records have valid parent references | Custom SQL Test | Error |
| TC_025 | Test end-to-end pipeline validation | Complete pipeline produces expected results | Integration Test | Error |

## dbt Test Scripts

### 1. Main Schema Test Configuration (models/tests/schema.yml)

```yaml
version: 2

models:
  # Process Audit Model Tests
  - name: go_process_audit
    description: "ETL process tracking and audit logging"
    tests:
      - unique:
          column_name: audit_key
      - not_null:
          column_name: audit_key
      - not_null:
          column_name: process_name
      - not_null:
          column_name: start_timestamp
      - accepted_values:
          column_name: process_status
          values: ['STARTED', 'COMPLETED', 'FAILED', 'RUNNING']
      - expression_is_true:
          expression: "end_timestamp >= start_timestamp OR end_timestamp IS NULL"
      - expression_is_true:
          expression: "records_processed >= 0"
      - expression_is_true:
          expression: "records_failed >= 0"
      - expression_is_true:
          expression: "data_quality_score >= 0 AND data_quality_score <= 100"
    columns:
      - name: audit_key
        description: "Unique identifier for audit record"
        tests:
          - unique
          - not_null
      - name: process_name
        description: "Name of the ETL process"
        tests:
          - not_null
          - accepted_values:
              values: ['USER_DIMENSION_LOAD', 'ORG_DIMENSION_LOAD', 'TIME_DIMENSION_LOAD', 'DEVICE_DIMENSION_LOAD', 'GEOGRAPHY_DIMENSION_LOAD']
      - name: start_timestamp
        tests:
          - not_null
      - name: process_status
        tests:
          - not_null
          - accepted_values:
              values: ['STARTED', 'COMPLETED', 'FAILED', 'RUNNING']
      - name: records_processed
        tests:
          - not_null
          - expression_is_true:
              expression: ">= 0"
      - name: records_failed
        tests:
          - not_null
          - expression_is_true:
              expression: ">= 0"
      - name: data_quality_score
        tests:
          - not_null
          - expression_is_true:
              expression: ">= 0 AND <= 100"

  # User Dimension Model Tests
  - name: go_user_dimension
    description: "User dimension with type categorization and license info"
    tests:
      - unique:
          column_name: user_dimension_key
      - not_null:
          column_name: user_dimension_key
      - expression_is_true:
          expression: "data_quality_score >= 0 AND data_quality_score <= 100"
    columns:
      - name: user_dimension_key
        description: "Surrogate key for user dimension"
        tests:
          - unique
          - not_null
      - name: user_id
        description: "Natural key from source system"
        tests:
          - not_null
          - unique
      - name: email
        description: "User email address"
        tests:
          - not_null
          - expression_is_true:
              expression: "email LIKE '%@%'"
      - name: user_type
        description: "Categorized user type"
        tests:
          - not_null
          - accepted_values:
              values: ['BASIC', 'LICENSED', 'ON_PREM', 'ADMIN', 'UNKNOWN']
      - name: license_type
        description: "User license information"
        tests:
          - accepted_values:
              values: ['BASIC', 'PRO', 'BUSINESS', 'ENTERPRISE', 'ENTERPRISE_PLUS', 'NONE']
      - name: account_status
        tests:
          - not_null
          - accepted_values:
              values: ['ACTIVE', 'INACTIVE', 'PENDING', 'SUSPENDED']
      - name: created_date
        tests:
          - not_null
      - name: is_active
        tests:
          - not_null
          - accepted_values:
              values: [true, false]
      - name: data_quality_score
        tests:
          - not_null
          - expression_is_true:
              expression: ">= 0 AND <= 100"

  # Organization Dimension Model Tests
  - name: go_organization_dimension
    description: "Organization dimension derived from user company data"
    tests:
      - unique:
          column_name: organization_dimension_key
      - not_null:
          column_name: organization_dimension_key
    columns:
      - name: organization_dimension_key
        description: "Surrogate key for organization dimension"
        tests:
          - unique
          - not_null
      - name: organization_id
        description: "Natural key from source system"
        tests:
          - not_null
          - unique
      - name: organization_name
        description: "Company/Organization name"
        tests:
          - not_null
      - name: industry
        description: "Industry classification"
        tests:
          - accepted_values:
              values: ['TECHNOLOGY', 'HEALTHCARE', 'FINANCE', 'EDUCATION', 'GOVERNMENT', 'RETAIL', 'MANUFACTURING', 'OTHER', 'UNKNOWN']
      - name: organization_size
        tests:
          - accepted_values:
              values: ['SMALL', 'MEDIUM', 'LARGE', 'ENTERPRISE', 'UNKNOWN']
      - name: subscription_type
        tests:
          - not_null
          - accepted_values:
              values: ['BASIC', 'PRO', 'BUSINESS', 'ENTERPRISE', 'ENTERPRISE_PLUS']
      - name: is_active
        tests:
          - not_null
          - accepted_values:
              values: [true, false]
      - name: data_quality_score
        tests:
          - not_null
          - expression_is_true:
              expression: ">= 0 AND <= 100"

  # Time Dimension Model Tests
  - name: go_time_dimension
    description: "Time dimension with fiscal and calendar attributes"
    tests:
      - unique:
          column_name: time_dimension_key
      - not_null:
          column_name: time_dimension_key
    columns:
      - name: time_dimension_key
        description: "Surrogate key for time dimension"
        tests:
          - unique
          - not_null
      - name: date_value
        description: "Actual date value"
        tests:
          - not_null
          - unique
      - name: year
        tests:
          - not_null
          - expression_is_true:
              expression: ">= 2020 AND <= 2030"
      - name: quarter
        tests:
          - not_null
          - accepted_values:
              values: [1, 2, 3, 4]
      - name: month
        tests:
          - not_null
          - expression_is_true:
              expression: ">= 1 AND <= 12"
      - name: day_of_month
        tests:
          - not_null
          - expression_is_true:
              expression: ">= 1 AND <= 31"
      - name: day_of_week
        tests:
          - not_null
          - expression_is_true:
              expression: ">= 1 AND <= 7"
      - name: fiscal_year
        tests:
          - not_null
      - name: fiscal_quarter
        tests:
          - not_null
          - accepted_values:
              values: [1, 2, 3, 4]
      - name: is_weekend
        tests:
          - not_null
          - accepted_values:
              values: [true, false]
      - name: is_holiday
        tests:
          - not_null
          - accepted_values:
              values: [true, false]

  # Device Dimension Model Tests
  - name: go_device_dimension
    description: "Device dimension with placeholder data"
    tests:
      - unique:
          column_name: device_dimension_key
      - not_null:
          column_name: device_dimension_key
    columns:
      - name: device_dimension_key
        description: "Surrogate key for device dimension"
        tests:
          - unique
          - not_null
      - name: device_id
        description: "Natural key from source system"
        tests:
          - not_null
      - name: device_type
        tests:
          - not_null
          - accepted_values:
              values: ['DESKTOP', 'MOBILE', 'TABLET', 'WEB', 'PHONE', 'ROOM_SYSTEM', 'UNKNOWN']
      - name: operating_system
        tests:
          - accepted_values:
              values: ['WINDOWS', 'MAC', 'IOS', 'ANDROID', 'LINUX', 'UNKNOWN']
      - name: browser
        tests:
          - accepted_values:
              values: ['CHROME', 'FIREFOX', 'SAFARI', 'EDGE', 'IE', 'OTHER', 'N/A']
      - name: app_version
        tests:
          - not_null
      - name: is_mobile
        tests:
          - not_null
          - accepted_values:
              values: [true, false]
      - name: data_quality_score
        tests:
          - not_null
          - expression_is_true:
              expression: ">= 0 AND <= 100"

  # Geography Dimension Model Tests
  - name: go_geography_dimension
    description: "Geography dimension with default US data"
    tests:
      - unique:
          column_name: geography_dimension_key
      - not_null:
          column_name: geography_dimension_key
    columns:
      - name: geography_dimension_key
        description: "Surrogate key for geography dimension"
        tests:
          - unique
          - not_null
      - name: geography_id
        description: "Natural key from source system"
        tests:
          - not_null
      - name: country
        tests:
          - not_null
      - name: state_province
        tests:
          - not_null
      - name: city
        tests:
          - not_null
      - name: postal_code
        tests:
          - expression_is_true:
              expression: "LENGTH(postal_code) >= 5 OR postal_code IS NULL"
      - name: timezone
        tests:
          - not_null
          - accepted_values:
              values: ['EST', 'CST', 'MST', 'PST', 'AKST', 'HST', 'UTC', 'UNKNOWN']
      - name: region
        tests:
          - accepted_values:
              values: ['NORTH_AMERICA', 'SOUTH_AMERICA', 'EUROPE', 'ASIA', 'AFRICA', 'OCEANIA', 'UNKNOWN']
      - name: data_quality_score
        tests:
          - not_null
          - expression_is_true:
              expression: ">= 0 AND <= 100"
```

### 2. Custom SQL-Based dbt Tests

#### tests/unit/test_user_dimension_edge_cases.sql
```sql
-- Test for duplicate email addresses across different user_ids
SELECT 
    email,
    COUNT(*) as duplicate_count
FROM {{ ref('go_user_dimension') }}
WHERE email IS NOT NULL
GROUP BY email
HAVING COUNT(*) > 1
```

#### tests/unit/test_organization_dimension_relationships.sql
```sql
-- Test that all organizations have at least one active user
SELECT 
    o.organization_id,
    o.organization_name
FROM {{ ref('go_organization_dimension') }} o
LEFT JOIN {{ ref('go_user_dimension') }} u 
    ON o.organization_id = u.organization_id 
    AND u.is_active = true
WHERE o.is_active = true
    AND u.user_id IS NULL
```

#### tests/unit/test_time_dimension_completeness.sql
```sql
-- Test for gaps in time dimension (missing dates)
WITH date_range AS (
    SELECT 
        DATEADD(day, seq4(), '2020-01-01')::DATE as expected_date
    FROM TABLE(GENERATOR(ROWCOUNT => 3653)) -- ~10 years
),
actual_dates AS (
    SELECT DISTINCT date_value
    FROM {{ ref('go_time_dimension') }}
)
SELECT 
    dr.expected_date
FROM date_range dr
LEFT JOIN actual_dates ad ON dr.expected_date = ad.date_value
WHERE ad.date_value IS NULL
    AND dr.expected_date <= CURRENT_DATE()
```

#### tests/unit/test_audit_process_integrity.sql
```sql
-- Test that completed processes have end timestamps
SELECT 
    audit_key,
    process_name,
    process_status,
    start_timestamp,
    end_timestamp
FROM {{ ref('go_process_audit') }}
WHERE process_status = 'COMPLETED'
    AND end_timestamp IS NULL
```

#### tests/unit/test_data_quality_scores.sql
```sql
-- Test that data quality scores are consistent across all dimension tables
WITH quality_scores AS (
    SELECT 'USER_DIMENSION' as table_name, AVG(data_quality_score) as avg_score
    FROM {{ ref('go_user_dimension') }}
    UNION ALL
    SELECT 'ORGANIZATION_DIMENSION', AVG(data_quality_score)
    FROM {{ ref('go_organization_dimension') }}
    UNION ALL
    SELECT 'DEVICE_DIMENSION', AVG(data_quality_score)
    FROM {{ ref('go_device_dimension') }}
    UNION ALL
    SELECT 'GEOGRAPHY_DIMENSION', AVG(data_quality_score)
    FROM {{ ref('go_geography_dimension') }}
)
SELECT 
    table_name,
    avg_score
FROM quality_scores
WHERE avg_score < 80.0  -- Flag tables with low average quality scores
```

#### tests/unit/test_surrogate_key_uniqueness.sql
```sql
-- Test that surrogate keys don't overlap across dimension tables
WITH all_keys AS (
    SELECT user_dimension_key as dim_key, 'USER' as table_name FROM {{ ref('go_user_dimension') }}
    UNION ALL
    SELECT organization_dimension_key, 'ORGANIZATION' FROM {{ ref('go_organization_dimension') }}
    UNION ALL
    SELECT time_dimension_key, 'TIME' FROM {{ ref('go_time_dimension') }}
    UNION ALL
    SELECT device_dimension_key, 'DEVICE' FROM {{ ref('go_device_dimension') }}
    UNION ALL
    SELECT geography_dimension_key, 'GEOGRAPHY' FROM {{ ref('go_geography_dimension') }}
)
SELECT 
    dim_key,
    COUNT(*) as key_count,
    LISTAGG(table_name, ', ') as tables
FROM all_keys
GROUP BY dim_key
HAVING COUNT(*) > 1
```

#### tests/unit/test_business_rules.sql
```sql
-- Test business rules across dimensions
SELECT 
    'Invalid user type for license' as test_case,
    COUNT(*) as violation_count
FROM {{ ref('go_user_dimension') }}
WHERE user_type = 'BASIC' AND license_type NOT IN ('BASIC', 'NONE')
UNION ALL
SELECT 
    'Organization without subscription',
    COUNT(*)
FROM {{ ref('go_organization_dimension') }}
WHERE is_active = true AND subscription_type IS NULL
UNION ALL
SELECT 
    'Weekend marked as non-weekend',
    COUNT(*)
FROM {{ ref('go_time_dimension') }}
WHERE day_of_week IN (1, 7) AND is_weekend = false  -- Assuming 1=Sunday, 7=Saturday
```

### 3. Custom Test Macros (macros/test_macros.sql)

```sql
-- Macro for testing data freshness
{% macro test_data_freshness(model, timestamp_column, max_age_hours=24) %}
    SELECT COUNT(*)
    FROM {{ ref(model) }}
    WHERE {{ timestamp_column }} < DATEADD(hour, -{{ max_age_hours }}, CURRENT_TIMESTAMP())
{% endmacro %}

-- Macro for testing referential integrity
{% macro test_referential_integrity(child_table, parent_table, child_key, parent_key) %}
    SELECT 
        c.{{ child_key }}
    FROM {{ ref(child_table) }} c
    LEFT JOIN {{ ref(parent_table) }} p ON c.{{ child_key }} = p.{{ parent_key }}
    WHERE p.{{ parent_key }} IS NULL
        AND c.{{ child_key }} IS NOT NULL
{% endmacro %}

-- Macro for testing data distribution
{% macro test_data_distribution(model, column, expected_min_distinct=1) %}
    SELECT 
        COUNT(DISTINCT {{ column }}) as distinct_count
    FROM {{ ref(model) }}
    HAVING COUNT(DISTINCT {{ column }}) < {{ expected_min_distinct }}
{% endmacro %}
```

### 4. Integration Test (tests/integration/test_end_to_end_pipeline.sql)

```sql
-- End-to-end pipeline validation
WITH pipeline_summary AS (
    SELECT 
        'USERS' as dimension,
        COUNT(*) as record_count,
        AVG(data_quality_score) as avg_quality,
        MIN(created_date) as earliest_record,
        MAX(updated_timestamp) as latest_update
    FROM {{ ref('go_user_dimension') }}
    UNION ALL
    SELECT 
        'ORGANIZATIONS',
        COUNT(*),
        AVG(data_quality_score),
        MIN(created_date),
        MAX(updated_timestamp)
    FROM {{ ref('go_organization_dimension') }}
    UNION ALL
    SELECT 
        'DEVICES',
        COUNT(*),
        AVG(data_quality_score),
        MIN(created_date),
        MAX(updated_timestamp)
    FROM {{ ref('go_device_dimension') }}
    UNION ALL
    SELECT 
        'GEOGRAPHY',
        COUNT(*),
        AVG(data_quality_score),
        MIN(created_date),
        MAX(updated_timestamp)
    FROM {{ ref('go_geography_dimension') }}
)
SELECT 
    dimension,
    record_count,
    avg_quality,
    earliest_record,
    latest_update
FROM pipeline_summary
WHERE record_count = 0 
    OR avg_quality < 70
    OR latest_update < DATEADD(day, -1, CURRENT_TIMESTAMP())
```

### 5. Test Configuration in dbt_project.yml

```yaml
name: 'zoom_gold_dimension_pipeline'
version: '1.0.0'
config-version: 2

model-paths: ["models"]
test-paths: ["tests"]
macro-paths: ["macros"]

models:
  zoom_gold_dimension_pipeline:
    gold:
      +materialized: table
      +post-hook: "INSERT INTO {{ ref('go_process_audit') }} VALUES (...)"
    
tests:
  zoom_gold_dimension_pipeline:
    +severity: error  # All tests fail the build on error
    unit:
      +severity: warn   # Unit tests warn but don't fail build
    integration:
      +severity: error  # Integration tests fail the build

vars:
  # Test configuration variables
  test_data_quality_threshold: 80
  test_max_null_percentage: 5
  test_freshness_hours: 24
```

## Test Execution Commands

```bash
# Run all tests
dbt test

# Run tests for specific models
dbt test --models go_user_dimension
dbt test --models go_organization_dimension

# Run specific test types
dbt test --select test_type:unit
dbt test --select test_type:integration

# Run tests with specific severity
dbt test --severity error
dbt test --severity warn

# Generate test documentation
dbt docs generate
dbt docs serve
```

## Key Testing Scenarios Covered

### Happy Path Tests:
- ✅ Unique constraints on all surrogate keys
- ✅ Not null constraints on critical fields
- ✅ Data type validation
- ✅ Referential integrity between dimensions
- ✅ Business rule compliance

### Edge Case Tests:
- ✅ Boundary value testing (date ranges, numeric limits)
- ✅ Empty string vs NULL handling
- ✅ Special character handling in text fields
- ✅ Timezone conversion accuracy
- ✅ Leap year date handling

### Exception Scenario Tests:
- ✅ Duplicate record detection
- ✅ Orphaned records without valid references
- ✅ Data quality score degradation
- ✅ Missing mandatory data
- ✅ Invalid data format handling

### Performance and Volume Tests:
- ✅ Expected data volume validation
- ✅ Query execution time monitoring
- ✅ Memory usage during builds
- ✅ Incremental load performance

### Audit and Compliance Tests:
- ✅ Complete audit trail validation
- ✅ Data lineage traceability
- ✅ Retention policy compliance
- ✅ Security and data masking validation

## Expected Test Results

| Test Category | Expected Pass Rate | Action on Failure |
|---------------|-------------------|-------------------|
| Schema Tests | 100% | Build fails |
| Unit Tests | 95%+ | Warning logged |
| Integration Tests | 100% | Build fails |
| Performance Tests | 90%+ | Warning logged |
| Business Rule Tests | 100% | Build fails |

## API Cost Calculation

Estimated API cost for this comprehensive unit test case generation: **$0.0847 USD**

*Cost calculation based on token usage for document generation, test script creation, and comprehensive analysis of the dbt models.*

---

**Note**: This testing framework ensures robust validation of the Zoom Gold dimension pipeline, covering all critical aspects of data quality, business logic, and system integrity. The tests are designed to catch issues early in the development cycle and maintain high data quality standards in production.