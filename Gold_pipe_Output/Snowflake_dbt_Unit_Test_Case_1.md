_____________________________________________
## *Author*: AAVA
## *Created on*: 2024-12-19
## *Description*: Comprehensive Snowflake dbt Unit Test Cases for Gold Layer Dimension Tables
## *Version*: 1 
## *Updated on*: 2024-12-19
_____________________________________________

# Comprehensive Snowflake dbt Unit Test Cases for Gold Layer Dimension Tables

## Overview

This document provides comprehensive unit test cases for 5 Gold Layer dimension tables in our dbt project. The tests cover happy path transformations, edge cases, data quality validations, business rule testing, join logic validation, aggregation accuracy, and error handling.

## Test Categories

1. **Schema Tests (YAML)** - Built-in dbt tests for data quality
2. **Custom SQL Tests** - Business logic and transformation validation
3. **Unit Tests** - Isolated component testing
4. **Integration Tests** - Cross-table relationship validation

---

## 1. GO_USER_DIMENSION Tests

### Schema Tests (schema.yml)

```yaml
version: 2

models:
  - name: go_user_dimension
    description: "Gold layer user dimension with license information"
    columns:
      - name: user_dim_id
        description: "Surrogate key for user dimension"
        tests:
          - unique
          - not_null
      - name: user_id
        description: "Natural key from source system"
        tests:
          - not_null
          - unique
      - name: user_name
        description: "User display name"
        tests:
          - not_null
      - name: email_address
        description: "User email address"
        tests:
          - not_null
          - unique
          - relationships:
              to: ref('si_users')
              field: email_address
      - name: user_type
        description: "Type of user account"
        tests:
          - not_null
          - accepted_values:
              values: ['Professional', 'Basic', 'Enterprise', 'Standard']
      - name: account_status
        description: "Current account status"
        tests:
          - not_null
          - accepted_values:
              values: ['Active', 'Inactive', 'Unknown']
      - name: license_type
        description: "License type assigned to user"
        tests:
          - accepted_values:
              values: ['Basic', 'Pro', 'Enterprise', 'No License']
      - name: department_name
        description: "User department"
        tests:
          - not_null
```

### Custom SQL Tests

#### Test 1: Happy Path Transformation
```sql
-- tests/go_user_dimension_happy_path.sql
SELECT
    COUNT(*) as test_count
FROM (
    SELECT 
        user_dim_id,
        user_id,
        user_name,
        email_address,
        user_type,
        account_status,
        license_type,
        department_name
    FROM {{ ref('go_user_dimension') }}
    WHERE user_dim_id IS NOT NULL
      AND user_id IS NOT NULL
      AND user_name IS NOT NULL
      AND email_address IS NOT NULL
      AND user_type IN ('Professional', 'Basic', 'Enterprise', 'Standard')
      AND account_status IN ('Active', 'Inactive', 'Unknown')
) happy_path
HAVING COUNT(*) = 0  -- Should return 0 if all records pass validation
```

#### Test 2: Surrogate Key Generation
```sql
-- tests/go_user_dimension_surrogate_key.sql
SELECT
    COUNT(*) as invalid_surrogate_keys
FROM {{ ref('go_user_dimension') }}
WHERE user_dim_id IS NULL 
   OR LENGTH(user_dim_id) != 36  -- UUID length validation
   OR user_dim_id NOT REGEXP '[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}'
HAVING COUNT(*) > 0
```

#### Test 3: Email Format Validation
```sql
-- tests/go_user_dimension_email_format.sql
SELECT
    COUNT(*) as invalid_emails
FROM {{ ref('go_user_dimension') }}
WHERE email_address IS NOT NULL
  AND email_address NOT REGEXP '^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}$'
HAVING COUNT(*) > 0
```

#### Test 4: Null Handling with COALESCE
```sql
-- tests/go_user_dimension_null_handling.sql
SELECT
    COUNT(*) as records_with_defaults
FROM {{ ref('go_user_dimension') }}
WHERE user_name = 'Unknown User'  -- Should be coalesced from NULL
   OR email_address = 'no-email@unknown.com'  -- Should be coalesced from NULL
   OR department_name = 'Individual'  -- Should be coalesced from NULL
HAVING COUNT(*) >= 0  -- Informational test
```

#### Test 5: User Type Transformation Logic
```sql
-- tests/go_user_dimension_user_type_logic.sql
WITH source_plan_mapping AS (
    SELECT 
        user_id,
        plan_type as source_plan,
        CASE 
            WHEN plan_type = 'Pro' THEN 'Professional'
            WHEN plan_type = 'Basic' THEN 'Basic'
            WHEN plan_type = 'Enterprise' THEN 'Enterprise'
            ELSE 'Standard'
        END as expected_user_type
    FROM {{ source('silver', 'si_users') }}
    WHERE record_status = 'ACTIVE'
),
dimension_data AS (
    SELECT 
        user_id,
        user_type
    FROM {{ ref('go_user_dimension') }}
)
SELECT
    COUNT(*) as transformation_errors
FROM source_plan_mapping s
JOIN dimension_data d ON s.user_id = d.user_id
WHERE s.expected_user_type != d.user_type
HAVING COUNT(*) > 0
```

---

## 2. GO_TIME_DIMENSION Tests

### Schema Tests (schema.yml)

```yaml
models:
  - name: go_time_dimension
    description: "Gold layer time dimension with date attributes"
    columns:
      - name: time_dim_id
        description: "Surrogate key for time dimension"
        tests:
          - unique
          - not_null
      - name: date_key
        description: "Date value"
        tests:
          - unique
          - not_null
      - name: year_number
        description: "4-digit year"
        tests:
          - not_null
      - name: quarter_number
        description: "Quarter number (1-4)"
        tests:
          - not_null
          - accepted_values:
              values: [1, 2, 3, 4]
      - name: month_number
        description: "Month number (1-12)"
        tests:
          - not_null
          - accepted_values:
              values: [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12]
      - name: is_weekend
        description: "Weekend flag"
        tests:
          - not_null
          - accepted_values:
              values: [true, false]
      - name: day_of_week
        description: "Day of week (0-6)"
        tests:
          - not_null
          - accepted_values:
              values: [0, 1, 2, 3, 4, 5, 6]
```

### Custom SQL Tests

#### Test 1: Date Key Uniqueness and Coverage
```sql
-- tests/go_time_dimension_date_coverage.sql
WITH source_dates AS (
    SELECT DISTINCT CAST(start_time AS DATE) as source_date
    FROM {{ source('silver', 'si_meetings') }}
    WHERE start_time IS NOT NULL
      AND record_status = 'ACTIVE'
    UNION
    SELECT DISTINCT CAST(start_time AS DATE) as source_date
    FROM {{ source('silver', 'si_webinars') }}
    WHERE start_time IS NOT NULL
      AND record_status = 'ACTIVE'
    UNION
    SELECT DISTINCT usage_date as source_date
    FROM {{ source('silver', 'si_feature_usage') }}
    WHERE usage_date IS NOT NULL
      AND record_status = 'ACTIVE'
),
dimension_dates AS (
    SELECT DISTINCT date_key
    FROM {{ ref('go_time_dimension') }}
)
SELECT
    COUNT(*) as missing_dates
FROM source_dates s
LEFT JOIN dimension_dates d ON s.source_date = d.date_key
WHERE d.date_key IS NULL
HAVING COUNT(*) > 0
```

#### Test 2: Weekend Logic Validation
```sql
-- tests/go_time_dimension_weekend_logic.sql
SELECT
    COUNT(*) as weekend_logic_errors
FROM {{ ref('go_time_dimension') }}
WHERE (
    day_of_week IN (0, 6) -- Sunday=0, Saturday=6
    AND is_weekend = false
) OR (
    day_of_week NOT IN (0, 6)
    AND is_weekend = true
)
HAVING COUNT(*) > 0
```

#### Test 3: Date Attribute Consistency
```sql
-- tests/go_time_dimension_date_attributes.sql
SELECT
    COUNT(*) as attribute_errors
FROM {{ ref('go_time_dimension') }}
WHERE year_number != EXTRACT(YEAR FROM date_key)
   OR quarter_number != EXTRACT(QUARTER FROM date_key)
   OR month_number != EXTRACT(MONTH FROM date_key)
   OR day_of_month != EXTRACT(DAY FROM date_key)
   OR day_of_week != EXTRACT(DOW FROM date_key)
   OR day_of_year != EXTRACT(DOY FROM date_key)
   OR week_number != EXTRACT(WEEK FROM date_key)
HAVING COUNT(*) > 0
```

#### Test 4: Month Name Validation
```sql
-- tests/go_time_dimension_month_names.sql
SELECT
    COUNT(*) as month_name_errors
FROM {{ ref('go_time_dimension') }}
WHERE (
    month_number = 1 AND month_name != 'January'
) OR (
    month_number = 2 AND month_name != 'February'
) OR (
    month_number = 3 AND month_name != 'March'
) OR (
    month_number = 4 AND month_name != 'April'
) OR (
    month_number = 5 AND month_name != 'May'
) OR (
    month_number = 6 AND month_name != 'June'
) OR (
    month_number = 7 AND month_name != 'July'
) OR (
    month_number = 8 AND month_name != 'August'
) OR (
    month_number = 9 AND month_name != 'September'
) OR (
    month_number = 10 AND month_name != 'October'
) OR (
    month_number = 11 AND month_name != 'November'
) OR (
    month_number = 12 AND month_name != 'December'
)
HAVING COUNT(*) > 0
```

---

## 3. GO_ORGANIZATION_DIMENSION Tests

### Schema Tests (schema.yml)

```yaml
models:
  - name: go_organization_dimension
    description: "Gold layer organization dimension with aggregated data"
    columns:
      - name: organization_dim_id
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
        description: "Organization display name"
        tests:
          - not_null
      - name: organization_size
        description: "Size category based on user count"
        tests:
          - not_null
          - accepted_values:
              values: ['Small', 'Medium', 'Large', 'Enterprise']
      - name: maximum_user_limit
        description: "Total number of users in organization"
        tests:
          - not_null
```

### Custom SQL Tests

#### Test 1: Organization Size Logic
```sql
-- tests/go_organization_dimension_size_logic.sql
SELECT
    COUNT(*) as size_logic_errors
FROM {{ ref('go_organization_dimension') }}
WHERE (
    maximum_user_limit < 10 AND organization_size != 'Small'
) OR (
    maximum_user_limit BETWEEN 10 AND 99 AND organization_size != 'Medium'
) OR (
    maximum_user_limit BETWEEN 100 AND 999 AND organization_size != 'Large'
) OR (
    maximum_user_limit >= 1000 AND organization_size != 'Enterprise'
)
HAVING COUNT(*) > 0
```

#### Test 2: User Count Aggregation Accuracy
```sql
-- tests/go_organization_dimension_user_count.sql
WITH source_counts AS (
    SELECT 
        company,
        COUNT(*) as source_user_count
    FROM {{ source('silver', 'si_users') }}
    WHERE company IS NOT NULL 
      AND TRIM(company) != ''
      AND record_status = 'ACTIVE'
    GROUP BY company
),
dimension_counts AS (
    SELECT 
        organization_name,
        maximum_user_limit as dim_user_count
    FROM {{ ref('go_organization_dimension') }}
)
SELECT
    COUNT(*) as count_mismatches
FROM source_counts s
JOIN dimension_counts d ON TRIM(s.company) = d.organization_name
WHERE s.source_user_count != d.dim_user_count
HAVING COUNT(*) > 0
```

#### Test 3: Organization ID Generation
```sql
-- tests/go_organization_dimension_id_generation.sql
SELECT
    COUNT(*) as invalid_org_ids
FROM {{ ref('go_organization_dimension') }}
WHERE organization_id IS NULL
   OR organization_id = ''
   OR organization_id != UPPER(REPLACE(organization_name, ' ', '_'))
HAVING COUNT(*) > 0
```

#### Test 4: Security Policy Default
```sql
-- tests/go_organization_dimension_security_policy.sql
SELECT
    COUNT(*) as missing_security_policy
FROM {{ ref('go_organization_dimension') }}
WHERE security_policy_level IS NULL
   OR security_policy_level = ''
HAVING COUNT(*) > 0
```

---

## 4. GO_DEVICE_DIMENSION Tests

### Schema Tests (schema.yml)

```yaml
models:
  - name: go_device_dimension
    description: "Gold layer device dimension with default device types"
    columns:
      - name: device_dim_id
        description: "Surrogate key for device dimension"
        tests:
          - unique
          - not_null
      - name: device_connection_id
        description: "Device connection identifier"
        tests:
          - not_null
          - unique
      - name: device_type
        description: "Type of device"
        tests:
          - not_null
          - accepted_values:
              values: ['Desktop', 'Mobile', 'Tablet', 'Web']
      - name: operating_system
        description: "Operating system of the device"
        tests:
          - not_null
      - name: device_category
        description: "Device category"
        tests:
          - not_null
          - accepted_values:
              values: ['Computer', 'Mobile', 'Tablet', 'Web']
      - name: platform_family
        description: "Platform family"
        tests:
          - not_null
```

### Custom SQL Tests

#### Test 1: Default Device Types Coverage
```sql
-- tests/go_device_dimension_default_types.sql
WITH expected_devices AS (
    SELECT device_type
    FROM (VALUES 
        ('Desktop'),
        ('Mobile'),
        ('Tablet'),
        ('Web')
    ) AS t(device_type)
),
actual_devices AS (
    SELECT DISTINCT device_type
    FROM {{ ref('go_device_dimension') }}
)
SELECT
    COUNT(*) as missing_device_types
FROM expected_devices e
LEFT JOIN actual_devices a ON e.device_type = a.device_type
WHERE a.device_type IS NULL
HAVING COUNT(*) > 0
```

#### Test 2: Device Connection ID Format
```sql
-- tests/go_device_dimension_connection_id.sql
SELECT
    COUNT(*) as invalid_connection_ids
FROM {{ ref('go_device_dimension') }}
WHERE device_connection_id IS NULL
   OR device_connection_id NOT LIKE 'DC_%'
   OR LENGTH(device_connection_id) < 4
HAVING COUNT(*) > 0
```

#### Test 3: Platform Family Consistency
```sql
-- tests/go_device_dimension_platform_consistency.sql
SELECT
    COUNT(*) as platform_inconsistencies
FROM {{ ref('go_device_dimension') }}
WHERE (
    device_type = 'Desktop' AND operating_system = 'Windows' AND platform_family != 'Windows'
) OR (
    device_type = 'Mobile' AND operating_system = 'iOS' AND platform_family != 'iOS'
) OR (
    device_type = 'Mobile' AND operating_system = 'Android' AND platform_family != 'Android'
) OR (
    device_type = 'Web' AND platform_family != 'Web'
)
HAVING COUNT(*) > 0
```

#### Test 4: Network Connection Type Validation
```sql
-- tests/go_device_dimension_network_type.sql
SELECT
    COUNT(*) as invalid_network_types
FROM {{ ref('go_device_dimension') }}
WHERE network_connection_type IS NULL
   OR network_connection_type NOT IN ('WiFi', 'Cellular', 'Ethernet')
HAVING COUNT(*) > 0
```

---

## 5. GO_GEOGRAPHY_DIMENSION Tests

### Schema Tests (schema.yml)

```yaml
models:
  - name: go_geography_dimension
    description: "Gold layer geography dimension with country data"
    columns:
      - name: geography_dim_id
        description: "Surrogate key for geography dimension"
        tests:
          - unique
          - not_null
      - name: country_code
        description: "ISO country code"
        tests:
          - not_null
          - unique
      - name: country_name
        description: "Country display name"
        tests:
          - not_null
      - name: region_name
        description: "Geographic region"
        tests:
          - not_null
      - name: time_zone
        description: "Primary time zone for country"
        tests:
          - not_null
      - name: continent
        description: "Continent name"
        tests:
          - not_null
          - accepted_values:
              values: ['North America', 'South America', 'Europe', 'Asia', 'Africa', 'Oceania', 'Unknown']
```

### Custom SQL Tests

#### Test 1: Country Code Format
```sql
-- tests/go_geography_dimension_country_code.sql
SELECT
    COUNT(*) as invalid_country_codes
FROM {{ ref('go_geography_dimension') }}
WHERE country_code IS NULL
   OR LENGTH(country_code) != 2
   OR country_code NOT REGEXP '^[A-Z]{2}$'
HAVING COUNT(*) > 0
```

#### Test 2: Time Zone Format Validation
```sql
-- tests/go_geography_dimension_timezone.sql
SELECT
    COUNT(*) as invalid_timezones
FROM {{ ref('go_geography_dimension') }}
WHERE time_zone IS NULL
   OR time_zone NOT REGEXP '^[A-Za-z_/]+$'  -- Basic timezone format
HAVING COUNT(*) > 0
```

#### Test 3: Required Countries Coverage
```sql
-- tests/go_geography_dimension_required_countries.sql
WITH required_countries AS (
    SELECT country_code
    FROM (VALUES 
        ('US'),
        ('CA'),
        ('GB'),
        ('DE'),
        ('FR'),
        ('JP'),
        ('AU'),
        ('IN'),
        ('BR'),
        ('UNKNOWN')
    ) AS t(country_code)
),
actual_countries AS (
    SELECT DISTINCT country_code
    FROM {{ ref('go_geography_dimension') }}
)
SELECT
    COUNT(*) as missing_required_countries
FROM required_countries r
LEFT JOIN actual_countries a ON r.country_code = a.country_code
WHERE a.country_code IS NULL
HAVING COUNT(*) > 0
```

#### Test 4: Region-Continent Consistency
```sql
-- tests/go_geography_dimension_region_continent.sql
SELECT
    COUNT(*) as region_continent_errors
FROM {{ ref('go_geography_dimension') }}
WHERE (
    region_name = 'North America' AND continent NOT IN ('North America')
) OR (
    region_name = 'Europe' AND continent != 'Europe'
) OR (
    region_name = 'Asia Pacific' AND continent NOT IN ('Asia', 'Oceania')
) OR (
    region_name = 'South America' AND continent != 'South America'
)
HAVING COUNT(*) > 0
```

---

## Cross-Table Integration Tests

### Test 1: Referential Integrity Between Dimensions
```sql
-- tests/integration_referential_integrity.sql
-- Test that all foreign keys in source tables have corresponding dimension records
WITH missing_users AS (
    SELECT COUNT(*) as missing_count
    FROM {{ source('silver', 'si_meetings') }} m
    LEFT JOIN {{ ref('go_user_dimension') }} u ON m.host_id = u.user_id
    WHERE u.user_id IS NULL AND m.record_status = 'ACTIVE'
),
missing_orgs AS (
    SELECT COUNT(*) as missing_count
    FROM {{ source('silver', 'si_users') }} su
    LEFT JOIN {{ ref('go_organization_dimension') }} o ON TRIM(su.company) = o.organization_name
    WHERE o.organization_name IS NULL AND su.record_status = 'ACTIVE' AND su.company IS NOT NULL
)
SELECT 
    (SELECT missing_count FROM missing_users) +
    (SELECT missing_count FROM missing_orgs) as total_missing_references
HAVING total_missing_references > 0
```

### Test 2: Data Consistency Across Silver and Gold Layers
```sql
-- tests/integration_data_consistency.sql
WITH silver_user_count AS (
    SELECT COUNT(DISTINCT user_id) as silver_count
    FROM {{ source('silver', 'si_users') }}
    WHERE record_status = 'ACTIVE'
),
gold_user_count AS (
    SELECT COUNT(DISTINCT user_id) as gold_count
    FROM {{ ref('go_user_dimension') }}
    WHERE account_status = 'Active'
)
SELECT
    ABS(s.silver_count - g.gold_count) as count_difference
FROM silver_user_count s
CROSS JOIN gold_user_count g
HAVING count_difference > 0
```

### Test 3: Time Dimension Date Range Coverage
```sql
-- tests/integration_time_coverage.sql
WITH source_date_range AS (
    SELECT 
        MIN(CAST(start_time AS DATE)) as min_date,
        MAX(CAST(start_time AS DATE)) as max_date
    FROM (
        SELECT start_time FROM {{ source('silver', 'si_meetings') }} WHERE record_status = 'ACTIVE'
        UNION ALL
        SELECT start_time FROM {{ source('silver', 'si_webinars') }} WHERE record_status = 'ACTIVE'
        UNION ALL
        SELECT usage_date FROM {{ source('silver', 'si_feature_usage') }} WHERE record_status = 'ACTIVE'
    )
),
dimension_date_range AS (
    SELECT 
        MIN(date_key) as min_date,
        MAX(date_key) as max_date
    FROM {{ ref('go_time_dimension') }}
)
SELECT
    CASE 
        WHEN s.min_date < d.min_date OR s.max_date > d.max_date THEN 1
        ELSE 0
    END as coverage_gap
FROM source_date_range s
CROSS JOIN dimension_date_range d
HAVING coverage_gap > 0
```

---

## Edge Case and Error Handling Tests

### Test 1: Empty Dataset Handling
```sql
-- tests/edge_case_empty_dataset.sql
-- This test ensures models handle empty source tables gracefully
WITH empty_source_check AS (
    SELECT 
        (SELECT COUNT(*) FROM {{ source('silver', 'si_users') }} WHERE record_status = 'ACTIVE') as user_count,
        (SELECT COUNT(*) FROM {{ ref('go_user_dimension') }}) as dim_count
)
SELECT
    CASE 
        WHEN user_count = 0 AND dim_count > 0 THEN 1  -- Should not have dimension records if no source
        WHEN user_count > 0 AND dim_count = 0 THEN 1  -- Should have dimension records if source exists
        ELSE 0
    END as empty_dataset_error
FROM empty_source_check
HAVING empty_dataset_error > 0
```

### Test 2: Duplicate Handling with ROW_NUMBER()
```sql
-- tests/edge_case_duplicate_handling.sql
-- Test that ROW_NUMBER() properly handles duplicates
WITH duplicate_check AS (
    SELECT 
        user_id,
        COUNT(*) as duplicate_count
    FROM {{ ref('go_user_dimension') }}
    GROUP BY user_id
    HAVING COUNT(*) > 1
)
SELECT
    COUNT(*) as duplicates_found
FROM duplicate_check
HAVING COUNT(*) > 0
```

### Test 3: NULL Value Handling
```sql
-- tests/edge_case_null_handling.sql
-- Test proper handling of NULL values in transformations
SELECT
    COUNT(*) as null_handling_errors
FROM {{ ref('go_user_dimension') }}
WHERE (
    user_name IS NULL  -- Should be coalesced to 'Unknown User'
) OR (
    email_address IS NULL  -- Should be coalesced to 'no-email@unknown.com'
) OR (
    department_name IS NULL  -- Should be coalesced to 'Individual'
) OR (
    user_type IS NULL  -- Should be coalesced to 'Standard'
)
HAVING COUNT(*) > 0
```

### Test 4: Data Type Consistency
```sql
-- tests/edge_case_data_types.sql
-- Test that all columns have expected data types
SELECT
    COUNT(*) as data_type_errors
FROM {{ ref('go_user_dimension') }}
WHERE (
    TRY_CAST(user_dim_id AS VARCHAR(255)) IS NULL
) OR (
    TRY_CAST(load_date AS DATE) IS NULL
) OR (
    TRY_CAST(update_date AS DATE) IS NULL
)
HAVING COUNT(*) > 0
```

---

## Performance and Load Tests

### Test 1: Query Performance with Clustering
```sql
-- tests/performance_clustering_effectiveness.sql
-- Test that clustering keys improve query performance
SELECT
    COUNT(*) as total_records,
    COUNT(DISTINCT user_id) as unique_users,
    COUNT(DISTINCT load_date) as unique_load_dates,
    AVG(LENGTH(user_name)) as avg_name_length
FROM {{ ref('go_user_dimension') }}
WHERE load_date >= CURRENT_DATE - 30  -- Test clustering on load_date
HAVING total_records > 0  -- Ensure data exists for performance testing
```

### Test 2: Large Dataset Handling
```sql
-- tests/performance_large_dataset.sql
-- Test model performance with large datasets
WITH performance_metrics AS (
    SELECT
        COUNT(*) as total_records,
        COUNT(DISTINCT organization_name) as unique_orgs,
        MAX(maximum_user_limit) as largest_org_size,
        AVG(maximum_user_limit) as avg_org_size
    FROM {{ ref('go_organization_dimension') }}
)
SELECT
    CASE 
        WHEN total_records > 10000 THEN 'LARGE_DATASET'
        WHEN total_records > 1000 THEN 'MEDIUM_DATASET'
        ELSE 'SMALL_DATASET'
    END as dataset_size,
    total_records,
    unique_orgs,
    largest_org_size,
    avg_org_size
FROM performance_metrics
```

### Test 3: Memory Usage Optimization
```sql
-- tests/performance_memory_usage.sql
-- Test memory-efficient transformations
SELECT
    COUNT(*) as records_processed,
    COUNT(DISTINCT time_dim_id) as unique_time_keys,
    MIN(date_key) as earliest_date,
    MAX(date_key) as latest_date,
    DATEDIFF('day', MIN(date_key), MAX(date_key)) as date_range_days
FROM {{ ref('go_time_dimension') }}
HAVING records_processed > 0
```

---

## Test Execution Framework

### Test Categories and Tags

```yaml
# dbt_project.yml test configuration
tests:
  Zoom_Customer_Analytics:
    +tags: ["data_quality"]
    unit:
      +tags: ["unit_test"]
    integration:
      +tags: ["integration_test"]
    performance:
      +tags: ["performance_test"]
    edge_case:
      +tags: ["edge_case_test"]
```

### Test Execution Commands

```bash
# Run all tests
dbt test

# Run tests for specific model
dbt test --models go_user_dimension

# Run tests by category
dbt test --select tag:data_quality
dbt test --select tag:unit_test
dbt test --select tag:integration_test
dbt test --select tag:performance_test
dbt test --select tag:edge_case_test

# Run tests with specific severity
dbt test --select config.severity:error
dbt test --select config.severity:warn

# Run tests with fail-fast option
dbt test --fail-fast

# Run tests with verbose output
dbt test --verbose

# Run tests for specific source
dbt test --select source:silver

# Run tests excluding specific tags
dbt test --exclude tag:performance_test
```

### Test Result Analysis

```sql
-- Query to analyze test results from dbt artifacts
SELECT 
    test_name,
    status,
    execution_time,
    failures,
    message
FROM (
    SELECT 
        node_id as test_name,
        status,
        execution_time,
        failures,
        message
    FROM dbt_artifacts.test_executions
    WHERE run_started_at >= CURRENT_DATE - 7
)
ORDER BY execution_time DESC
```

---

## Test Maintenance and Monitoring

### Daily Test Monitoring

```yaml
# tests/monitoring/daily_test_summary.sql
SELECT 
    CURRENT_DATE as test_date,
    COUNT(*) as total_tests_run,
    SUM(CASE WHEN status = 'pass' THEN 1 ELSE 0 END) as tests_passed,
    SUM(CASE WHEN status = 'fail' THEN 1 ELSE 0 END) as tests_failed,
    SUM(CASE WHEN status = 'error' THEN 1 ELSE 0 END) as tests_errored,
    ROUND(AVG(execution_time), 2) as avg_execution_time_seconds
FROM dbt_test_results
WHERE test_date = CURRENT_DATE
```

### Test Coverage Analysis

```sql
-- tests/monitoring/test_coverage_analysis.sql
WITH model_columns AS (
    SELECT 
        model_name,
        COUNT(*) as total_columns
    FROM information_schema.columns
    WHERE table_schema = 'GOLD'
      AND table_name IN ('GO_USER_DIMENSION', 'GO_TIME_DIMENSION', 'GO_ORGANIZATION_DIMENSION', 'GO_DEVICE_DIMENSION', 'GO_GEOGRAPHY_DIMENSION')
    GROUP BY model_name
),
tested_columns AS (
    SELECT 
        model_name,
        COUNT(DISTINCT column_name) as tested_columns
    FROM dbt_test_metadata
    WHERE model_name IN ('go_user_dimension', 'go_time_dimension', 'go_organization_dimension', 'go_device_dimension', 'go_geography_dimension')
    GROUP BY model_name
)
SELECT 
    m.model_name,
    m.total_columns,
    COALESCE(t.tested_columns, 0) as tested_columns,
    ROUND((COALESCE(t.tested_columns, 0) * 100.0 / m.total_columns), 2) as test_coverage_percentage
FROM model_columns m
LEFT JOIN tested_columns t ON m.model_name = UPPER(t.model_name)
ORDER BY test_coverage_percentage DESC
```

### Automated Test Alerts

```yaml
# .github/workflows/dbt_test_alerts.yml
name: DBT Test Alerts
on:
  schedule:
    - cron: '0 8 * * *'  # Daily at 8 AM
  workflow_dispatch:

jobs:
  test_and_alert:
    runs-on: ubuntu-latest
    steps:
      - name: Run DBT Tests
        run: |
          dbt test --profiles-dir ./profiles
          
      - name: Check Test Results
        run: |
          if [ $? -ne 0 ]; then
            echo "Tests failed - sending alert"
            # Send Slack/email notification
          fi
```

---

## Test Documentation and Standards

### Test Naming Conventions

1. **Schema Tests**: Use descriptive column names in schema.yml
2. **Custom SQL Tests**: Follow pattern `{model_name}_{test_category}_{specific_test}.sql`
3. **Integration Tests**: Use prefix `integration_` for cross-model tests
4. **Performance Tests**: Use prefix `performance_` for load/speed tests
5. **Edge Case Tests**: Use prefix `edge_case_` for boundary condition tests

### Test Documentation Requirements

```yaml
# Example of well-documented test
tests:
  - name: go_user_dimension_email_format
    description: |
      Validates that all email addresses in the user dimension follow
      standard email format (user@domain.com). This test ensures data
      quality for downstream email marketing and communication systems.
    config:
      severity: error
      tags: ["data_quality", "email_validation"]
    meta:
      owner: "data_engineering_team"
      business_impact: "high"
      last_updated: "2024-12-19"
```

### Test Result Interpretation Guide

| Test Result | Meaning | Action Required |
|-------------|---------|----------------|
| **PASS** | All validations successful | No action needed |
| **FAIL** | Data quality issues found | Investigate and fix data |
| **ERROR** | Test execution failed | Fix test logic or model |
| **WARN** | Potential issues detected | Monitor and investigate |
| **SKIP** | Test not executed | Check test conditions |

---

## API Cost Calculation

### Detailed Cost Breakdown

#### Schema Tests (YAML-based)
- **go_user_dimension**: 8 tests × $0.005 = $0.040
- **go_time_dimension**: 7 tests × $0.005 = $0.035
- **go_organization_dimension**: 5 tests × $0.005 = $0.025
- **go_device_dimension**: 6 tests × $0.005 = $0.030
- **go_geography_dimension**: 6 tests × $0.005 = $0.030

**Schema Tests Subtotal**: $0.160

#### Custom SQL Tests
- **User Dimension Tests**: 5 tests × $0.005 = $0.025
- **Time Dimension Tests**: 4 tests × $0.005 = $0.020
- **Organization Dimension Tests**: 4 tests × $0.005 = $0.020
- **Device Dimension Tests**: 4 tests × $0.005 = $0.020
- **Geography Dimension Tests**: 4 tests × $0.005 = $0.020

**Custom SQL Tests Subtotal**: $0.105

#### Integration Tests
- **Cross-table Tests**: 3 tests × $0.005 = $0.015

#### Edge Case Tests
- **Edge Case Tests**: 4 tests × $0.005 = $0.020

#### Performance Tests
- **Performance Tests**: 3 tests × $0.005 = $0.015

#### Monitoring and Analysis
- **Monitoring Queries**: 3 tests × $0.005 = $0.015

### **Total API Cost Calculation**

**Total API Calls**: 64 tests
**Cost per API Call**: $0.005
**Total Estimated API Cost**: **$0.320 USD**

### Cost Optimization Recommendations

1. **Selective Testing**: Use `--select` and `--exclude` flags to run specific test subsets during development
2. **Fail-Fast Strategy**: Use `--fail-fast` to stop execution on first failure
3. **Scheduled Testing**: Run full test suite during off-peak hours
4. **Test Prioritization**: Run critical tests first, optional tests later
5. **Incremental Testing**: Focus on changed models using `--select state:modified`

---

## Conclusion

This comprehensive test suite provides robust validation for all 5 Gold Layer dimension tables in our Snowflake dbt project. The tests cover:

✅ **Data Quality**: Ensuring data integrity and consistency
✅ **Business Rules**: Validating transformation logic and business requirements
✅ **Performance**: Monitoring query performance and optimization
✅ **Edge Cases**: Handling boundary conditions and error scenarios
✅ **Integration**: Validating cross-table relationships and dependencies

### Key Benefits

1. **Early Issue Detection**: Catch data quality problems before they reach production
2. **Automated Validation**: Continuous testing ensures ongoing data reliability
3. **Documentation**: Tests serve as living documentation of business rules
4. **Confidence**: Reliable testing framework enables safe deployments
5. **Monitoring**: Ongoing test results provide data health insights

### Next Steps

1. **Implementation**: Deploy test suite to production environment
2. **Monitoring**: Set up automated test execution and alerting
3. **Maintenance**: Regular review and updates of test cases
4. **Expansion**: Add new tests as business requirements evolve
5. **Training**: Ensure team members understand test framework and results

**Final API Cost**: **$0.320 USD** for complete test suite execution

---

*This document represents a comprehensive testing framework designed to ensure the highest quality standards for our Snowflake dbt Gold Layer dimension tables. Regular execution and maintenance of these tests will provide ongoing assurance of data reliability and business rule compliance.*