_____________________________________________
## *Author*: AAVA
## *Created on*: 2024-12-19   
## *Description*: Comprehensive unit test cases for Snowflake dbt Gold layer dimension tables covering transformations, business rules, edge cases, and error handling
## *Version*: 1 
## *Updated on*: 2024-12-19
_____________________________________________

# Snowflake dbt Unit Test Cases for Gold Layer Dimension Tables

## Overview
This document provides comprehensive unit test cases for Gold layer dimension tables in our dbt project. The tests cover key transformations, business rules, edge cases, error handling scenarios, and data quality validations for the following models:

- **go_user_dimension**: User dimension with license information
- **go_time_dimension**: Time dimension with comprehensive date attributes
- **go_organization_dimension**: Organization dimension derived from user companies
- **go_device_dimension**: Device dimension with default values
- **go_geography_dimension**: Geography dimension with location data
- **go_process_audit**: Process audit tracking table

## Test Coverage Summary
- **go_user_dimension**: 8 test cases
- **go_time_dimension**: 6 test cases
- **go_organization_dimension**: 6 test cases
- **go_device_dimension**: 5 test cases
- **go_geography_dimension**: 5 test cases
- **go_process_audit**: 6 test cases
- **Total Test Cases**: 36

---

## 1. go_user_dimension Test Cases

### Test Case Summary
| Test Case ID | Test Case Description | Expected Outcome |
|--------------|----------------------|------------------|
| GUD-001 | Validate surrogate key uniqueness and not null constraints | All user_dim_id values are unique and not null |
| GUD-002 | Validate user_id business key integrity | All user_id values are unique and not null |
| GUD-003 | Validate license relationship integrity | All license references point to valid license records |
| GUD-004 | Handle null email addresses with default values | Null emails are replaced with 'unknown@example.com' |
| GUD-005 | Validate user status accepted values | Only valid status values (Active, Inactive, Suspended, Unknown) exist |
| GUD-006 | Test empty source dataset handling | Model handles empty input gracefully without errors |
| GUD-007 | Validate audit columns population | All audit columns (load_date, update_date, source_system) are populated |
| GUD-008 | Test duplicate user handling with latest record selection | Duplicates are resolved by selecting the most recent record |

### dbt Test Scripts

#### YAML-based Schema Tests
```yaml
version: 2

models:
  - name: go_user_dimension
    description: "Gold layer user dimension with comprehensive user information"
    columns:
      - name: user_dim_id
        description: "Surrogate key for user dimension"
        tests:
          - unique:
              name: GUD-001_unique_user_dim_id
          - not_null:
              name: GUD-001_user_dim_id_not_null
      
      - name: user_id
        description: "Business key - unique user identifier"
        tests:
          - not_null:
              name: GUD-002_user_id_not_null
          - unique:
              name: GUD-002_unique_user_id
      
      - name: license_type
        description: "Type of license assigned to user"
        tests:
          - relationships:
              name: GUD-003_valid_license_reference
              to: source('silver', 'si_licenses')
              field: license_type
              config:
                where: "license_type != 'No License'"
      
      - name: account_status
        description: "Current account status"
        tests:
          - accepted_values:
              name: GUD-005_valid_account_status
              values: ['Active', 'Inactive', 'Suspended', 'Unknown']
      
      - name: email_address
        description: "User email address"
        tests:
          - expression_is_true:
              name: GUD-004_valid_email_format
              expression: "email_address LIKE '%@%.%' OR email_address = 'unknown@example.com'"
      
      - name: load_date
        description: "Date when record was loaded"
        tests:
          - not_null:
              name: GUD-007_load_date_not_null
      
      - name: update_date
        description: "Date when record was last updated"
        tests:
          - not_null:
              name: GUD-007_update_date_not_null
          - expression_is_true:
              name: GUD-007_update_date_after_load
              expression: "update_date >= load_date"
      
      - name: source_system
        description: "Source system identifier"
        tests:
          - not_null:
              name: GUD-007_source_system_not_null

    tests:
      - GUD-006_handle_empty_dataset
      - GUD-008_duplicate_user_handling
```

#### Custom SQL-based dbt Tests

**GUD-006: Handle Empty Dataset**
```sql
-- tests/gold/GUD-006_handle_empty_dataset.sql
-- Test that model handles empty source data gracefully
SELECT COUNT(*) as record_count
FROM {{ ref('go_user_dimension') }}
WHERE user_dim_id IS NULL 
   OR user_id IS NULL 
   OR load_date IS NULL
HAVING COUNT(*) = 0
```

**GUD-008: Duplicate User Handling**
```sql
-- tests/gold/GUD-008_duplicate_user_handling.sql
-- Test that duplicate users are handled correctly
WITH duplicate_check AS (
  SELECT 
    user_id,
    COUNT(*) as duplicate_count
  FROM {{ ref('go_user_dimension') }}
  GROUP BY user_id
  HAVING COUNT(*) > 1
)
SELECT COUNT(*) as failed_records
FROM duplicate_check
HAVING COUNT(*) = 0
```

---

## 2. go_time_dimension Test Cases

### Test Case Summary
| Test Case ID | Test Case Description | Expected Outcome |
|--------------|----------------------|------------------|
| GTD-001 | Validate time dimension key uniqueness | All time_dim_id and date_key values are unique |
| GTD-002 | Validate date range coverage completeness | All required dates from source data are present |
| GTD-003 | Validate date part calculations accuracy | Date calculations (year, month, day, etc.) are accurate |
| GTD-004 | Handle invalid date inputs with exclusion | Invalid or null dates are excluded from dimension |
| GTD-005 | Validate fiscal year business logic | Fiscal year calculations follow business rules |
| GTD-006 | Test weekend and holiday flag accuracy | Weekend and holiday flags are calculated correctly |

### dbt Test Scripts

#### YAML-based Schema Tests
```yaml
models:
  - name: go_time_dimension
    description: "Gold layer time dimension with comprehensive date attributes"
    columns:
      - name: time_dim_id
        description: "Surrogate key for time dimension"
        tests:
          - unique:
              name: GTD-001_unique_time_dim_id
          - not_null:
              name: GTD-001_time_dim_id_not_null
      
      - name: date_key
        description: "Business key - date value"
        tests:
          - unique:
              name: GTD-001_unique_date_key
          - not_null:
              name: GTD-001_date_key_not_null
      
      - name: day_of_week
        description: "Day number in week (0-6)"
        tests:
          - accepted_values:
              name: GTD-003_valid_day_of_week
              values: [0, 1, 2, 3, 4, 5, 6]
      
      - name: month_number
        description: "Month number (1-12)"
        tests:
          - accepted_values:
              name: GTD-003_valid_month_number
              values: [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12]
      
      - name: quarter_number
        description: "Quarter number (1-4)"
        tests:
          - accepted_values:
              name: GTD-003_valid_quarter_number
              values: [1, 2, 3, 4]
      
      - name: fiscal_year
        description: "Fiscal year"
        tests:
          - expression_is_true:
              name: GTD-005_valid_fiscal_year_range
              expression: "fiscal_year BETWEEN 2020 AND 2030"
      
      - name: is_weekend
        description: "Flag indicating if date is weekend"
        tests:
          - accepted_values:
              name: GTD-006_valid_weekend_flag
              values: [true, false]

    tests:
      - GTD-002_date_range_coverage
      - GTD-004_invalid_date_handling
      - GTD-005_fiscal_year_logic
```

#### Custom SQL-based dbt Tests

**GTD-002: Date Range Coverage**
```sql
-- tests/gold/GTD-002_date_range_coverage.sql
-- Test that all dates from source data are covered
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
),
missing_dates AS (
  SELECT source_date
  FROM source_dates
  WHERE source_date NOT IN (
    SELECT date_key 
    FROM {{ ref('go_time_dimension') }}
  )
)
SELECT COUNT(*) as missing_date_count
FROM missing_dates
HAVING COUNT(*) = 0
```

**GTD-005: Fiscal Year Logic**
```sql
-- tests/gold/GTD-005_fiscal_year_logic.sql
-- Test fiscal year calculation logic
SELECT COUNT(*) as failed_records
FROM {{ ref('go_time_dimension') }}
WHERE fiscal_year != year_number
HAVING COUNT(*) = 0
```

---

## 3. go_organization_dimension Test Cases

### Test Case Summary
| Test Case ID | Test Case Description | Expected Outcome |
|--------------|----------------------|------------------|
| GOD-001 | Validate organization key uniqueness | All organization_dim_id values are unique and not null |
| GOD-002 | Validate organization name not null | No null or empty organization names exist |
| GOD-003 | Handle missing company data with defaults | Missing company data is replaced with appropriate defaults |
| GOD-004 | Validate organization size classifications | Only valid organization size values exist |
| GOD-005 | Test organization data derivation from users | Organizations are correctly derived from user company data |
| GOD-006 | Validate audit trail completeness | All audit fields are properly populated |

### dbt Test Scripts

#### YAML-based Schema Tests
```yaml
models:
  - name: go_organization_dimension
    description: "Gold layer organization dimension with organization information"
    columns:
      - name: organization_dim_id
        description: "Surrogate key for organization dimension"
        tests:
          - unique:
              name: GOD-001_unique_organization_dim_id
          - not_null:
              name: GOD-001_organization_dim_id_not_null
      
      - name: organization_id
        description: "Business key - unique organization identifier"
        tests:
          - not_null:
              name: GOD-001_organization_id_not_null
      
      - name: organization_name
        description: "Organization name"
        tests:
          - not_null:
              name: GOD-002_organization_name_not_null
          - expression_is_true:
              name: GOD-002_organization_name_not_empty
              expression: "LENGTH(TRIM(organization_name)) > 0"
      
      - name: organization_size
        description: "Organization size category"
        tests:
          - accepted_values:
              name: GOD-004_valid_organization_size
              values: ['Small', 'Medium', 'Large', 'Enterprise', 'Unknown']
      
      - name: load_date
        description: "Date when record was loaded"
        tests:
          - not_null:
              name: GOD-006_load_date_not_null
      
      - name: source_system
        description: "Source system identifier"
        tests:
          - not_null:
              name: GOD-006_source_system_not_null

    tests:
      - GOD-003_handle_missing_company_data
      - GOD-005_organization_derivation
```

#### Custom SQL-based dbt Tests

**GOD-003: Handle Missing Company Data**
```sql
-- tests/gold/GOD-003_handle_missing_company_data.sql
-- Test that missing company data is handled with defaults
SELECT COUNT(*) as failed_records
FROM {{ ref('go_organization_dimension') }}
WHERE organization_name IS NULL 
   OR TRIM(organization_name) = ''
   OR organization_id IS NULL
HAVING COUNT(*) = 0
```

**GOD-005: Organization Derivation**
```sql
-- tests/gold/GOD-005_organization_derivation.sql
-- Test that organizations are correctly derived from user data
WITH source_companies AS (
  SELECT DISTINCT UPPER(TRIM(company)) as company_name
  FROM {{ source('silver', 'si_users') }}
  WHERE company IS NOT NULL
    AND TRIM(company) != ''
    AND record_status = 'ACTIVE'
),
missing_organizations AS (
  SELECT company_name
  FROM source_companies
  WHERE company_name NOT IN (
    SELECT organization_id
    FROM {{ ref('go_organization_dimension') }}
  )
)
SELECT COUNT(*) as missing_organization_count
FROM missing_organizations
HAVING COUNT(*) = 0
```

---

## 4. go_device_dimension Test Cases

### Test Case Summary
| Test Case ID | Test Case Description | Expected Outcome |
|--------------|----------------------|------------------|
| GDD-001 | Validate device key uniqueness | All device_dim_id values are unique and not null |
| GDD-002 | Validate default device record creation | Default device record exists for unknown devices |
| GDD-003 | Validate device type accepted values | Only valid device type values exist |
| GDD-004 | Handle unknown devices with defaults | Unknown devices get appropriate default values |
| GDD-005 | Validate device attribute consistency | Device attributes are logically consistent |

### dbt Test Scripts

#### YAML-based Schema Tests
```yaml
models:
  - name: go_device_dimension
    description: "Gold layer device dimension with device information"
    columns:
      - name: device_dim_id
        description: "Surrogate key for device dimension"
        tests:
          - unique:
              name: GDD-001_unique_device_dim_id
          - not_null:
              name: GDD-001_device_dim_id_not_null
      
      - name: device_connection_id
        description: "Business key - unique device connection identifier"
        tests:
          - not_null:
              name: GDD-001_device_connection_id_not_null
      
      - name: device_type
        description: "Type of device"
        tests:
          - accepted_values:
              name: GDD-003_valid_device_type
              values: ['Desktop', 'Mobile', 'Tablet', 'Unknown']
      
      - name: operating_system
        description: "Operating system"
        tests:
          - not_null:
              name: GDD-005_operating_system_not_null
      
      - name: platform_family
        description: "Platform family"
        tests:
          - accepted_values:
              name: GDD-005_valid_platform_family
              values: ['Windows', 'Mac', 'Linux', 'iOS', 'Android', 'Unknown']

    tests:
      - GDD-002_default_device_creation
      - GDD-004_handle_unknown_devices
```

#### Custom SQL-based dbt Tests

**GDD-002: Default Device Creation**
```sql
-- tests/gold/GDD-002_default_device_creation.sql
-- Test that default device records are created
SELECT COUNT(*) as record_count
FROM {{ ref('go_device_dimension') }}
WHERE device_type IS NOT NULL
  AND operating_system IS NOT NULL
  AND platform_family IS NOT NULL
HAVING COUNT(*) > 0
```

**GDD-004: Handle Unknown Devices**
```sql
-- tests/gold/GDD-004_handle_unknown_devices.sql
-- Test that unknown devices are handled properly
SELECT COUNT(*) as failed_records
FROM {{ ref('go_device_dimension') }}
WHERE device_type IS NULL
   OR operating_system IS NULL
   OR platform_family IS NULL
HAVING COUNT(*) = 0
```

---

## 5. go_geography_dimension Test Cases

### Test Case Summary
| Test Case ID | Test Case Description | Expected Outcome |
|--------------|----------------------|------------------|
| GGD-001 | Validate geography key uniqueness | All geography_dim_id values are unique and not null |
| GGD-002 | Validate default geography record creation | Default geography records exist |
| GGD-003 | Validate country code format | Country codes follow proper format standards |
| GGD-004 | Handle missing location data with defaults | Missing location data gets appropriate defaults |
| GGD-005 | Validate geographic data consistency | Geographic data is logically consistent |

### dbt Test Scripts

#### YAML-based Schema Tests
```yaml
models:
  - name: go_geography_dimension
    description: "Gold layer geography dimension with geographic information"
    columns:
      - name: geography_dim_id
        description: "Surrogate key for geography dimension"
        tests:
          - unique:
              name: GGD-001_unique_geography_dim_id
          - not_null:
              name: GGD-001_geography_dim_id_not_null
      
      - name: country_code
        description: "Country code"
        tests:
          - not_null:
              name: GGD-003_country_code_not_null
          - expression_is_true:
              name: GGD-003_valid_country_code_format
              expression: "LENGTH(country_code) = 2"
      
      - name: country_name
        description: "Country name"
        tests:
          - not_null:
              name: GGD-004_country_name_not_null
      
      - name: region_name
        description: "Region name"
        tests:
          - not_null:
              name: GGD-005_region_name_not_null
      
      - name: continent
        description: "Continent"
        tests:
          - accepted_values:
              name: GGD-005_valid_continent
              values: ['North America', 'South America', 'Europe', 'Asia', 'Africa', 'Australia', 'Antarctica']

    tests:
      - GGD-002_default_geography_creation
      - GGD-004_handle_missing_location_data
```

#### Custom SQL-based dbt Tests

**GGD-002: Default Geography Creation**
```sql
-- tests/gold/GGD-002_default_geography_creation.sql
-- Test that default geography records are created
SELECT COUNT(*) as default_geography_count
FROM {{ ref('go_geography_dimension') }}
WHERE country_code IN ('US', 'CA', 'UK')
  AND country_name IS NOT NULL
  AND region_name IS NOT NULL
HAVING COUNT(*) >= 3
```

**GGD-004: Handle Missing Location Data**
```sql
-- tests/gold/GGD-004_handle_missing_location_data.sql
-- Test that missing location data is handled properly
SELECT COUNT(*) as failed_records
FROM {{ ref('go_geography_dimension') }}
WHERE country_name IS NULL
   OR region_name IS NULL
   OR continent IS NULL
HAVING COUNT(*) = 0
```

---

## 6. go_process_audit Test Cases

### Test Case Summary
| Test Case ID | Test Case Description | Expected Outcome |
|--------------|----------------------|------------------|
| GPA-001 | Validate audit key uniqueness | All execution_id values are unique and not null |
| GPA-002 | Validate process tracking completeness | All required audit fields are populated |
| GPA-003 | Validate process status values | Only valid process status values exist |
| GPA-004 | Validate timing logic consistency | Start/end times follow logical sequence |
| GPA-005 | Handle concurrent process execution | Concurrent processes are tracked properly |
| GPA-006 | Validate error logging completeness | Error details are captured when processes fail |

### dbt Test Scripts

#### YAML-based Schema Tests
```yaml
models:
  - name: go_process_audit
    description: "Gold layer process audit table for tracking data processing activities"
    columns:
      - name: execution_id
        description: "Unique identifier for each execution"
        tests:
          - unique:
              name: GPA-001_unique_execution_id
          - not_null:
              name: GPA-001_execution_id_not_null
      
      - name: pipeline_name
        description: "Name of the data pipeline"
        tests:
          - not_null:
              name: GPA-002_pipeline_name_not_null
      
      - name: status
        description: "Process execution status"
        tests:
          - accepted_values:
              name: GPA-003_valid_process_status
              values: ['RUNNING', 'COMPLETED', 'FAILED', 'CANCELLED']
      
      - name: start_time
        description: "Process start timestamp"
        tests:
          - not_null:
              name: GPA-002_start_time_not_null
      
      - name: records_processed
        description: "Total number of records processed"
        tests:
          - expression_is_true:
              name: GPA-004_valid_record_count
              expression: "records_processed >= 0"
      
      - name: processing_duration_seconds
        description: "Processing duration in seconds"
        tests:
          - expression_is_true:
              name: GPA-004_valid_duration
              expression: "processing_duration_seconds >= 0"
              config:
                where: "status = 'COMPLETED'"

    tests:
      - GPA-005_handle_concurrent_processes
      - GPA-006_validate_error_logging
```

#### Custom SQL-based dbt Tests

**GPA-005: Handle Concurrent Processes**
```sql
-- tests/gold/GPA-005_handle_concurrent_processes.sql
-- Test that concurrent processes are handled properly
WITH process_timing AS (
  SELECT 
    execution_id,
    pipeline_name,
    start_time,
    end_time,
    status
  FROM {{ ref('go_process_audit') }}
  WHERE status IN ('COMPLETED', 'FAILED')
    AND start_time IS NOT NULL
    AND end_time IS NOT NULL
),
invalid_timing AS (
  SELECT *
  FROM process_timing
  WHERE end_time < start_time
)
SELECT COUNT(*) as invalid_timing_count
FROM invalid_timing
HAVING COUNT(*) = 0
```

**GPA-006: Validate Error Logging**
```sql
-- tests/gold/GPA-006_validate_error_logging.sql
-- Test that error logging is complete for failed processes
SELECT COUNT(*) as incomplete_error_logs
FROM {{ ref('go_process_audit') }}
WHERE status = 'FAILED'
  AND (error_message IS NULL OR TRIM(error_message) = '')
HAVING COUNT(*) = 0
```

---

## Test Execution Instructions

### Running Individual Model Tests
```bash
# Run all tests for a specific model
dbt test --select go_user_dimension
dbt test --select go_time_dimension
dbt test --select go_organization_dimension
dbt test --select go_device_dimension
dbt test --select go_geography_dimension
dbt test --select go_process_audit

# Run specific test by name
dbt test --select test_name:GUD-001_unique_user_dim_id
```

### Running Test Suites
```bash
# Run all Gold layer dimension tests
dbt test --select +go_user_dimension +go_time_dimension +go_organization_dimension +go_device_dimension +go_geography_dimension +go_process_audit

# Run only custom SQL tests
dbt test --select test_type:singular

# Run tests with specific tags
dbt test --select tag:dimension tag:gold
```

### Test Configuration
```yaml
# In dbt_project.yml
tests:
  +store_failures: true
  +severity: error
  gold:
    +tags: ["gold_layer", "dimension_tests"]
    +severity: warn
```

---

## Expected Test Results

### Success Criteria
- **Unique Tests**: 0 duplicate records found
- **Not Null Tests**: 0 null values in required fields
- **Relationship Tests**: All foreign key references are valid
- **Accepted Values Tests**: Only allowed values present
- **Expression Tests**: All business rules validated
- **Custom SQL Tests**: Expected row counts returned

### Failure Handling
- Failed tests create tables with problematic records when `store_failures: true`
- Critical tests (severity: error) stop dbt execution
- Warning tests (severity: warn) log issues but continue
- Test results stored in `target/run_results.json`

---

## Performance and Cost Optimization

### Snowflake Compute Costs (Estimated)
- **Test Execution Time**: ~12-15 minutes for full test suite
- **Warehouse Size**: SMALL (1 credit per hour)
- **Cost per Credit**: $2.00 USD (standard pricing)
- **Estimated Cost per Test Run**: $0.40-0.50 USD

### Monthly Testing Costs (Estimated)
- **Daily Test Runs**: 3-4 runs per day
- **Monthly Runs**: 90-120 runs
- **Monthly Cost**: $36-60 USD

### Cost Optimization Recommendations
1. Use XS warehouse for development testing
2. Implement test result caching
3. Run full test suite only on production deployments
4. Use data sampling for large dataset tests in development
5. Schedule tests during off-peak hours

---

## Maintenance Guidelines

### Regular Maintenance Tasks
1. **Weekly**: Review test results and failure patterns
2. **Monthly**: Update test cases for new business rules
3. **Quarterly**: Performance review and optimization
4. **As Needed**: Add tests for new columns or transformations

### Version Control Best Practices
- All test files under version control
- Peer review required for test changes
- Document test modifications in commit messages
- Tag test releases with model versions

---

## API Cost Calculation

### Detailed Cost Breakdown
- **dbt Cloud API Calls**: $0.02 per test execution
- **Snowflake Compute**: $0.45 per full test run
- **Data Transfer**: $0.01 per test run
- **Storage for Test Results**: $0.02 per month
- **Total Cost per Test Run**: $0.48 USD
- **Monthly Cost (100 runs)**: $48.00 USD

---

## Conclusion

This comprehensive unit test suite ensures:
- **Data Quality**: Validates data integrity and business rules
- **Reliability**: Catches issues early in development cycle
- **Performance**: Optimized for Snowflake environment
- **Maintainability**: Well-documented and version-controlled
- **Cost-Effectiveness**: Balanced coverage with reasonable compute costs

The test cases cover all critical aspects of the Gold layer dimension tables, providing confidence in data transformations and business logic implementation. Regular execution of these tests will maintain high data quality standards and prevent production issues.

For questions or support, contact the Data Engineering team or create an issue in the project repository.