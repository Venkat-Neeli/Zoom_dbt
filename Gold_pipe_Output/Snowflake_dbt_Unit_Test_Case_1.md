# Snowflake dbt Unit Test Case

## Metadata
- **Author**: AAVA
- **Created on**: 2024-12-19
- **Description**: Comprehensive unit test cases for Snowflake dbt models focusing on data transformations, business rules validation, edge cases, and error handling scenarios
- **Version**: 1.0
- **Updated on**: 2024-12-19

## Test Case List

### Test Case ID: TC_001
**Test Case Description**: Validate data type transformations and casting operations
**Expected Outcome**: All data types should be correctly transformed without data loss or corruption

### Test Case ID: TC_002
**Test Case Description**: Test null value handling and default value assignments
**Expected Outcome**: Null values should be properly handled according to business rules

### Test Case ID: TC_003
**Test Case Description**: Validate date/timestamp transformations and timezone conversions
**Expected Outcome**: Date operations should maintain accuracy and proper timezone handling

### Test Case ID: TC_004
**Test Case Description**: Test aggregation functions and grouping operations
**Expected Outcome**: Aggregations should produce accurate results with proper grouping

### Test Case ID: TC_005
**Test Case Description**: Validate join operations and referential integrity
**Expected Outcome**: Joins should maintain data integrity and produce expected result sets

### Test Case ID: TC_006
**Test Case Description**: Test edge cases with empty datasets and boundary values
**Expected Outcome**: Models should handle edge cases gracefully without failures

### Test Case ID: TC_007
**Test Case Description**: Validate business rule implementations and calculations
**Expected Outcome**: Business logic should be correctly implemented and produce accurate results

### Test Case ID: TC_008
**Test Case Description**: Test error handling and data quality checks
**Expected Outcome**: Invalid data should be identified and handled appropriately

## dbt Test Scripts

### YAML-based Schema Tests

```yaml
# schema.yml
version: 2

models:
  - name: fact_sales
    description: "Sales fact table with comprehensive testing"
    columns:
      - name: sale_id
        description: "Primary key for sales transactions"
        tests:
          - unique
          - not_null
      - name: customer_id
        description: "Foreign key to customer dimension"
        tests:
          - not_null
          - relationships:
              to: ref('dim_customer')
              field: customer_id
      - name: product_id
        description: "Foreign key to product dimension"
        tests:
          - not_null
          - relationships:
              to: ref('dim_product')
              field: product_id
      - name: sale_amount
        description: "Transaction amount"
        tests:
          - not_null
          - dbt_utils.accepted_range:
              min_value: 0
              inclusive: false
      - name: sale_date
        description: "Date of transaction"
        tests:
          - not_null
          - dbt_utils.accepted_range:
              min_value: "'2020-01-01'"
              max_value: "current_date()"
      - name: quantity
        description: "Quantity sold"
        tests:
          - not_null
          - dbt_utils.accepted_range:
              min_value: 1
              max_value: 10000

  - name: dim_customer
    description: "Customer dimension table"
    columns:
      - name: customer_id
        description: "Primary key for customers"
        tests:
          - unique
          - not_null
      - name: email
        description: "Customer email address"
        tests:
          - unique
          - not_null
      - name: created_at
        description: "Customer creation timestamp"
        tests:
          - not_null

  - name: agg_monthly_sales
    description: "Monthly sales aggregation"
    tests:
      - dbt_utils.unique_combination_of_columns:
          combination_of_columns:
            - year_month
            - product_category
    columns:
      - name: total_sales
        description: "Total monthly sales amount"
        tests:
          - not_null
          - dbt_utils.accepted_range:
              min_value: 0
```

### Custom SQL-based dbt Tests

```sql
-- tests/test_data_freshness.sql
-- TC_001: Validate data freshness
SELECT COUNT(*) as stale_records
FROM {{ ref('fact_sales') }}
WHERE sale_date < CURRENT_DATE - INTERVAL '7 days'
HAVING COUNT(*) = 0
```

```sql
-- tests/test_revenue_consistency.sql
-- TC_002: Validate revenue calculation consistency
WITH revenue_check AS (
  SELECT 
    sale_id,
    sale_amount,
    quantity,
    unit_price,
    (quantity * unit_price) as calculated_amount
  FROM {{ ref('fact_sales') }}
  WHERE ABS(sale_amount - (quantity * unit_price)) > 0.01
)
SELECT COUNT(*) as inconsistent_records
FROM revenue_check
HAVING COUNT(*) = 0
```

```sql
-- tests/test_null_critical_fields.sql
-- TC_003: Ensure critical fields are never null
SELECT COUNT(*) as null_critical_fields
FROM {{ ref('fact_sales') }}
WHERE sale_id IS NULL 
   OR customer_id IS NULL 
   OR product_id IS NULL 
   OR sale_amount IS NULL
HAVING COUNT(*) = 0
```

```sql
-- tests/test_duplicate_transactions.sql
-- TC_004: Check for duplicate transactions
WITH duplicate_check AS (
  SELECT 
    customer_id,
    product_id,
    sale_amount,
    sale_date,
    COUNT(*) as duplicate_count
  FROM {{ ref('fact_sales') }}
  GROUP BY customer_id, product_id, sale_amount, sale_date
  HAVING COUNT(*) > 1
)
SELECT COUNT(*) as duplicate_transactions
FROM duplicate_check
HAVING COUNT(*) = 0
```

```sql
-- tests/test_aggregation_accuracy.sql
-- TC_005: Validate aggregation accuracy
WITH source_total AS (
  SELECT SUM(sale_amount) as source_sum
  FROM {{ ref('fact_sales') }}
  WHERE DATE_TRUNC('month', sale_date) = '2024-01-01'
),
agg_total AS (
  SELECT SUM(total_sales) as agg_sum
  FROM {{ ref('agg_monthly_sales') }}
  WHERE year_month = '2024-01'
)
SELECT 
  ABS(source_sum - agg_sum) as difference
FROM source_total
CROSS JOIN agg_total
WHERE ABS(source_sum - agg_sum) > 0.01
```

```sql
-- tests/test_date_range_validity.sql
-- TC_006: Validate date ranges
SELECT COUNT(*) as invalid_dates
FROM {{ ref('fact_sales') }}
WHERE sale_date > CURRENT_DATE 
   OR sale_date < '2020-01-01'
HAVING COUNT(*) = 0
```

```sql
-- tests/test_customer_dimension_integrity.sql
-- TC_007: Validate customer dimension integrity
SELECT COUNT(*) as orphaned_customers
FROM {{ ref('fact_sales') }} f
LEFT JOIN {{ ref('dim_customer') }} d
  ON f.customer_id = d.customer_id
WHERE d.customer_id IS NULL
HAVING COUNT(*) = 0
```

```sql
-- tests/test_business_rules.sql
-- TC_008: Validate specific business rules
SELECT COUNT(*) as rule_violations
FROM {{ ref('fact_sales') }}
WHERE (
  -- Rule 1: Sale amount should not exceed $100,000
  sale_amount > 100000
  OR
  -- Rule 2: Quantity should be reasonable (1-1000)
  quantity < 1 OR quantity > 1000
  OR
  -- Rule 3: Unit price should be positive
  unit_price <= 0
)
HAVING COUNT(*) = 0
```

## Edge Case Testing

### Empty Dataset Tests
```sql
-- tests/test_empty_dataset_handling.sql
-- Ensure models handle empty source tables gracefully
WITH empty_source AS (
  SELECT * FROM {{ ref('raw_sales') }} WHERE 1=0
)
SELECT 
  CASE 
    WHEN COUNT(*) = 0 THEN 'PASS'
    ELSE 'FAIL'
  END as test_result
FROM (
  SELECT * FROM {{ ref('fact_sales') }}
  WHERE sale_date = '1900-01-01' -- Non-existent date to simulate empty result
)
```

### Boundary Value Tests
```sql
-- tests/test_boundary_values.sql
-- Test boundary conditions
SELECT COUNT(*) as boundary_violations
FROM {{ ref('fact_sales') }}
WHERE (
  -- Test maximum values
  sale_amount = 999999999.99
  OR quantity = 2147483647
  OR
  -- Test minimum values
  sale_amount = 0.01
  OR quantity = 1
) AND (
  -- Ensure these boundary values are handled correctly
  sale_amount IS NULL
  OR quantity IS NULL
  OR sale_date IS NULL
)
HAVING COUNT(*) = 0
```

## Error Handling Tests

### Data Type Conversion Tests
```sql
-- tests/test_data_type_conversions.sql
-- Validate data type conversions don't cause errors
SELECT COUNT(*) as conversion_errors
FROM (
  SELECT 
    TRY_CAST(sale_amount AS DECIMAL(15,2)) as amount_decimal,
    TRY_CAST(sale_date AS DATE) as date_converted,
    TRY_CAST(quantity AS INTEGER) as qty_integer
  FROM {{ ref('fact_sales') }}
  WHERE TRY_CAST(sale_amount AS DECIMAL(15,2)) IS NULL
     OR TRY_CAST(sale_date AS DATE) IS NULL
     OR TRY_CAST(quantity AS INTEGER) IS NULL
)
HAVING COUNT(*) = 0
```

## Performance Tests

### Query Performance Validation
```sql
-- tests/test_query_performance.sql
-- Ensure queries complete within acceptable time limits
-- This would typically be run as part of CI/CD pipeline monitoring
SELECT 
  'Performance Test' as test_type,
  COUNT(*) as record_count,
  CURRENT_TIMESTAMP as execution_time
FROM {{ ref('fact_sales') }}
WHERE sale_date >= CURRENT_DATE - INTERVAL '30 days'
```

## API Cost Calculation

### Snowflake Credit Consumption Estimation

**Test Execution Cost Analysis:**

1. **Schema Tests (YAML-based)**:
   - Estimated credits per test: 0.001 - 0.005
   - Number of schema tests: 15
   - Total estimated credits: 0.015 - 0.075

2. **Custom SQL Tests**:
   - Simple validation tests: 0.002 - 0.01 credits each
   - Complex aggregation tests: 0.005 - 0.02 credits each
   - Number of custom tests: 12
   - Total estimated credits: 0.024 - 0.24

3. **Edge Case and Performance Tests**:
   - Boundary value tests: 0.003 - 0.015 credits each
   - Error handling tests: 0.002 - 0.01 credits each
   - Performance tests: 0.01 - 0.05 credits each
   - Number of additional tests: 6
   - Total estimated credits: 0.018 - 0.09

**Total Estimated Cost per Test Run**: 0.057 - 0.405 Snowflake credits

**Monthly Cost Estimation** (assuming daily test runs):
- Daily runs: 30
- Monthly credit consumption: 1.71 - 12.15 credits
- Estimated monthly cost: $3.42 - $24.30 (assuming $2 per credit)

**Cost Optimization Recommendations**:
1. Run full test suite only on main branch commits
2. Use incremental testing for feature branches
3. Implement test result caching where possible
4. Schedule heavy performance tests during off-peak hours
5. Use smaller warehouse sizes for simple validation tests

## Test Execution Instructions

### Running Tests Locally
```bash
# Run all tests
dbt test

# Run specific test
dbt test --select test_name

# Run tests for specific model
dbt test --select model_name

# Run tests with verbose output
dbt test --verbose
```

### CI/CD Integration
```yaml
# Example GitHub Actions workflow
name: dbt-tests
on:
  pull_request:
    branches: [main]
  push:
    branches: [main]

jobs:
  test:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v2
      - name: Setup dbt
        run: |
          pip install dbt-snowflake
      - name: Run dbt tests
        run: |
          dbt deps
          dbt test
        env:
          SNOWFLAKE_ACCOUNT: ${{ secrets.SNOWFLAKE_ACCOUNT }}
          SNOWFLAKE_USER: ${{ secrets.SNOWFLAKE_USER }}
          SNOWFLAKE_PASSWORD: ${{ secrets.SNOWFLAKE_PASSWORD }}
```

## Maintenance and Updates

### Regular Review Schedule
- **Weekly**: Review test results and failure patterns
- **Monthly**: Update test cases based on new business requirements
- **Quarterly**: Performance review and cost optimization
- **Annually**: Comprehensive test strategy review

### Test Case Versioning
- All test cases should be version controlled
- Document changes in test logic
- Maintain backward compatibility where possible
- Archive obsolete tests with proper documentation

---

**Document Control**
- Last Updated: 2024-12-19
- Next Review Date: 2025-01-19
- Approved By: AAVA
- Status: Active