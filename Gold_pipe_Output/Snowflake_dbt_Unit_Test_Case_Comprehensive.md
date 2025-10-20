# Comprehensive Snowflake dbt Unit Test Cases

## Overview
This document provides comprehensive unit test cases for dbt models in Snowflake environment. These tests validate data transformations, business rules, edge cases, and error handling to ensure reliable and performant data pipelines.

## Test Categories

### 1. Data Quality Tests

#### 1.1 Not Null Tests
```yaml
# tests/not_null_tests.yml
version: 2

models:
  - name: dim_customer
    columns:
      - name: customer_id
        tests:
          - not_null:
              severity: error
      - name: customer_name
        tests:
          - not_null:
              severity: warn
      - name: email
        tests:
          - not_null:
              severity: error
```

#### 1.2 Unique Tests
```yaml
# tests/unique_tests.yml
version: 2

models:
  - name: dim_customer
    columns:
      - name: customer_id
        tests:
          - unique:
              severity: error
      - name: email
        tests:
          - unique:
              severity: error
              config:
                where: "email IS NOT NULL"
```

#### 1.3 Accepted Values Tests
```yaml
# tests/accepted_values_tests.yml
version: 2

models:
  - name: fact_orders
    columns:
      - name: order_status
        tests:
          - accepted_values:
              values: ['pending', 'processing', 'shipped', 'delivered', 'cancelled']
              severity: error
      - name: payment_method
        tests:
          - accepted_values:
              values: ['credit_card', 'debit_card', 'paypal', 'bank_transfer']
              severity: warn
```

### 2. Referential Integrity Tests

#### 2.1 Relationships Tests
```yaml
# tests/relationships_tests.yml
version: 2

models:
  - name: fact_orders
    columns:
      - name: customer_id
        tests:
          - relationships:
              to: ref('dim_customer')
              field: customer_id
              severity: error
      - name: product_id
        tests:
          - relationships:
              to: ref('dim_product')
              field: product_id
              severity: error
```

### 3. Business Logic Tests

#### 3.1 Custom Data Tests
```sql
-- tests/test_order_total_calculation.sql
-- Test that order total equals sum of line items
SELECT 
    order_id,
    order_total,
    calculated_total,
    ABS(order_total - calculated_total) as difference
FROM (
    SELECT 
        o.order_id,
        o.order_total,
        SUM(ol.quantity * ol.unit_price) as calculated_total
    FROM {{ ref('fact_orders') }} o
    JOIN {{ ref('fact_order_lines') }} ol ON o.order_id = ol.order_id
    GROUP BY o.order_id, o.order_total
) 
WHERE ABS(order_total - calculated_total) > 0.01
```

```sql
-- tests/test_customer_lifetime_value.sql
-- Test that customer lifetime value is calculated correctly
SELECT 
    customer_id,
    calculated_clv,
    expected_clv,
    ABS(calculated_clv - expected_clv) as difference
FROM (
    SELECT 
        c.customer_id,
        c.customer_lifetime_value as calculated_clv,
        SUM(o.order_total) as expected_clv
    FROM {{ ref('dim_customer') }} c
    LEFT JOIN {{ ref('fact_orders') }} o ON c.customer_id = o.customer_id
    GROUP BY c.customer_id, c.customer_lifetime_value
)
WHERE ABS(calculated_clv - expected_clv) > 0.01
```

### 4. Data Freshness Tests

```yaml
# tests/freshness_tests.yml
version: 2

sources:
  - name: raw_data
    tables:
      - name: orders
        freshness:
          warn_after: {count: 12, period: hour}
          error_after: {count: 24, period: hour}
      - name: customers
        freshness:
          warn_after: {count: 1, period: day}
          error_after: {count: 2, period: day}
```

### 5. Performance Tests

#### 5.1 Row Count Tests
```sql
-- tests/test_row_count_consistency.sql
-- Ensure row counts are within expected ranges
SELECT 
    'fact_orders' as table_name,
    COUNT(*) as actual_count
FROM {{ ref('fact_orders') }}
WHERE COUNT(*) < 1000 OR COUNT(*) > 10000000

UNION ALL

SELECT 
    'dim_customer' as table_name,
    COUNT(*) as actual_count
FROM {{ ref('dim_customer') }}
WHERE COUNT(*) < 100 OR COUNT(*) > 1000000
```

#### 5.2 Data Volume Growth Tests
```sql
-- tests/test_data_volume_growth.sql
-- Test for unexpected data volume changes
WITH daily_counts AS (
    SELECT 
        DATE(created_at) as date,
        COUNT(*) as daily_count
    FROM {{ ref('fact_orders') }}
    WHERE created_at >= CURRENT_DATE - 30
    GROUP BY DATE(created_at)
),
avg_counts AS (
    SELECT AVG(daily_count) as avg_daily_count
    FROM daily_counts
)
SELECT 
    date,
    daily_count,
    avg_daily_count,
    daily_count / avg_daily_count as ratio
FROM daily_counts
CROSS JOIN avg_counts
WHERE daily_count / avg_daily_count > 2.0 OR daily_count / avg_daily_count < 0.5
```

### 6. Edge Case Tests

#### 6.1 Null Handling Tests
```sql
-- tests/test_null_handling.sql
-- Test proper handling of null values in calculations
SELECT 
    customer_id,
    order_total,
    discount_amount,
    final_amount
FROM {{ ref('fact_orders') }}
WHERE 
    (discount_amount IS NULL AND final_amount != order_total)
    OR (discount_amount IS NOT NULL AND final_amount != (order_total - discount_amount))
```

#### 6.2 Date Range Tests
```sql
-- tests/test_date_ranges.sql
-- Test for invalid date ranges
SELECT 
    order_id,
    order_date,
    ship_date,
    delivery_date
FROM {{ ref('fact_orders') }}
WHERE 
    ship_date < order_date
    OR delivery_date < ship_date
    OR order_date > CURRENT_DATE
```

### 7. Data Type and Format Tests

```sql
-- tests/test_email_format.sql
-- Test email format validation
SELECT 
    customer_id,
    email
FROM {{ ref('dim_customer') }}
WHERE 
    email IS NOT NULL 
    AND NOT REGEXP_LIKE(email, '^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}$')
```

```sql
-- tests/test_phone_format.sql
-- Test phone number format
SELECT 
    customer_id,
    phone_number
FROM {{ ref('dim_customer') }}
WHERE 
    phone_number IS NOT NULL
    AND NOT REGEXP_LIKE(phone_number, '^\+?[1-9]\d{1,14}$')
```

### 8. Aggregation Tests

```sql
-- tests/test_monthly_aggregations.sql
-- Test monthly aggregation accuracy
WITH monthly_orders AS (
    SELECT 
        DATE_TRUNC('month', order_date) as month,
        COUNT(*) as order_count,
        SUM(order_total) as total_revenue
    FROM {{ ref('fact_orders') }}
    GROUP BY DATE_TRUNC('month', order_date)
),
monthly_summary AS (
    SELECT 
        month,
        order_count as summary_order_count,
        total_revenue as summary_total_revenue
    FROM {{ ref('monthly_order_summary') }}
)
SELECT 
    mo.month,
    mo.order_count,
    ms.summary_order_count,
    mo.total_revenue,
    ms.summary_total_revenue
FROM monthly_orders mo
FULL OUTER JOIN monthly_summary ms ON mo.month = ms.month
WHERE 
    mo.order_count != ms.summary_order_count
    OR ABS(mo.total_revenue - ms.summary_total_revenue) > 0.01
```

### 9. Incremental Model Tests

```sql
-- tests/test_incremental_updates.sql
-- Test incremental model behavior
SELECT 
    order_id,
    updated_at,
    COUNT(*) as duplicate_count
FROM {{ ref('fact_orders_incremental') }}
GROUP BY order_id, updated_at
HAVING COUNT(*) > 1
```

### 10. Snapshot Tests

```sql
-- tests/test_snapshot_validity.sql
-- Test snapshot table validity
SELECT 
    customer_id,
    dbt_valid_from,
    dbt_valid_to
FROM {{ ref('customer_snapshot') }}
WHERE 
    dbt_valid_from > dbt_valid_to
    OR (dbt_valid_to IS NULL AND dbt_valid_from > CURRENT_TIMESTAMP)
```

## Test Execution Strategy

### 1. Pre-commit Tests
```bash
# Run basic data quality tests before committing
dbt test --select tag:data_quality
```

### 2. CI/CD Pipeline Tests
```bash
# Run all tests in CI/CD pipeline
dbt test --fail-fast
dbt test --store-failures
```

### 3. Production Monitoring Tests
```bash
# Run critical tests in production
dbt test --select tag:critical
```

## Test Configuration

### dbt_project.yml Configuration
```yaml
name: 'snowflake_dbt_project'
version: '1.0.0'
config-version: 2

model-paths: ["models"]
analysis-paths: ["analysis"]
test-paths: ["tests"]
seed-paths: ["data"]
macro-paths: ["macros"]
snapshot-paths: ["snapshots"]

target-path: "target"
clean-targets:
  - "target"
  - "dbt_packages"

tests:
  +store_failures: true
  +severity: 'error'

models:
  snowflake_dbt_project:
    +materialized: table
    staging:
      +materialized: view
    marts:
      +materialized: table
```

## Custom Test Macros

### Macro for Testing Data Completeness
```sql
-- macros/test_data_completeness.sql
{% macro test_data_completeness(model, column_name, threshold=0.95) %}

SELECT 
    '{{ column_name }}' as column_name,
    COUNT(*) as total_rows,
    COUNT({{ column_name }}) as non_null_rows,
    COUNT({{ column_name }}) * 1.0 / COUNT(*) as completeness_ratio
FROM {{ model }}
HAVING completeness_ratio < {{ threshold }}

{% endmacro %}
```

### Macro for Testing Value Ranges
```sql
-- macros/test_value_range.sql
{% macro test_value_range(model, column_name, min_value, max_value) %}

SELECT 
    {{ column_name }}
FROM {{ model }}
WHERE 
    {{ column_name }} IS NOT NULL
    AND ({{ column_name }} < {{ min_value }} OR {{ column_name }} > {{ max_value }})

{% endmacro %}
```

## Test Documentation

### Test Results Tracking
```sql
-- models/test_results_summary.sql
SELECT 
    test_name,
    model_name,
    column_name,
    test_status,
    failure_count,
    execution_time,
    run_timestamp
FROM {{ ref('dbt_test_results') }}
WHERE run_timestamp >= CURRENT_DATE - 7
ORDER BY run_timestamp DESC
```

## Best Practices

1. **Test Naming Convention**: Use descriptive names that clearly indicate what is being tested
2. **Severity Levels**: Use appropriate severity levels (error, warn) based on business impact
3. **Test Coverage**: Aim for comprehensive test coverage across all critical data paths
4. **Performance**: Optimize test queries for performance, especially on large datasets
5. **Documentation**: Document complex test logic and business rules
6. **Maintenance**: Regularly review and update tests as business requirements change

## Monitoring and Alerting

### Test Failure Notifications
```yaml
# profiles.yml
snowflake_dbt_project:
  target: prod
  outputs:
    prod:
      type: snowflake
      account: your_account
      user: your_user
      password: your_password
      role: your_role
      database: your_database
      warehouse: your_warehouse
      schema: your_schema
      threads: 4
      client_session_keep_alive: False
      query_tag: dbt_testing
```

## Conclusion

This comprehensive unit test suite ensures:
- Data quality and integrity
- Business rule compliance
- Performance monitoring
- Edge case handling
- Proper error detection

Regular execution of these tests will maintain high-quality data pipelines and prevent production issues.