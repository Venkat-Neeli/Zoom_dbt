# Snowflake dbt Unit Test Case - Version 4

## Metadata
- **Author**: AAVA
- **Version**: 4
- **Date**: 2024
- **Description**: Enhanced comprehensive unit test cases for Snowflake dbt models with advanced data quality tests, business rule validation, cross-table integration tests, and improved error handling for Gold Layer fact tables
- **Target Environment**: Snowflake Data Warehouse
- **dbt Version**: 1.0+

## Overview

This document provides comprehensive unit test cases for validating dbt models in Snowflake environment. Version 4 includes enhanced data quality tests, advanced business rule validation, improved cross-table integration tests, and robust error handling mechanisms.

### Gold Layer Fact Tables
1. Go_Meeting_Facts
2. Go_Participant_Facts
3. Go_Webinar_Facts
4. Go_Billing_Facts
5. Go_Usage_Facts
6. Go_Quality_Facts

## Test Categories

### 1. Enhanced Data Quality Tests

#### 1.1 Granular Null Checks

```sql
-- Test: Critical fields should not be null in Go_Meeting_Facts
SELECT 'Go_Meeting_Facts_null_check' as test_name,
       COUNT(*) as failed_records
FROM {{ ref('Go_Meeting_Facts') }}
WHERE meeting_id IS NULL 
   OR meeting_start_time IS NULL 
   OR host_id IS NULL
   OR meeting_duration IS NULL;

-- Test: Essential participant fields validation
SELECT 'Go_Participant_Facts_null_check' as test_name,
       COUNT(*) as failed_records
FROM {{ ref('Go_Participant_Facts') }}
WHERE participant_id IS NULL 
   OR meeting_id IS NULL 
   OR join_time IS NULL
   OR participant_email IS NULL;

-- Test: Webinar critical fields validation
SELECT 'Go_Webinar_Facts_null_check' as test_name,
       COUNT(*) as failed_records
FROM {{ ref('Go_Webinar_Facts') }}
WHERE webinar_id IS NULL 
   OR webinar_topic IS NULL 
   OR start_time IS NULL
   OR host_id IS NULL;

-- Test: Billing essential fields validation
SELECT 'Go_Billing_Facts_null_check' as test_name,
       COUNT(*) as failed_records
FROM {{ ref('Go_Billing_Facts') }}
WHERE billing_id IS NULL 
   OR account_id IS NULL 
   OR billing_amount IS NULL
   OR billing_date IS NULL;

-- Test: Usage metrics validation
SELECT 'Go_Usage_Facts_null_check' as test_name,
       COUNT(*) as failed_records
FROM {{ ref('Go_Usage_Facts') }}
WHERE usage_id IS NULL 
   OR account_id IS NULL 
   OR usage_date IS NULL
   OR usage_type IS NULL;

-- Test: Quality metrics validation
SELECT 'Go_Quality_Facts_null_check' as test_name,
       COUNT(*) as failed_records
FROM {{ ref('Go_Quality_Facts') }}
WHERE quality_id IS NULL 
   OR meeting_id IS NULL 
   OR quality_score IS NULL
   OR measurement_time IS NULL;
```

#### 1.2 Referential Integrity Tests

```sql
-- Test: Meeting-Participant referential integrity
SELECT 'meeting_participant_integrity' as test_name,
       COUNT(*) as failed_records
FROM {{ ref('Go_Participant_Facts') }} p
LEFT JOIN {{ ref('Go_Meeting_Facts') }} m
  ON p.meeting_id = m.meeting_id
WHERE m.meeting_id IS NULL;

-- Test: Meeting-Quality referential integrity
SELECT 'meeting_quality_integrity' as test_name,
       COUNT(*) as failed_records
FROM {{ ref('Go_Quality_Facts') }} q
LEFT JOIN {{ ref('Go_Meeting_Facts') }} m
  ON q.meeting_id = m.meeting_id
WHERE m.meeting_id IS NULL;

-- Test: Account-Billing referential integrity
SELECT 'account_billing_integrity' as test_name,
       COUNT(*) as failed_records
FROM {{ ref('Go_Billing_Facts') }} b
LEFT JOIN {{ ref('Go_Usage_Facts') }} u
  ON b.account_id = u.account_id
WHERE u.account_id IS NULL;
```

#### 1.3 Data Freshness Validation

```sql
-- Test: Meeting data freshness (within last 7 days)
SELECT 'meeting_data_freshness' as test_name,
       CASE WHEN MAX(meeting_start_time) >= CURRENT_DATE - 7 
            THEN 0 ELSE 1 END as failed_records
FROM {{ ref('Go_Meeting_Facts') }};

-- Test: Billing data freshness (within last 30 days)
SELECT 'billing_data_freshness' as test_name,
       CASE WHEN MAX(billing_date) >= CURRENT_DATE - 30 
            THEN 0 ELSE 1 END as failed_records
FROM {{ ref('Go_Billing_Facts') }};

-- Test: Usage data freshness (within last 24 hours)
SELECT 'usage_data_freshness' as test_name,
       CASE WHEN MAX(usage_date) >= CURRENT_DATE - 1 
            THEN 0 ELSE 1 END as failed_records
FROM {{ ref('Go_Usage_Facts') }};
```

#### 1.4 Duplicate Detection with Business Keys

```sql
-- Test: Meeting duplicates by business key
SELECT 'meeting_duplicates' as test_name,
       COUNT(*) as failed_records
FROM (
    SELECT meeting_id, host_id, meeting_start_time, COUNT(*) as cnt
    FROM {{ ref('Go_Meeting_Facts') }}
    GROUP BY meeting_id, host_id, meeting_start_time
    HAVING COUNT(*) > 1
);

-- Test: Participant duplicates by business key
SELECT 'participant_duplicates' as test_name,
       COUNT(*) as failed_records
FROM (
    SELECT participant_id, meeting_id, join_time, COUNT(*) as cnt
    FROM {{ ref('Go_Participant_Facts') }}
    GROUP BY participant_id, meeting_id, join_time
    HAVING COUNT(*) > 1
);

-- Test: Billing duplicates by business key
SELECT 'billing_duplicates' as test_name,
       COUNT(*) as failed_records
FROM (
    SELECT billing_id, account_id, billing_date, billing_amount, COUNT(*) as cnt
    FROM {{ ref('Go_Billing_Facts') }}
    GROUP BY billing_id, account_id, billing_date, billing_amount
    HAVING COUNT(*) > 1
);
```

### 2. Advanced Business Rule Validation

#### 2.1 Calculated Metrics and KPIs

```sql
-- Test: Meeting duration calculation validation
SELECT 'meeting_duration_calculation' as test_name,
       COUNT(*) as failed_records
FROM {{ ref('Go_Meeting_Facts') }}
WHERE meeting_duration != DATEDIFF('minute', meeting_start_time, meeting_end_time)
   OR meeting_duration < 0
   OR meeting_duration > 1440; -- Max 24 hours

-- Test: Participant attendance rate calculation
SELECT 'participant_attendance_rate' as test_name,
       COUNT(*) as failed_records
FROM (
    SELECT meeting_id,
           COUNT(participant_id) as actual_participants,
           MAX(expected_participants) as expected_participants,
           CASE WHEN MAX(expected_participants) > 0 
                THEN COUNT(participant_id)::FLOAT / MAX(expected_participants)
                ELSE 0 END as attendance_rate
    FROM {{ ref('Go_Participant_Facts') }} p
    JOIN {{ ref('Go_Meeting_Facts') }} m ON p.meeting_id = m.meeting_id
    GROUP BY meeting_id
) 
WHERE attendance_rate < 0 OR attendance_rate > 2; -- Allow 200% for walk-ins

-- Test: Quality score aggregation validation
SELECT 'quality_score_aggregation' as test_name,
       COUNT(*) as failed_records
FROM {{ ref('Go_Quality_Facts') }}
WHERE quality_score < 0 
   OR quality_score > 100
   OR (audio_quality + video_quality + connection_quality) / 3 != quality_score;
```

#### 2.2 Boundary Value Testing

```sql
-- Test: Numerical field boundaries - Meeting capacity
SELECT 'meeting_capacity_boundaries' as test_name,
       COUNT(*) as failed_records
FROM {{ ref('Go_Meeting_Facts') }}
WHERE meeting_capacity < 1 
   OR meeting_capacity > 10000; -- Reasonable upper limit

-- Test: Billing amount boundaries
SELECT 'billing_amount_boundaries' as test_name,
       COUNT(*) as failed_records
FROM {{ ref('Go_Billing_Facts') }}
WHERE billing_amount < 0 
   OR billing_amount > 1000000; -- $1M upper limit

-- Test: Usage metrics boundaries
SELECT 'usage_metrics_boundaries' as test_name,
       COUNT(*) as failed_records
FROM {{ ref('Go_Usage_Facts') }}
WHERE usage_minutes < 0 
   OR usage_minutes > 43200 -- 30 days * 24 hours * 60 minutes
   OR storage_gb < 0
   OR storage_gb > 10000; -- 10TB upper limit
```

#### 2.3 Date Range Validation

```sql
-- Test: Meeting date range validation
SELECT 'meeting_date_range' as test_name,
       COUNT(*) as failed_records
FROM {{ ref('Go_Meeting_Facts') }}
WHERE meeting_start_time < '2020-01-01' -- Zoom founded in 2011
   OR meeting_start_time > CURRENT_DATE + 365 -- Future meetings max 1 year
   OR meeting_end_time < meeting_start_time;

-- Test: Billing date range validation
SELECT 'billing_date_range' as test_name,
       COUNT(*) as failed_records
FROM {{ ref('Go_Billing_Facts') }}
WHERE billing_date < '2020-01-01'
   OR billing_date > CURRENT_DATE + 30 -- Future billing max 30 days
   OR due_date < billing_date;

-- Test: Webinar scheduling validation
SELECT 'webinar_date_range' as test_name,
       COUNT(*) as failed_records
FROM {{ ref('Go_Webinar_Facts') }}
WHERE start_time < '2020-01-01'
   OR start_time > CURRENT_DATE + 730 -- Future webinars max 2 years
   OR end_time < start_time;
```

#### 2.4 Currency Conversion Validation

```sql
-- Test: Currency conversion rates validation
SELECT 'currency_conversion_validation' as test_name,
       COUNT(*) as failed_records
FROM {{ ref('Go_Billing_Facts') }}
WHERE currency_code IS NOT NULL
  AND (exchange_rate <= 0 
       OR exchange_rate > 1000 -- Reasonable upper limit
       OR (currency_code = 'USD' AND exchange_rate != 1.0));

-- Test: Multi-currency billing consistency
SELECT 'multi_currency_consistency' as test_name,
       COUNT(*) as failed_records
FROM {{ ref('Go_Billing_Facts') }}
WHERE ABS(billing_amount_usd - (billing_amount * exchange_rate)) > 0.01;
```

### 3. Improved Cross-Table Integration Tests

#### 3.1 Comprehensive Join Validation

```sql
-- Test: Meeting-Participant-Quality three-way join
SELECT 'meeting_participant_quality_join' as test_name,
       COUNT(*) as failed_records
FROM {{ ref('Go_Meeting_Facts') }} m
FULL OUTER JOIN {{ ref('Go_Participant_Facts') }} p ON m.meeting_id = p.meeting_id
FULL OUTER JOIN {{ ref('Go_Quality_Facts') }} q ON m.meeting_id = q.meeting_id
WHERE m.meeting_id IS NULL OR p.meeting_id IS NULL OR q.meeting_id IS NULL;

-- Test: Account-Usage-Billing integration
SELECT 'account_usage_billing_join' as test_name,
       COUNT(*) as failed_records
FROM {{ ref('Go_Usage_Facts') }} u
FULL OUTER JOIN {{ ref('Go_Billing_Facts') }} b 
  ON u.account_id = b.account_id 
  AND DATE_TRUNC('month', u.usage_date) = DATE_TRUNC('month', b.billing_date)
WHERE u.account_id IS NULL OR b.account_id IS NULL;

-- Test: Webinar-Participant cross-reference
SELECT 'webinar_participant_cross_ref' as test_name,
       COUNT(*) as failed_records
FROM {{ ref('Go_Webinar_Facts') }} w
LEFT JOIN {{ ref('Go_Participant_Facts') }} p 
  ON w.webinar_id = p.webinar_id
WHERE w.registration_count > 0 AND p.participant_id IS NULL;
```

#### 3.2 Aggregate Consistency Tests

```sql
-- Test: Meeting participant count consistency
SELECT 'meeting_participant_count_consistency' as test_name,
       COUNT(*) as failed_records
FROM (
    SELECT m.meeting_id,
           m.actual_participants,
           COUNT(p.participant_id) as counted_participants
    FROM {{ ref('Go_Meeting_Facts') }} m
    LEFT JOIN {{ ref('Go_Participant_Facts') }} p ON m.meeting_id = p.meeting_id
    GROUP BY m.meeting_id, m.actual_participants
    HAVING m.actual_participants != COUNT(p.participant_id)
);

-- Test: Usage-Billing amount correlation
SELECT 'usage_billing_correlation' as test_name,
       COUNT(*) as failed_records
FROM (
    SELECT u.account_id,
           DATE_TRUNC('month', u.usage_date) as usage_month,
           SUM(u.usage_minutes) as total_usage,
           AVG(b.billing_amount) as avg_billing
    FROM {{ ref('Go_Usage_Facts') }} u
    JOIN {{ ref('Go_Billing_Facts') }} b 
      ON u.account_id = b.account_id 
      AND DATE_TRUNC('month', u.usage_date) = DATE_TRUNC('month', b.billing_date)
    GROUP BY u.account_id, DATE_TRUNC('month', u.usage_date)
    HAVING (SUM(u.usage_minutes) > 0 AND AVG(b.billing_amount) = 0)
        OR (SUM(u.usage_minutes) = 0 AND AVG(b.billing_amount) > 100)
);

-- Test: Quality metrics aggregation across meetings
SELECT 'quality_metrics_aggregation' as test_name,
       COUNT(*) as failed_records
FROM (
    SELECT m.host_id,
           AVG(q.quality_score) as avg_quality,
           COUNT(m.meeting_id) as meeting_count
    FROM {{ ref('Go_Meeting_Facts') }} m
    JOIN {{ ref('Go_Quality_Facts') }} q ON m.meeting_id = q.meeting_id
    GROUP BY m.host_id
    HAVING AVG(q.quality_score) < 0 OR AVG(q.quality_score) > 100
);
```

#### 3.3 Shared Dimension Relationship Tests

```sql
-- Test: Host consistency across meetings and webinars
SELECT 'host_consistency_meetings_webinars' as test_name,
       COUNT(*) as failed_records
FROM (
    SELECT host_id, host_email, COUNT(DISTINCT host_name) as name_variations
    FROM (
        SELECT host_id, host_email, host_name FROM {{ ref('Go_Meeting_Facts') }}
        UNION ALL
        SELECT host_id, host_email, host_name FROM {{ ref('Go_Webinar_Facts') }}
    )
    GROUP BY host_id, host_email
    HAVING COUNT(DISTINCT host_name) > 1
);

-- Test: Account dimension consistency
SELECT 'account_dimension_consistency' as test_name,
       COUNT(*) as failed_records
FROM (
    SELECT account_id, COUNT(DISTINCT account_name) as name_variations
    FROM (
        SELECT account_id, account_name FROM {{ ref('Go_Usage_Facts') }}
        UNION ALL
        SELECT account_id, account_name FROM {{ ref('Go_Billing_Facts') }}
    )
    GROUP BY account_id
    HAVING COUNT(DISTINCT account_name) > 1
);

-- Test: Time dimension consistency
SELECT 'time_dimension_consistency' as test_name,
       COUNT(*) as failed_records
FROM (
    SELECT DATE(event_time) as event_date, 
           COUNT(DISTINCT timezone) as timezone_variations
    FROM (
        SELECT meeting_start_time as event_time, timezone FROM {{ ref('Go_Meeting_Facts') }}
        UNION ALL
        SELECT start_time as event_time, timezone FROM {{ ref('Go_Webinar_Facts') }}
    )
    GROUP BY DATE(event_time)
    HAVING COUNT(DISTINCT timezone) > 10 -- Reasonable limit for global events
);
```

### 4. Enhanced Error Handling

#### 4.1 Malformed Data Tests

```sql
-- Test: Email format validation
SELECT 'email_format_validation' as test_name,
       COUNT(*) as failed_records
FROM (
    SELECT participant_email FROM {{ ref('Go_Participant_Facts') }}
    WHERE participant_email IS NOT NULL
      AND NOT REGEXP_LIKE(participant_email, '^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}$')
    UNION ALL
    SELECT host_email FROM {{ ref('Go_Meeting_Facts') }}
    WHERE host_email IS NOT NULL
      AND NOT REGEXP_LIKE(host_email, '^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}$')
);

-- Test: Phone number format validation
SELECT 'phone_format_validation' as test_name,
       COUNT(*) as failed_records
FROM {{ ref('Go_Participant_Facts') }}
WHERE phone_number IS NOT NULL
  AND NOT REGEXP_LIKE(phone_number, '^[+]?[0-9\s\-\(\)]{10,15}$');

-- Test: URL format validation for webinars
SELECT 'url_format_validation' as test_name,
       COUNT(*) as failed_records
FROM {{ ref('Go_Webinar_Facts') }}
WHERE webinar_url IS NOT NULL
  AND NOT REGEXP_LIKE(webinar_url, '^https?://[^\s/$.?#].[^\s]*$');

-- Test: JSON field validation
SELECT 'json_format_validation' as test_name,
       COUNT(*) as failed_records
FROM {{ ref('Go_Quality_Facts') }}
WHERE metadata_json IS NOT NULL
  AND NOT IS_VALID_JSON(metadata_json);
```

#### 4.2 Timezone Conversion Edge Cases

```sql
-- Test: Timezone conversion consistency
SELECT 'timezone_conversion_consistency' as test_name,
       COUNT(*) as failed_records
FROM {{ ref('Go_Meeting_Facts') }}
WHERE timezone IS NOT NULL
  AND meeting_start_time_utc IS NOT NULL
  AND meeting_start_time IS NOT NULL
  AND ABS(DATEDIFF('hour', meeting_start_time_utc, meeting_start_time)) > 14; -- Max timezone offset

-- Test: Daylight saving time handling
SELECT 'dst_handling_validation' as test_name,
       COUNT(*) as failed_records
FROM {{ ref('Go_Meeting_Facts') }}
WHERE timezone IN ('America/New_York', 'Europe/London', 'Australia/Sydney')
  AND DATE(meeting_start_time) BETWEEN '2024-03-10' AND '2024-03-17' -- DST transition week
  AND meeting_start_time_utc IS NULL;

-- Test: Invalid timezone handling
SELECT 'invalid_timezone_handling' as test_name,
       COUNT(*) as failed_records
FROM {{ ref('Go_Meeting_Facts') }}
WHERE timezone IS NOT NULL
  AND timezone NOT IN (
    SELECT timezone_name FROM INFORMATION_SCHEMA.TIME_ZONES
  );
```

#### 4.3 Extreme Date Values

```sql
-- Test: Extreme future dates
SELECT 'extreme_future_dates' as test_name,
       COUNT(*) as failed_records
FROM (
    SELECT meeting_start_time as event_date FROM {{ ref('Go_Meeting_Facts') }}
    UNION ALL
    SELECT start_time FROM {{ ref('Go_Webinar_Facts') }}
    UNION ALL
    SELECT billing_date FROM {{ ref('Go_Billing_Facts') }}
)
WHERE event_date > '2050-12-31'; -- Reasonable future limit

-- Test: Extreme past dates
SELECT 'extreme_past_dates' as test_name,
       COUNT(*) as failed_records
FROM (
    SELECT meeting_start_time as event_date FROM {{ ref('Go_Meeting_Facts') }}
    UNION ALL
    SELECT start_time FROM {{ ref('Go_Webinar_Facts') }}
    UNION ALL
    SELECT billing_date FROM {{ ref('Go_Billing_Facts') }}
)
WHERE event_date < '1970-01-01'; -- Unix epoch start

-- Test: Leap year handling
SELECT 'leap_year_handling' as test_name,
       COUNT(*) as failed_records
FROM (
    SELECT meeting_start_time as event_date FROM {{ ref('Go_Meeting_Facts') }}
    UNION ALL
    SELECT start_time FROM {{ ref('Go_Webinar_Facts') }}
)
WHERE MONTH(event_date) = 2 
  AND DAY(event_date) = 29
  AND YEAR(event_date) % 4 != 0; -- Invalid Feb 29 on non-leap years

-- Test: End-of-month date handling
SELECT 'end_of_month_handling' as test_name,
       COUNT(*) as failed_records
FROM {{ ref('Go_Billing_Facts') }}
WHERE DAY(billing_date) > 28
  AND MONTH(billing_date) = 2; -- February dates > 28
```

### 5. Performance and Optimization Tests

#### 5.1 Query Performance Tests

```sql
-- Test: Large table scan performance
SELECT 'large_table_scan_performance' as test_name,
       CASE WHEN COUNT(*) > 10000000 THEN 1 ELSE 0 END as failed_records
FROM {{ ref('Go_Meeting_Facts') }}
WHERE meeting_start_time >= CURRENT_DATE - 1; -- Should use partition pruning

-- Test: Join performance validation
SELECT 'join_performance_validation' as test_name,
       COUNT(*) as failed_records
FROM (
    SELECT m.meeting_id
    FROM {{ ref('Go_Meeting_Facts') }} m
    JOIN {{ ref('Go_Participant_Facts') }} p ON m.meeting_id = p.meeting_id
    JOIN {{ ref('Go_Quality_Facts') }} q ON m.meeting_id = q.meeting_id
    WHERE m.meeting_start_time >= CURRENT_DATE - 7
    GROUP BY m.meeting_id
    HAVING COUNT(*) = 0 -- Should not happen with proper joins
);
```

#### 5.2 Data Volume Tests

```sql
-- Test: Expected data volume ranges
SELECT 'meeting_volume_validation' as test_name,
       CASE WHEN daily_count BETWEEN 100 AND 100000 THEN 0 ELSE 1 END as failed_records
FROM (
    SELECT DATE(meeting_start_time) as meeting_date,
           COUNT(*) as daily_count
    FROM {{ ref('Go_Meeting_Facts') }}
    WHERE meeting_start_time >= CURRENT_DATE - 7
    GROUP BY DATE(meeting_start_time)
    ORDER BY meeting_date DESC
    LIMIT 1
);

-- Test: Participant volume validation
SELECT 'participant_volume_validation' as test_name,
       CASE WHEN daily_count BETWEEN 1000 AND 1000000 THEN 0 ELSE 1 END as failed_records
FROM (
    SELECT DATE(join_time) as participation_date,
           COUNT(*) as daily_count
    FROM {{ ref('Go_Participant_Facts') }}
    WHERE join_time >= CURRENT_DATE - 7
    GROUP BY DATE(join_time)
    ORDER BY participation_date DESC
    LIMIT 1
);
```

## dbt Test Implementation

### Generic Tests Configuration

```yaml
# schema.yml
version: 2

models:
  - name: Go_Meeting_Facts
    description: "Gold layer meeting facts with enhanced validation"
    tests:
      - dbt_utils.unique_combination_of_columns:
          combination_of_columns:
            - meeting_id
            - host_id
            - meeting_start_time
      - dbt_utils.expression_is_true:
          expression: "meeting_duration >= 0 AND meeting_duration <= 1440"
      - dbt_utils.not_null_proportion:
          at_least: 0.95
    columns:
      - name: meeting_id
        tests:
          - not_null
          - unique
      - name: meeting_start_time
        tests:
          - not_null
          - dbt_utils.expression_is_true:
              expression: ">= '2020-01-01'"
      - name: meeting_duration
        tests:
          - not_null
          - dbt_utils.expression_is_true:
              expression: ">= 0 AND <= 1440"
      - name: host_email
        tests:
          - dbt_utils.expression_is_true:
              expression: "REGEXP_LIKE(host_email, '^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\\.[A-Za-z]{2,}$')"

  - name: Go_Participant_Facts
    description: "Gold layer participant facts with enhanced validation"
    tests:
      - dbt_utils.unique_combination_of_columns:
          combination_of_columns:
            - participant_id
            - meeting_id
            - join_time
      - relationships:
          to: ref('Go_Meeting_Facts')
          field: meeting_id
    columns:
      - name: participant_id
        tests:
          - not_null
      - name: meeting_id
        tests:
          - not_null
          - relationships:
              to: ref('Go_Meeting_Facts')
              field: meeting_id
      - name: participant_email
        tests:
          - dbt_utils.expression_is_true:
              expression: "REGEXP_LIKE(participant_email, '^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\\.[A-Za-z]{2,}$')"

  - name: Go_Webinar_Facts
    description: "Gold layer webinar facts with enhanced validation"
    tests:
      - dbt_utils.unique_combination_of_columns:
          combination_of_columns:
            - webinar_id
            - host_id
            - start_time
    columns:
      - name: webinar_id
        tests:
          - not_null
          - unique
      - name: start_time
        tests:
          - not_null
          - dbt_utils.expression_is_true:
              expression: ">= '2020-01-01' AND <= CURRENT_DATE + 730"
      - name: registration_count
        tests:
          - dbt_utils.expression_is_true:
              expression: ">= 0"

  - name: Go_Billing_Facts
    description: "Gold layer billing facts with enhanced validation"
    tests:
      - dbt_utils.unique_combination_of_columns:
          combination_of_columns:
            - billing_id
            - account_id
            - billing_date
    columns:
      - name: billing_id
        tests:
          - not_null
          - unique
      - name: billing_amount
        tests:
          - not_null
          - dbt_utils.expression_is_true:
              expression: ">= 0 AND <= 1000000"
      - name: currency_code
        tests:
          - accepted_values:
              values: ['USD', 'EUR', 'GBP', 'JPY', 'CAD', 'AUD']
      - name: exchange_rate
        tests:
          - dbt_utils.expression_is_true:
              expression: "> 0 AND <= 1000"

  - name: Go_Usage_Facts
    description: "Gold layer usage facts with enhanced validation"
    tests:
      - dbt_utils.unique_combination_of_columns:
          combination_of_columns:
            - usage_id
            - account_id
            - usage_date
    columns:
      - name: usage_id
        tests:
          - not_null
          - unique
      - name: usage_minutes
        tests:
          - not_null
          - dbt_utils.expression_is_true:
              expression: ">= 0 AND <= 43200"
      - name: storage_gb
        tests:
          - dbt_utils.expression_is_true:
              expression: ">= 0 AND <= 10000"

  - name: Go_Quality_Facts
    description: "Gold layer quality facts with enhanced validation"
    tests:
      - dbt_utils.unique_combination_of_columns:
          combination_of_columns:
            - quality_id
            - meeting_id
            - measurement_time
      - relationships:
          to: ref('Go_Meeting_Facts')
          field: meeting_id
    columns:
      - name: quality_id
        tests:
          - not_null
          - unique
      - name: quality_score
        tests:
          - not_null
          - dbt_utils.expression_is_true:
              expression: ">= 0 AND <= 100"
      - name: audio_quality
        tests:
          - dbt_utils.expression_is_true:
              expression: ">= 0 AND <= 100"
      - name: video_quality
        tests:
          - dbt_utils.expression_is_true:
              expression: ">= 0 AND <= 100"
      - name: connection_quality
        tests:
          - dbt_utils.expression_is_true:
              expression: ">= 0 AND <= 100"
```

### Custom Test Macros

```sql
-- macros/test_email_format.sql
{% macro test_email_format(model, column_name) %}
  SELECT COUNT(*) as failures
  FROM {{ model }}
  WHERE {{ column_name }} IS NOT NULL
    AND NOT REGEXP_LIKE({{ column_name }}, '^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\\.[A-Za-z]{2,}$')
{% endmacro %}

-- macros/test_date_range.sql
{% macro test_date_range(model, column_name, min_date, max_date) %}
  SELECT COUNT(*) as failures
  FROM {{ model }}
  WHERE {{ column_name }} IS NOT NULL
    AND ({{ column_name }} < '{{ min_date }}' OR {{ column_name }} > '{{ max_date }}')
{% endmacro %}

-- macros/test_referential_integrity.sql
{% macro test_referential_integrity(model, column_name, ref_model, ref_column) %}
  SELECT COUNT(*) as failures
  FROM {{ model }} a
  LEFT JOIN {{ ref_model }} b ON a.{{ column_name }} = b.{{ ref_column }}
  WHERE a.{{ column_name }} IS NOT NULL
    AND b.{{ ref_column }} IS NULL
{% endmacro %}

-- macros/test_business_rule.sql
{% macro test_business_rule(model, rule_expression, rule_name) %}
  SELECT COUNT(*) as failures
  FROM {{ model }}
  WHERE NOT ({{ rule_expression }})
{% endmacro %}
```

## Test Execution Framework

### Test Runner Script

```bash
#!/bin/bash
# run_enhanced_tests.sh

echo "Starting Enhanced dbt Test Suite v4..."

# Set environment variables
export DBT_PROFILES_DIR=~/.dbt
export DBT_PROJECT_DIR=$(pwd)

# Run data quality tests
echo "Running Enhanced Data Quality Tests..."
dbt test --select tag:data_quality

# Run business rule tests
echo "Running Advanced Business Rule Tests..."
dbt test --select tag:business_rules

# Run integration tests
echo "Running Cross-Table Integration Tests..."
dbt test --select tag:integration

# Run error handling tests
echo "Running Enhanced Error Handling Tests..."
dbt test --select tag:error_handling

# Run performance tests
echo "Running Performance Tests..."
dbt test --select tag:performance

# Generate test report
echo "Generating Enhanced Test Report..."
dbt docs generate
dbt docs serve --port 8081

echo "Enhanced Test Suite v4 completed!"
```

### Continuous Integration Configuration

```yaml
# .github/workflows/enhanced_dbt_tests.yml
name: Enhanced dbt Tests v4

on:
  push:
    branches: [ main, develop ]
  pull_request:
    branches: [ main ]

jobs:
  enhanced-test:
    runs-on: ubuntu-latest
    
    steps:
    - uses: actions/checkout@v2
    
    - name: Set up Python
      uses: actions/setup-python@v2
      with:
        python-version: 3.9
    
    - name: Install dependencies
      run: |
        pip install dbt-snowflake dbt-utils
    
    - name: Run Enhanced Data Quality Tests
      run: |
        dbt test --select tag:data_quality
      env:
        SNOWFLAKE_ACCOUNT: ${{ secrets.SNOWFLAKE_ACCOUNT }}
        SNOWFLAKE_USER: ${{ secrets.SNOWFLAKE_USER }}
        SNOWFLAKE_PASSWORD: ${{ secrets.SNOWFLAKE_PASSWORD }}
    
    - name: Run Advanced Business Rule Tests
      run: |
        dbt test --select tag:business_rules
    
    - name: Run Integration Tests
      run: |
        dbt test --select tag:integration
    
    - name: Run Error Handling Tests
      run: |
        dbt test --select tag:error_handling
    
    - name: Generate Test Report
      run: |
        dbt docs generate
        
    - name: Upload Test Results
      uses: actions/upload-artifact@v2
      with:
        name: enhanced-test-results-v4
        path: target/
```

## Monitoring and Alerting

### Test Results Dashboard

```sql
-- Create test results monitoring view
CREATE OR REPLACE VIEW test_results_dashboard AS
SELECT 
    test_name,
    test_category,
    execution_time,
    status,
    failed_records,
    total_records,
    CASE 
        WHEN failed_records = 0 THEN 'PASS'
        WHEN failed_records <= total_records * 0.01 THEN 'WARNING'
        ELSE 'FAIL'
    END as test_result,
    execution_timestamp
FROM dbt_test_results
WHERE execution_timestamp >= CURRENT_DATE - 7
ORDER BY execution_timestamp DESC;
```

### Alert Configuration

```sql
-- Alert for critical test failures
CREATE OR REPLACE TASK alert_critical_test_failures
    WAREHOUSE = 'COMPUTE_WH'
    SCHEDULE = '5 MINUTE'
AS
SELECT 
    'CRITICAL_TEST_FAILURE' as alert_type,
    test_name,
    failed_records,
    'Test failed with ' || failed_records || ' failures' as message
FROM test_results_dashboard
WHERE test_result = 'FAIL'
  AND test_category IN ('data_quality', 'business_rules')
  AND execution_timestamp >= CURRENT_TIMESTAMP - INTERVAL '5 MINUTE';
```

## Best Practices and Recommendations

### 1. Test Organization
- Group tests by category (data quality, business rules, integration, error handling)
- Use consistent naming conventions
- Implement test dependencies and execution order
- Maintain test documentation and metadata

### 2. Performance Optimization
- Use appropriate WHERE clauses to limit test scope
- Implement incremental testing for large datasets
- Optimize test queries with proper indexing
- Monitor test execution times and resource usage

### 3. Error Handling
- Implement graceful failure handling
- Provide meaningful error messages
- Log test results for audit trails
- Set up automated alerting for critical failures

### 4. Maintenance
- Regularly review and update test cases
- Remove obsolete tests
- Update test thresholds based on data patterns
- Maintain test coverage metrics

## Conclusion

This enhanced version 4 of the Snowflake dbt Unit Test Case provides comprehensive coverage for data quality, business rules, integration testing, and error handling. The framework ensures robust validation of all Gold Layer fact tables while maintaining performance and scalability.

Key improvements in version 4:
- Enhanced granular data quality checks
- Advanced business rule validation with KPI testing
- Comprehensive cross-table integration tests
- Robust error handling for edge cases
- Performance optimization and monitoring
- Automated CI/CD integration
- Real-time alerting and dashboard monitoring

Regular execution of these tests will help maintain data integrity, catch issues early, and ensure reliable data pipeline operations in the Snowflake environment.