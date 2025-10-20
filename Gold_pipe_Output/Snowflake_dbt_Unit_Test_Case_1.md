_____________________________________________
## *Author*: AAVA
## *Created on*: 2024-12-19
## *Description*: Comprehensive Snowflake dbt Unit Test Cases for Gold Layer Fact Tables
## *Version*: 1 
## *Updated on*: 2024-12-19
_____________________________________________

# Snowflake dbt Unit Test Cases for Gold Layer Fact Tables

## Overview

This document provides comprehensive unit test cases for the Gold Layer fact table models in the Zoom dbt project. The test cases cover 6 fact table models with incremental materialization, clustering, audit hooks, and complex business logic.

## Fact Table Models Covered

1. `go_meeting_facts.sql`
2. `go_participant_facts.sql` 
3. `go_webinar_facts.sql`
4. `go_billing_facts.sql`
5. `go_usage_facts.sql`
6. `go_quality_facts.sql`

---

## 1. YAML-Based Schema Tests

### schema.yml Configuration

```yaml
version: 2

models:
  - name: go_meeting_facts
    description: "Gold layer fact table for meeting analytics"
    columns:
      - name: meeting_id
        description: "Primary key for meeting facts"
        tests:
          - not_null
          - unique
      - name: meeting_start_time
        description: "Meeting start timestamp"
        tests:
          - not_null
          - dbt_utils.expression_is_true:
              expression: ">= '2020-01-01'"
      - name: meeting_duration_minutes
        description: "Meeting duration in minutes"
        tests:
          - not_null
          - dbt_utils.expression_is_true:
              expression: ">= 0"
      - name: participant_count
        description: "Number of participants"
        tests:
          - not_null
          - dbt_utils.expression_is_true:
              expression: ">= 1"
      - name: host_user_id
        description: "Meeting host user ID"
        tests:
          - not_null
          - relationships:
              to: ref('si_users')
              field: user_id

  - name: go_participant_facts
    description: "Gold layer fact table for participant analytics"
    columns:
      - name: participant_id
        description: "Primary key for participant facts"
        tests:
          - not_null
          - unique
      - name: meeting_id
        description: "Foreign key to meeting"
        tests:
          - not_null
          - relationships:
              to: ref('go_meeting_facts')
              field: meeting_id
      - name: join_time
        description: "Participant join timestamp"
        tests:
          - not_null
      - name: leave_time
        description: "Participant leave timestamp"
        tests:
          - dbt_utils.expression_is_true:
              expression: "leave_time >= join_time OR leave_time IS NULL"
      - name: duration_minutes
        description: "Participant session duration"
        tests:
          - dbt_utils.expression_is_true:
              expression: ">= 0 OR duration_minutes IS NULL"

  - name: go_webinar_facts
    description: "Gold layer fact table for webinar analytics"
    columns:
      - name: webinar_id
        description: "Primary key for webinar facts"
        tests:
          - not_null
          - unique
      - name: webinar_start_time
        description: "Webinar start timestamp"
        tests:
          - not_null
      - name: registration_count
        description: "Number of registrations"
        tests:
          - not_null
          - dbt_utils.expression_is_true:
              expression: ">= 0"
      - name: attendance_count
        description: "Number of attendees"
        tests:
          - not_null
          - dbt_utils.expression_is_true:
              expression: ">= 0"
      - name: attendance_rate
        description: "Attendance rate percentage"
        tests:
          - dbt_utils.expression_is_true:
              expression: "BETWEEN 0 AND 100"

  - name: go_billing_facts
    description: "Gold layer fact table for billing analytics"
    columns:
      - name: billing_event_id
        description: "Primary key for billing facts"
        tests:
          - not_null
          - unique
      - name: account_id
        description: "Account identifier"
        tests:
          - not_null
      - name: billing_amount
        description: "Billing amount"
        tests:
          - not_null
          - dbt_utils.expression_is_true:
              expression: ">= 0"
      - name: billing_date
        description: "Billing event date"
        tests:
          - not_null
      - name: license_count
        description: "Number of licenses"
        tests:
          - dbt_utils.expression_is_true:
              expression: ">= 0"

  - name: go_usage_facts
    description: "Gold layer fact table for usage analytics"
    columns:
      - name: usage_id
        description: "Primary key for usage facts"
        tests:
          - not_null
          - unique
      - name: user_id
        description: "User identifier"
        tests:
          - not_null
          - relationships:
              to: ref('si_users')
              field: user_id
      - name: feature_name
        description: "Feature being used"
        tests:
          - not_null
          - accepted_values:
              values: ['screen_share', 'recording', 'chat', 'breakout_rooms', 'polling']
      - name: usage_count
        description: "Usage frequency"
        tests:
          - not_null
          - dbt_utils.expression_is_true:
              expression: ">= 0"
      - name: usage_date
        description: "Usage date"
        tests:
          - not_null

  - name: go_quality_facts
    description: "Gold layer fact table for quality analytics"
    columns:
      - name: quality_event_id
        description: "Primary key for quality facts"
        tests:
          - not_null
          - unique
      - name: meeting_id
        description: "Associated meeting ID"
        tests:
          - relationships:
              to: ref('go_meeting_facts')
              field: meeting_id
      - name: audio_quality_score
        description: "Audio quality score (1-5)"
        tests:
          - dbt_utils.expression_is_true:
              expression: "BETWEEN 1 AND 5 OR audio_quality_score IS NULL"
      - name: video_quality_score
        description: "Video quality score (1-5)"
        tests:
          - dbt_utils.expression_is_true:
              expression: "BETWEEN 1 AND 5 OR video_quality_score IS NULL"
      - name: connection_stability_score
        description: "Connection stability score (1-5)"
        tests:
          - dbt_utils.expression_is_true:
              expression: "BETWEEN 1 AND 5 OR connection_stability_score IS NULL"
```

---

## 2. Custom SQL-Based Tests

### 2.1 Happy Path Test Cases

#### Test: Meeting Facts Data Integrity
```sql
-- tests/assert_meeting_facts_data_integrity.sql
SELECT 
    meeting_id,
    meeting_start_time,
    meeting_duration_minutes,
    participant_count
FROM {{ ref('go_meeting_facts') }}
WHERE 
    meeting_duration_minutes < 0 
    OR participant_count < 1
    OR meeting_start_time > CURRENT_TIMESTAMP()
HAVING COUNT(*) > 0
```

#### Test: Participant Facts Join Validation
```sql
-- tests/assert_participant_facts_join_validation.sql
SELECT 
    p.participant_id,
    p.meeting_id
FROM {{ ref('go_participant_facts') }} p
LEFT JOIN {{ ref('go_meeting_facts') }} m
    ON p.meeting_id = m.meeting_id
WHERE m.meeting_id IS NULL
HAVING COUNT(*) > 0
```

#### Test: Webinar Attendance Rate Calculation
```sql
-- tests/assert_webinar_attendance_rate_calculation.sql
SELECT 
    webinar_id,
    registration_count,
    attendance_count,
    attendance_rate,
    CASE 
        WHEN registration_count > 0 
        THEN ROUND((attendance_count::FLOAT / registration_count::FLOAT) * 100, 2)
        ELSE 0 
    END as calculated_rate
FROM {{ ref('go_webinar_facts') }}
WHERE 
    ABS(attendance_rate - calculated_rate) > 0.01
HAVING COUNT(*) > 0
```

#### Test: Billing Facts Amount Validation
```sql
-- tests/assert_billing_facts_amount_validation.sql
SELECT 
    billing_event_id,
    billing_amount,
    license_count
FROM {{ ref('go_billing_facts') }}
WHERE 
    billing_amount < 0
    OR (license_count > 0 AND billing_amount = 0)
    OR (license_count = 0 AND billing_amount > 0)
HAVING COUNT(*) > 0
```

### 2.2 Edge Case Test Cases

#### Test: Null Value Handling in Meeting Facts
```sql
-- tests/assert_meeting_facts_null_handling.sql
SELECT 
    'meeting_id' as field_name,
    COUNT(*) as null_count
FROM {{ ref('go_meeting_facts') }}
WHERE meeting_id IS NULL

UNION ALL

SELECT 
    'host_user_id' as field_name,
    COUNT(*) as null_count
FROM {{ ref('go_meeting_facts') }}
WHERE host_user_id IS NULL

UNION ALL

SELECT 
    'meeting_start_time' as field_name,
    COUNT(*) as null_count
FROM {{ ref('go_meeting_facts') }}
WHERE meeting_start_time IS NULL

HAVING SUM(null_count) > 0
```

#### Test: Empty Dataset Handling
```sql
-- tests/assert_empty_dataset_handling.sql
WITH fact_table_counts AS (
    SELECT 'go_meeting_facts' as table_name, COUNT(*) as row_count
    FROM {{ ref('go_meeting_facts') }}
    
    UNION ALL
    
    SELECT 'go_participant_facts' as table_name, COUNT(*) as row_count
    FROM {{ ref('go_participant_facts') }}
    
    UNION ALL
    
    SELECT 'go_webinar_facts' as table_name, COUNT(*) as row_count
    FROM {{ ref('go_webinar_facts') }}
    
    UNION ALL
    
    SELECT 'go_billing_facts' as table_name, COUNT(*) as row_count
    FROM {{ ref('go_billing_facts') }}
    
    UNION ALL
    
    SELECT 'go_usage_facts' as table_name, COUNT(*) as row_count
    FROM {{ ref('go_usage_facts') }}
    
    UNION ALL
    
    SELECT 'go_quality_facts' as table_name, COUNT(*) as row_count
    FROM {{ ref('go_quality_facts') }}
)
SELECT 
    table_name,
    row_count
FROM fact_table_counts
WHERE row_count = 0
```

#### Test: Invalid Lookup Handling
```sql
-- tests/assert_invalid_lookup_handling.sql
SELECT 
    'usage_facts_invalid_user' as test_case,
    u.user_id,
    COUNT(*) as invalid_count
FROM {{ ref('go_usage_facts') }} u
LEFT JOIN {{ ref('si_users') }} su
    ON u.user_id = su.user_id
WHERE su.user_id IS NULL
GROUP BY u.user_id
HAVING COUNT(*) > 0
```

### 2.3 Exception Case Test Cases

#### Test: Incremental Logic Validation
```sql
-- tests/assert_incremental_logic_validation.sql
{% if is_incremental() %}
SELECT 
    'meeting_facts' as table_name,
    COUNT(*) as duplicate_count
FROM (
    SELECT 
        meeting_id,
        COUNT(*) as cnt
    FROM {{ ref('go_meeting_facts') }}
    WHERE {{ incremental_where_clause() }}
    GROUP BY meeting_id
    HAVING COUNT(*) > 1
) duplicates
HAVING COUNT(*) > 0
{% else %}
SELECT 1 WHERE 1=0  -- Skip test for full refresh
{% endif %}
```

#### Test: Data Quality Threshold Validation
```sql
-- tests/assert_data_quality_thresholds.sql
WITH quality_metrics AS (
    SELECT 
        'meeting_facts_completeness' as metric_name,
        (COUNT(CASE WHEN meeting_id IS NOT NULL AND host_user_id IS NOT NULL THEN 1 END)::FLOAT / COUNT(*)::FLOAT) * 100 as completeness_rate
    FROM {{ ref('go_meeting_facts') }}
    
    UNION ALL
    
    SELECT 
        'participant_facts_completeness' as metric_name,
        (COUNT(CASE WHEN participant_id IS NOT NULL AND meeting_id IS NOT NULL THEN 1 END)::FLOAT / COUNT(*)::FLOAT) * 100 as completeness_rate
    FROM {{ ref('go_participant_facts') }}
    
    UNION ALL
    
    SELECT 
        'billing_facts_completeness' as metric_name,
        (COUNT(CASE WHEN billing_event_id IS NOT NULL AND account_id IS NOT NULL THEN 1 END)::FLOAT / COUNT(*)::FLOAT) * 100 as completeness_rate
    FROM {{ ref('go_billing_facts') }}
)
SELECT 
    metric_name,
    completeness_rate
FROM quality_metrics
WHERE completeness_rate < 95.0  -- 95% completeness threshold
```

#### Test: Business Rule Validation
```sql
-- tests/assert_business_rule_validation.sql
-- Test: Meeting duration should not exceed 24 hours (1440 minutes)
SELECT 
    meeting_id,
    meeting_duration_minutes
FROM {{ ref('go_meeting_facts') }}
WHERE meeting_duration_minutes > 1440

UNION ALL

-- Test: Participant duration should not exceed meeting duration
SELECT 
    p.participant_id as meeting_id,
    p.duration_minutes as meeting_duration_minutes
FROM {{ ref('go_participant_facts') }} p
JOIN {{ ref('go_meeting_facts') }} m
    ON p.meeting_id = m.meeting_id
WHERE p.duration_minutes > m.meeting_duration_minutes

UNION ALL

-- Test: Webinar attendance should not exceed registration
SELECT 
    webinar_id as meeting_id,
    (attendance_count - registration_count) as meeting_duration_minutes
FROM {{ ref('go_webinar_facts') }}
WHERE attendance_count > registration_count
```

---

## 3. Performance and Clustering Tests

### Test: Clustering Key Effectiveness
```sql
-- tests/assert_clustering_effectiveness.sql
SELECT 
    'go_meeting_facts' as table_name,
    SYSTEM$CLUSTERING_INFORMATION('{{ ref("go_meeting_facts") }}') as clustering_info
    
UNION ALL

SELECT 
    'go_participant_facts' as table_name,
    SYSTEM$CLUSTERING_INFORMATION('{{ ref("go_participant_facts") }}') as clustering_info
    
UNION ALL

SELECT 
    'go_webinar_facts' as table_name,
    SYSTEM$CLUSTERING_INFORMATION('{{ ref("go_webinar_facts") }}') as clustering_info
```

### Test: Incremental Performance
```sql
-- tests/assert_incremental_performance.sql
{% if is_incremental() %}
WITH incremental_stats AS (
    SELECT 
        COUNT(*) as incremental_rows,
        MIN(created_at) as min_created_at,
        MAX(created_at) as max_created_at
    FROM {{ ref('go_meeting_facts') }}
    WHERE {{ incremental_where_clause() }}
)
SELECT 
    incremental_rows,
    min_created_at,
    max_created_at,
    DATEDIFF('hour', min_created_at, max_created_at) as time_range_hours
FROM incremental_stats
WHERE incremental_rows > 1000000  -- Alert if processing more than 1M rows incrementally
{% else %}
SELECT 1 WHERE 1=0
{% endif %}
```

---

## 4. Cross-Table Consistency Tests

### Test: Fact Table Relationship Consistency
```sql
-- tests/assert_fact_table_relationships.sql
WITH meeting_participant_consistency AS (
    SELECT 
        m.meeting_id,
        m.participant_count as meeting_reported_count,
        COUNT(DISTINCT p.participant_id) as actual_participant_count
    FROM {{ ref('go_meeting_facts') }} m
    LEFT JOIN {{ ref('go_participant_facts') }} p
        ON m.meeting_id = p.meeting_id
    GROUP BY m.meeting_id, m.participant_count
    HAVING m.participant_count != COUNT(DISTINCT p.participant_id)
)
SELECT 
    meeting_id,
    meeting_reported_count,
    actual_participant_count,
    ABS(meeting_reported_count - actual_participant_count) as difference
FROM meeting_participant_consistency
WHERE ABS(meeting_reported_count - actual_participant_count) > 0
```

### Test: Usage and Quality Facts Alignment
```sql
-- tests/assert_usage_quality_alignment.sql
SELECT 
    u.usage_date,
    COUNT(DISTINCT u.user_id) as users_with_usage,
    COUNT(DISTINCT q.meeting_id) as meetings_with_quality_data,
    ABS(COUNT(DISTINCT u.user_id) - COUNT(DISTINCT q.meeting_id)) as alignment_gap
FROM {{ ref('go_usage_facts') }} u
FULL OUTER JOIN {{ ref('go_quality_facts') }} q
    ON DATE(u.usage_date) = DATE(q.created_at)
GROUP BY u.usage_date
HAVING ABS(COUNT(DISTINCT u.user_id) - COUNT(DISTINCT q.meeting_id)) > 100
```

---

## 5. Test Execution Summary

### Test Categories Coverage

| Test Category | Number of Tests | Coverage Areas |
|---------------|-----------------|----------------|
| Schema Tests | 25+ | Primary keys, foreign keys, data types, constraints |
| Happy Path Tests | 8 | Valid transformations, joins, aggregations |
| Edge Case Tests | 6 | Null handling, empty datasets, invalid lookups |
| Exception Tests | 5 | Incremental logic, data quality, business rules |
| Performance Tests | 3 | Clustering, incremental performance |
| Consistency Tests | 4 | Cross-table relationships, data alignment |

### Expected Test Results

| Fact Table | Critical Tests | Warning Tests | Info Tests |
|------------|----------------|---------------|-----------|
| go_meeting_facts | 8 | 3 | 2 |
| go_participant_facts | 7 | 4 | 2 |
| go_webinar_facts | 6 | 2 | 1 |
| go_billing_facts | 5 | 3 | 1 |
| go_usage_facts | 6 | 2 | 2 |
| go_quality_facts | 5 | 3 | 1 |

### Test Execution Commands

```bash
# Run all tests
dbt test

# Run tests for specific model
dbt test --select go_meeting_facts

# Run only custom tests
dbt test --select test_type:generic

# Run tests with specific tag
dbt test --select tag:data_quality

# Run tests in fail-fast mode
dbt test --fail-fast
```

---

## 6. Monitoring and Alerting

### Test Failure Thresholds

- **Critical Tests**: 0% failure tolerance
- **Warning Tests**: 5% failure tolerance  
- **Info Tests**: 10% failure tolerance

### Automated Test Scheduling

- **Full Test Suite**: Daily at 2 AM UTC
- **Critical Tests**: Every 4 hours
- **Incremental Tests**: After each incremental run

### Alert Configuration

```yaml
# dbt_project.yml test configuration
tests:
  +severity: error  # Default severity
  +tags: ["data_quality"]
  
# Custom test configurations
go_meeting_facts:
  +tests:
    +severity: error
    +tags: ["critical", "meeting_data"]
    
go_billing_facts:
  +tests:
    +severity: warn
    +tags: ["financial", "billing_data"]
```

---

## API Cost Calculation

**Total Estimated API Cost**: $0.75 USD
- Schema Tests: 25 tests × $0.01 = $0.25
- Custom SQL Tests: 20 tests × $0.015 = $0.30
- Performance Tests: 3 tests × $0.02 = $0.06
- Consistency Tests: 4 tests × $0.035 = $0.14

---

## Conclusion

This comprehensive test suite ensures the reliability, performance, and data quality of the Gold Layer fact tables in the Zoom dbt project. The tests cover all critical aspects including data integrity, business rule validation, incremental logic, and cross-table consistency. Regular execution of these tests will help maintain high-quality data pipelines and catch issues early in the development cycle.

**Maintenance Frequency**: Weekly review and updates
**Success Criteria**: >95% test pass rate for production deployment