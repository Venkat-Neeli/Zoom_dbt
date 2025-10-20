_____________________________________________
## *Author*: AAVA
## *Created on*: 2024-12-19   
## *Description*: Comprehensive unit test cases for Gold Layer fact tables in Snowflake dbt environment
## *Version*: 1 
## *Updated on*: 2024-12-19
_____________________________________________

# Snowflake dbt Unit Test Cases - Gold Layer Fact Tables

## Description

This document provides comprehensive unit test cases for validating the reliability, performance, and data quality of five Gold Layer fact tables in our Snowflake dbt environment:

- `go_meeting_facts`
- `go_participant_facts` 
- `go_webinar_facts`
- `go_billing_facts`
- `go_usage_facts`

These Gold Layer models transform Silver Layer data into business-ready fact tables using table materialization, applying `record_status = 'ACTIVE'` filtering, and including audit timestamps. The testing framework validates data transformations, business rules, edge cases, and cross-model integrations to ensure consistent and reliable data delivery.

## Instructions

### Analysis Requirements

1. **Data Transformation Validation**: Verify that all transformations from Silver to Gold layer maintain data integrity
2. **Business Rule Testing**: Ensure all business logic is correctly implemented
3. **Edge Case Handling**: Test boundary conditions and unusual data scenarios
4. **Performance Monitoring**: Validate query performance and resource utilization
5. **Cross-Model Integration**: Test relationships and dependencies between fact tables
6. **Data Quality Assurance**: Implement comprehensive data quality checks
7. **Error Handling**: Validate graceful handling of data anomalies

### Test Execution Guidelines

- Run tests in development environment before production deployment
- Execute full test suite during CI/CD pipeline
- Monitor test results and investigate failures immediately
- Update test cases when business requirements change
- Document test results and maintain test coverage metrics

## Test Case List

### Happy Path Scenarios

| Test Case ID | Test Case Description | Expected Outcome |
|--------------|----------------------|------------------|
| TC_GP_001 | Validate successful transformation of si_meetings to go_meeting_facts | All active meeting records transformed correctly with proper audit timestamps |
| TC_GP_002 | Validate successful transformation of si_participants to go_participant_facts | All active participant records transformed with correct aggregations |
| TC_GP_003 | Validate successful transformation of si_webinars to go_webinar_facts | All active webinar records transformed with proper metrics calculation |
| TC_GP_004 | Validate successful transformation of si_billing_events to go_billing_facts | All active billing records transformed with accurate financial calculations |
| TC_GP_005 | Validate successful transformation of si_feature_usage to go_usage_facts | All active usage records transformed with proper usage metrics |

### Edge Cases

| Test Case ID | Test Case Description | Expected Outcome |
|--------------|----------------------|------------------|
| TC_EC_001 | Test handling of null values in source data | Null values handled gracefully with appropriate defaults or exclusions |
| TC_EC_002 | Test processing of records with future dates | Future dated records processed according to business rules |
| TC_EC_003 | Test handling of duplicate records in source | Duplicates identified and handled per deduplication logic |
| TC_EC_004 | Test processing with zero or negative values | Invalid values handled according to business validation rules |
| TC_EC_005 | Test handling of extremely large datasets | Performance maintained within acceptable thresholds |

### Exception Cases

| Test Case ID | Test Case Description | Expected Outcome |
|--------------|----------------------|------------------|
| TC_EX_001 | Test behavior when source table is empty | Model completes successfully with empty result set |
| TC_EX_002 | Test handling of missing required columns | Model fails gracefully with descriptive error message |
| TC_EX_003 | Test processing with corrupted data types | Data type mismatches handled with appropriate error handling |
| TC_EX_004 | Test behavior during source table unavailability | Model fails with clear dependency error message |
| TC_EX_005 | Test handling of schema changes in source | Schema evolution handled according to defined policies |

### Data Quality Validation

| Test Case ID | Test Case Description | Expected Outcome |
|--------------|----------------------|------------------|
| TC_DQ_001 | Validate primary key uniqueness across all fact tables | No duplicate primary keys in any fact table |
| TC_DQ_002 | Validate referential integrity with dimension tables | All foreign keys have valid references |
| TC_DQ_003 | Validate data completeness and required field population | All mandatory fields populated according to business rules |
| TC_DQ_004 | Validate data accuracy through source-to-target reconciliation | Row counts and key metrics match between Silver and Gold layers |
| TC_DQ_005 | Validate data consistency across related fact tables | Cross-table aggregations and relationships are consistent |

### Business Rule Testing

| Test Case ID | Test Case Description | Expected Outcome |
|--------------|----------------------|------------------|
| TC_BR_001 | Validate meeting duration calculations in go_meeting_facts | Duration calculated correctly from start/end timestamps |
| TC_BR_002 | Validate participant engagement metrics in go_participant_facts | Engagement scores calculated per business logic |
| TC_BR_003 | Validate webinar attendance calculations in go_webinar_facts | Attendance metrics aggregated correctly |
| TC_BR_004 | Validate billing amount calculations in go_billing_facts | Financial calculations accurate with proper rounding |
| TC_BR_005 | Validate usage aggregations in go_usage_facts | Usage metrics summed and averaged correctly by time periods |

### Cross-Model Integration Tests

| Test Case ID | Test Case Description | Expected Outcome |
|--------------|----------------------|------------------|
| TC_CM_001 | Validate consistency between meeting and participant facts | Participant counts match between go_meeting_facts and go_participant_facts |
| TC_CM_002 | Validate billing and usage correlation | Usage patterns align with billing events where applicable |
| TC_CM_003 | Validate webinar and participant relationship | Webinar attendance matches participant webinar records |
| TC_CM_004 | Validate temporal consistency across all fact tables | Timestamp ranges and periods are consistent across models |
| TC_CM_005 | Validate aggregate consistency across fact tables | Summary metrics are consistent when aggregated across models |

### Performance Monitoring

| Test Case ID | Test Case Description | Expected Outcome |
|--------------|----------------------|------------------|
| TC_PM_001 | Validate query execution time for go_meeting_facts | Model completes within 5 minutes for standard dataset |
| TC_PM_002 | Validate memory usage during transformation | Memory consumption stays within allocated limits |
| TC_PM_003 | Validate incremental processing performance | Incremental runs complete within 2 minutes |
| TC_PM_004 | Validate concurrent execution capability | Multiple models can run simultaneously without conflicts |
| TC_PM_005 | Validate resource utilization efficiency | Warehouse utilization optimized for cost-effectiveness |

## dbt Test Scripts

### YAML-based Schema Tests

```yaml
version: 2

models:
  - name: go_meeting_facts
    description: "Gold layer fact table for meeting data"
    columns:
      - name: meeting_id
        description: "Primary key for meeting facts"
        tests:
          - unique
          - not_null
      - name: meeting_duration_minutes
        description: "Duration of meeting in minutes"
        tests:
          - not_null
          - dbt_utils.accepted_range:
              min_value: 0
              max_value: 1440  # 24 hours max
      - name: participant_count
        description: "Number of participants in meeting"
        tests:
          - not_null
          - dbt_utils.accepted_range:
              min_value: 1
              max_value: 1000
      - name: created_at
        description: "Record creation timestamp"
        tests:
          - not_null
      - name: updated_at
        description: "Record update timestamp"
        tests:
          - not_null

  - name: go_participant_facts
    description: "Gold layer fact table for participant data"
    columns:
      - name: participant_id
        description: "Primary key for participant facts"
        tests:
          - unique
          - not_null
      - name: meeting_id
        description: "Foreign key to meeting facts"
        tests:
          - not_null
          - relationships:
              to: ref('go_meeting_facts')
              field: meeting_id
      - name: join_duration_minutes
        description: "Duration participant was in meeting"
        tests:
          - not_null
          - dbt_utils.accepted_range:
              min_value: 0
              max_value: 1440
      - name: engagement_score
        description: "Participant engagement score"
        tests:
          - dbt_utils.accepted_range:
              min_value: 0
              max_value: 100
      - name: created_at
        tests:
          - not_null
      - name: updated_at
        tests:
          - not_null

  - name: go_webinar_facts
    description: "Gold layer fact table for webinar data"
    columns:
      - name: webinar_id
        description: "Primary key for webinar facts"
        tests:
          - unique
          - not_null
      - name: total_attendees
        description: "Total number of webinar attendees"
        tests:
          - not_null
          - dbt_utils.accepted_range:
              min_value: 0
              max_value: 10000
      - name: average_attendance_duration
        description: "Average attendance duration in minutes"
        tests:
          - dbt_utils.accepted_range:
              min_value: 0
              max_value: 480  # 8 hours max
      - name: webinar_duration_minutes
        description: "Total webinar duration"
        tests:
          - not_null
          - dbt_utils.accepted_range:
              min_value: 1
              max_value: 480
      - name: created_at
        tests:
          - not_null
      - name: updated_at
        tests:
          - not_null

  - name: go_billing_facts
    description: "Gold layer fact table for billing data"
    columns:
      - name: billing_id
        description: "Primary key for billing facts"
        tests:
          - unique
          - not_null
      - name: amount
        description: "Billing amount"
        tests:
          - not_null
          - dbt_utils.accepted_range:
              min_value: 0
              max_value: 1000000
      - name: currency_code
        description: "Currency code for billing"
        tests:
          - not_null
          - accepted_values:
              values: ['USD', 'EUR', 'GBP', 'CAD', 'AUD']
      - name: billing_date
        description: "Date of billing event"
        tests:
          - not_null
      - name: created_at
        tests:
          - not_null
      - name: updated_at
        tests:
          - not_null

  - name: go_usage_facts
    description: "Gold layer fact table for usage data"
    columns:
      - name: usage_id
        description: "Primary key for usage facts"
        tests:
          - unique
          - not_null
      - name: feature_name
        description: "Name of the feature used"
        tests:
          - not_null
      - name: usage_count
        description: "Number of times feature was used"
        tests:
          - not_null
          - dbt_utils.accepted_range:
              min_value: 0
              max_value: 1000000
      - name: usage_duration_seconds
        description: "Duration of feature usage in seconds"
        tests:
          - dbt_utils.accepted_range:
              min_value: 0
              max_value: 86400  # 24 hours max
      - name: usage_date
        description: "Date of usage event"
        tests:
          - not_null
      - name: created_at
        tests:
          - not_null
      - name: updated_at
        tests:
          - not_null
```

### Custom SQL-based dbt Tests

#### Test: Data Completeness Validation

```sql
-- tests/assert_meeting_facts_completeness.sql
-- Validates that all active meetings from silver layer are present in gold layer

select count(*) as missing_records
from (
    select meeting_id
    from {{ ref('si_meetings') }}
    where record_status = 'ACTIVE'
    
    except
    
    select meeting_id
    from {{ ref('go_meeting_facts') }}
)

having count(*) > 0
```

#### Test: Business Rule Validation - Meeting Duration

```sql
-- tests/assert_meeting_duration_calculation.sql
-- Validates meeting duration calculation accuracy

with duration_check as (
    select 
        meeting_id,
        meeting_duration_minutes,
        datediff('minute', meeting_start_time, meeting_end_time) as calculated_duration
    from {{ ref('go_meeting_facts') }}
    where meeting_start_time is not null 
      and meeting_end_time is not null
)

select count(*) as invalid_durations
from duration_check
where abs(meeting_duration_minutes - calculated_duration) > 1  -- Allow 1 minute tolerance

having count(*) > 0
```

#### Test: Cross-Model Consistency

```sql
-- tests/assert_meeting_participant_consistency.sql
-- Validates participant count consistency between meeting and participant facts

with meeting_participant_counts as (
    select 
        m.meeting_id,
        m.participant_count as meeting_fact_count,
        count(p.participant_id) as actual_participant_count
    from {{ ref('go_meeting_facts') }} m
    left join {{ ref('go_participant_facts') }} p
        on m.meeting_id = p.meeting_id
    group by m.meeting_id, m.participant_count
)

select count(*) as inconsistent_counts
from meeting_participant_counts
where meeting_fact_count != actual_participant_count

having count(*) > 0
```

#### Test: Data Quality - No Future Dates

```sql
-- tests/assert_no_future_billing_dates.sql
-- Ensures no billing events have future dates

select count(*) as future_billing_records
from {{ ref('go_billing_facts') }}
where billing_date > current_date()

having count(*) > 0
```

#### Test: Performance Validation

```sql
-- tests/assert_reasonable_processing_time.sql
-- Validates that model processing completes within reasonable time
-- This would typically be implemented as a macro or external monitoring

select 
    case 
        when count(*) > 1000000 then 1  -- Flag if processing very large dataset
        else 0
    end as performance_flag
from {{ ref('go_meeting_facts') }}

having performance_flag > 0
```

#### Test: Referential Integrity

```sql
-- tests/assert_webinar_participant_integrity.sql
-- Validates that webinar participants exist in participant facts

select count(*) as orphaned_webinar_participants
from (
    select distinct participant_id
    from {{ ref('go_webinar_facts') }} w
    join {{ ref('si_participants') }} sp
        on w.webinar_id = sp.webinar_id
    where sp.record_status = 'ACTIVE'
    
    except
    
    select participant_id
    from {{ ref('go_participant_facts') }}
)

having count(*) > 0
```

#### Test: Data Freshness Validation

```sql
-- tests/assert_data_freshness.sql
-- Validates that data is being updated regularly

select count(*) as stale_records
from (
    select 'go_meeting_facts' as table_name, max(updated_at) as last_update
    from {{ ref('go_meeting_facts') }}
    
    union all
    
    select 'go_participant_facts' as table_name, max(updated_at) as last_update
    from {{ ref('go_participant_facts') }}
    
    union all
    
    select 'go_webinar_facts' as table_name, max(updated_at) as last_update
    from {{ ref('go_webinar_facts') }}
    
    union all
    
    select 'go_billing_facts' as table_name, max(updated_at) as last_update
    from {{ ref('go_billing_facts') }}
    
    union all
    
    select 'go_usage_facts' as table_name, max(updated_at) as last_update
    from {{ ref('go_usage_facts') }}
)
where last_update < dateadd('hour', -24, current_timestamp())

having count(*) > 0
```

#### Test: Usage Facts Aggregation Validation

```sql
-- tests/assert_usage_aggregation_accuracy.sql
-- Validates usage aggregation calculations

with usage_validation as (
    select 
        feature_name,
        usage_date,
        sum(usage_count) as total_usage_count,
        avg(usage_duration_seconds) as avg_duration
    from {{ ref('go_usage_facts') }}
    group by feature_name, usage_date
    having sum(usage_count) < 0 
        or avg(usage_duration_seconds) < 0
        or sum(usage_count) is null
)

select count(*) as invalid_aggregations
from usage_validation

having count(*) > 0
```

#### Test: Billing Facts Currency Consistency

```sql
-- tests/assert_billing_currency_consistency.sql
-- Validates currency consistency and amount calculations

select count(*) as invalid_billing_records
from {{ ref('go_billing_facts') }}
where (currency_code is null and amount > 0)
   or (amount < 0)
   or (amount is null)
   or (currency_code not in ('USD', 'EUR', 'GBP', 'CAD', 'AUD'))

having count(*) > 0
```

### dbt Test Configuration

```yaml
# dbt_project.yml test configuration
tests:
  +store_failures: true
  +severity: 'error'
  
models:
  your_project:
    gold:
      +materialized: table
      +tests:
        - dbt_utils.recency:
            datepart: hour
            field: updated_at
            interval: 24
```

## API Cost Calculation

### Snowflake Compute Costs

| Test Category | Estimated Warehouse Time (seconds) | Cost per Credit | Estimated Cost (USD) |
|---------------|-----------------------------------|-----------------|---------------------|
| Schema Tests | 120 | $2.00 | $0.067 |
| Custom SQL Tests | 300 | $2.00 | $0.167 |
| Performance Tests | 180 | $2.00 | $0.100 |
| Cross-Model Tests | 240 | $2.00 | $0.133 |
| **Total per Test Run** | **840** | **$2.00** | **$0.467** |

### Monthly Cost Estimation

| Frequency | Runs per Month | Monthly Cost (USD) |
|-----------|----------------|-------------------|
| Daily CI/CD | 30 | $14.01 |
| Weekly Full Suite | 4 | $1.87 |
| Ad-hoc Testing | 10 | $4.67 |
| **Total Monthly Cost** | **44** | **$20.55** |

### Cost Optimization Recommendations

1. **Warehouse Sizing**: Use XS warehouse for most tests, scale up only for performance testing
2. **Test Scheduling**: Run comprehensive tests during off-peak hours
3. **Incremental Testing**: Focus on changed models during development
4. **Test Parallelization**: Group related tests to minimize warehouse startup costs
5. **Result Caching**: Leverage Snowflake's result caching for repeated test patterns

## Conclusion

This comprehensive testing framework ensures the reliability, performance, and data quality of our Gold Layer fact tables. Regular execution of these tests will:

- Catch data quality issues early in the development cycle
- Validate business rule implementations
- Ensure cross-model consistency and integrity
- Monitor performance and resource utilization
- Provide confidence in production deployments

The estimated monthly cost of $20.55 provides significant value in preventing production issues and maintaining data trust across the organization.

---

**Document Control**
- Next Review Date: 2025-01-19
- Approval Required: Data Engineering Team Lead
- Distribution: Data Engineering Team, QA Team, Business Stakeholders