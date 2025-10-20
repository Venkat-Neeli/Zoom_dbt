# Snowflake dbt Unit Test Cases - Zoom Gold Layer Fact Tables

## Metadata
- **Author**: AAVA
- **Version**: 2.0
- **Creation Date**: 2024-12-19
- **Last Updated**: 2024-12-19
- **Description**: Comprehensive unit test cases for Zoom Customer Analytics Gold Layer fact tables in Snowflake using dbt. Covers data transformations, business rules, edge cases, and error handling scenarios.
- **Models Covered**: go_meeting_facts, go_participant_facts, go_webinar_facts, go_billing_facts, go_usage_facts, go_quality_facts

## Overview

This document provides comprehensive unit test cases for the Zoom Gold Layer fact tables built with dbt in Snowflake. The tests validate data transformations, business logic, data quality, and edge case handling across all fact table models.

## Test Framework Structure

### Test Categories
1. **Data Quality Tests** - Basic validation (not_null, unique)
2. **Business Rule Tests** - Domain-specific validation
3. **Referential Integrity Tests** - Foreign key relationships
4. **Edge Case Tests** - Boundary conditions and exceptions
5. **Performance Tests** - Data volume and efficiency validation
6. **Custom SQL Tests** - Complex business logic validation

---

## 1. GO_MEETING_FACTS Tests

### 1.1 Data Quality Tests

```yaml
# tests/go_meeting_facts_tests.yml
version: 2

models:
  - name: go_meeting_facts
    description: "Meeting facts table with comprehensive test coverage"
    tests:
      - dbt_utils.row_count:
          above: 0
    columns:
      - name: meeting_id
        description: "Unique meeting identifier"
        tests:
          - not_null
          - unique
      - name: meeting_start_time
        description: "Meeting start timestamp"
        tests:
          - not_null
          - dbt_utils.expression_is_true:
              expression: "meeting_start_time <= current_timestamp()"
      - name: meeting_duration_minutes
        description: "Meeting duration in minutes"
        tests:
          - not_null
          - dbt_utils.expression_is_true:
              expression: "meeting_duration_minutes >= 0"
          - dbt_utils.expression_is_true:
              expression: "meeting_duration_minutes <= 1440" # Max 24 hours
      - name: participant_count
        description: "Number of participants"
        tests:
          - not_null
          - dbt_utils.expression_is_true:
              expression: "participant_count >= 1"
      - name: meeting_status
        description: "Meeting status"
        tests:
          - not_null
          - accepted_values:
              values: ['completed', 'in_progress', 'cancelled', 'scheduled']
```

### 1.2 Custom SQL Tests for GO_MEETING_FACTS

```sql
-- tests/go_meeting_facts_business_rules.sql
-- Test: Meeting end time should be after start time
select meeting_id
from {{ ref('go_meeting_facts') }}
where meeting_end_time <= meeting_start_time
  and meeting_status = 'completed'
```

```sql
-- tests/go_meeting_facts_duration_consistency.sql
-- Test: Duration calculation consistency
select meeting_id
from {{ ref('go_meeting_facts') }}
where abs(
    meeting_duration_minutes - 
    datediff('minute', meeting_start_time, meeting_end_time)
) > 1 -- Allow 1 minute tolerance
and meeting_status = 'completed'
```

```sql
-- tests/go_meeting_facts_future_meetings.sql
-- Test: No completed meetings in the future
select meeting_id
from {{ ref('go_meeting_facts') }}
where meeting_start_time > current_timestamp()
  and meeting_status = 'completed'
```

---

## 2. GO_PARTICIPANT_FACTS Tests

### 2.1 Data Quality Tests

```yaml
# tests/go_participant_facts_tests.yml
version: 2

models:
  - name: go_participant_facts
    description: "Participant facts table with comprehensive test coverage"
    columns:
      - name: participant_id
        tests:
          - not_null
          - unique
      - name: meeting_id
        tests:
          - not_null
          - relationships:
              to: ref('go_meeting_facts')
              field: meeting_id
      - name: join_time
        tests:
          - not_null
      - name: leave_time
        tests:
          - dbt_utils.expression_is_true:
              expression: "leave_time >= join_time or leave_time is null"
      - name: duration_minutes
        tests:
          - dbt_utils.expression_is_true:
              expression: "duration_minutes >= 0 or duration_minutes is null"
      - name: participant_role
        tests:
          - accepted_values:
              values: ['host', 'co_host', 'participant', 'panelist']
```

### 2.2 Custom SQL Tests for GO_PARTICIPANT_FACTS

```sql
-- tests/go_participant_facts_host_validation.sql
-- Test: Each meeting should have at least one host
select meeting_id
from {{ ref('go_participant_facts') }}
group by meeting_id
having sum(case when participant_role = 'host' then 1 else 0 end) = 0
```

```sql
-- tests/go_participant_facts_duration_validation.sql
-- Test: Participant duration should not exceed meeting duration
select p.participant_id
from {{ ref('go_participant_facts') }} p
join {{ ref('go_meeting_facts') }} m on p.meeting_id = m.meeting_id
where p.duration_minutes > m.meeting_duration_minutes + 5 -- 5 minute tolerance
```

---

## 3. GO_WEBINAR_FACTS Tests

### 3.1 Data Quality Tests

```yaml
# tests/go_webinar_facts_tests.yml
version: 2

models:
  - name: go_webinar_facts
    columns:
      - name: webinar_id
        tests:
          - not_null
          - unique
      - name: webinar_topic
        tests:
          - not_null
      - name: scheduled_start_time
        tests:
          - not_null
      - name: actual_start_time
        tests:
          - dbt_utils.expression_is_true:
              expression: "actual_start_time >= scheduled_start_time - interval '30 minutes' or actual_start_time is null"
      - name: attendee_count
        tests:
          - dbt_utils.expression_is_true:
              expression: "attendee_count >= 0"
      - name: registration_count
        tests:
          - dbt_utils.expression_is_true:
              expression: "registration_count >= attendee_count or registration_count is null"
      - name: webinar_type
        tests:
          - accepted_values:
              values: ['live', 'recorded', 'hybrid']
```

### 3.2 Custom SQL Tests for GO_WEBINAR_FACTS

```sql
-- tests/go_webinar_facts_attendance_rate.sql
-- Test: Attendance rate should be reasonable (0-100%)
select webinar_id
from {{ ref('go_webinar_facts') }}
where (attendee_count::float / nullif(registration_count, 0)) > 1.1 -- Allow 10% over-registration
   or (attendee_count::float / nullif(registration_count, 0)) < 0
```

---

## 4. GO_BILLING_FACTS Tests

### 4.1 Data Quality Tests

```yaml
# tests/go_billing_facts_tests.yml
version: 2

models:
  - name: go_billing_facts
    columns:
      - name: billing_id
        tests:
          - not_null
          - unique
      - name: account_id
        tests:
          - not_null
      - name: billing_period_start
        tests:
          - not_null
      - name: billing_period_end
        tests:
          - not_null
          - dbt_utils.expression_is_true:
              expression: "billing_period_end > billing_period_start"
      - name: total_amount
        tests:
          - not_null
          - dbt_utils.expression_is_true:
              expression: "total_amount >= 0"
      - name: currency_code
        tests:
          - not_null
          - accepted_values:
              values: ['USD', 'EUR', 'GBP', 'JPY', 'CAD', 'AUD']
      - name: payment_status
        tests:
          - accepted_values:
              values: ['paid', 'pending', 'failed', 'refunded']
```

### 4.2 Custom SQL Tests for GO_BILLING_FACTS

```sql
-- tests/go_billing_facts_amount_validation.sql
-- Test: Total amount should equal sum of line items
select billing_id
from {{ ref('go_billing_facts') }}
where abs(total_amount - (base_amount + tax_amount + discount_amount)) > 0.01
```

```sql
-- tests/go_billing_facts_period_validation.sql
-- Test: Billing periods should not overlap for same account
select b1.billing_id
from {{ ref('go_billing_facts') }} b1
join {{ ref('go_billing_facts') }} b2 
  on b1.account_id = b2.account_id 
  and b1.billing_id != b2.billing_id
where b1.billing_period_start < b2.billing_period_end
  and b1.billing_period_end > b2.billing_period_start
```

---

## 5. GO_USAGE_FACTS Tests

### 5.1 Data Quality Tests

```yaml
# tests/go_usage_facts_tests.yml
version: 2

models:
  - name: go_usage_facts
    columns:
      - name: usage_id
        tests:
          - not_null
          - unique
      - name: account_id
        tests:
          - not_null
      - name: usage_date
        tests:
          - not_null
          - dbt_utils.expression_is_true:
              expression: "usage_date <= current_date()"
      - name: meeting_minutes
        tests:
          - dbt_utils.expression_is_true:
              expression: "meeting_minutes >= 0"
      - name: webinar_minutes
        tests:
          - dbt_utils.expression_is_true:
              expression: "webinar_minutes >= 0"
      - name: storage_gb
        tests:
          - dbt_utils.expression_is_true:
              expression: "storage_gb >= 0"
      - name: license_type
        tests:
          - accepted_values:
              values: ['basic', 'pro', 'business', 'enterprise']
```

### 5.2 Custom SQL Tests for GO_USAGE_FACTS

```sql
-- tests/go_usage_facts_daily_aggregation.sql
-- Test: Only one usage record per account per day
select account_id, usage_date
from {{ ref('go_usage_facts') }}
group by account_id, usage_date
having count(*) > 1
```

```sql
-- tests/go_usage_facts_reasonable_limits.sql
-- Test: Usage should be within reasonable limits
select usage_id
from {{ ref('go_usage_facts') }}
where meeting_minutes > 10080 -- More than 7 days worth of minutes
   or webinar_minutes > 10080
   or storage_gb > 10000 -- More than 10TB
```

---

## 6. GO_QUALITY_FACTS Tests

### 6.1 Data Quality Tests

```yaml
# tests/go_quality_facts_tests.yml
version: 2

models:
  - name: go_quality_facts
    columns:
      - name: quality_id
        tests:
          - not_null
          - unique
      - name: meeting_id
        tests:
          - not_null
          - relationships:
              to: ref('go_meeting_facts')
              field: meeting_id
      - name: audio_quality_score
        tests:
          - dbt_utils.expression_is_true:
              expression: "audio_quality_score between 0 and 100 or audio_quality_score is null"
      - name: video_quality_score
        tests:
          - dbt_utils.expression_is_true:
              expression: "video_quality_score between 0 and 100 or video_quality_score is null"
      - name: network_quality_score
        tests:
          - dbt_utils.expression_is_true:
              expression: "network_quality_score between 0 and 100 or network_quality_score is null"
      - name: overall_quality_rating
        tests:
          - accepted_values:
              values: ['excellent', 'good', 'fair', 'poor']
```

### 6.2 Custom SQL Tests for GO_QUALITY_FACTS

```sql
-- tests/go_quality_facts_rating_consistency.sql
-- Test: Overall rating should be consistent with individual scores
select quality_id
from {{ ref('go_quality_facts') }}
where (
    (audio_quality_score + video_quality_score + network_quality_score) / 3 >= 80
    and overall_quality_rating not in ('excellent', 'good')
) or (
    (audio_quality_score + video_quality_score + network_quality_score) / 3 < 50
    and overall_quality_rating not in ('poor', 'fair')
)
```

---

## 7. Cross-Model Integration Tests

### 7.1 Data Consistency Tests

```sql
-- tests/cross_model_participant_meeting_consistency.sql
-- Test: Participant count in meetings should match participant facts
select m.meeting_id
from {{ ref('go_meeting_facts') }} m
left join (
    select meeting_id, count(*) as actual_participant_count
    from {{ ref('go_participant_facts') }}
    group by meeting_id
) p on m.meeting_id = p.meeting_id
where m.participant_count != coalesce(p.actual_participant_count, 0)
```

```sql
-- tests/cross_model_usage_billing_consistency.sql
-- Test: Usage data should exist for billed accounts
select distinct b.account_id
from {{ ref('go_billing_facts') }} b
left join {{ ref('go_usage_facts') }} u 
  on b.account_id = u.account_id
  and u.usage_date between b.billing_period_start and b.billing_period_end
where u.account_id is null
  and b.payment_status = 'paid'
```

### 7.2 Performance Tests

```sql
-- tests/performance_large_meetings.sql
-- Test: Identify meetings with unusually high participant counts
select meeting_id, participant_count
from {{ ref('go_meeting_facts') }}
where participant_count > 1000
```

---

## 8. Edge Case and Error Handling Tests

### 8.1 Null Value Handling

```sql
-- tests/edge_case_null_handling.sql
-- Test: Critical fields should never be null
select 'go_meeting_facts' as table_name, meeting_id as record_id
from {{ ref('go_meeting_facts') }}
where meeting_id is null or meeting_start_time is null

union all

select 'go_billing_facts' as table_name, billing_id as record_id
from {{ ref('go_billing_facts') }}
where billing_id is null or total_amount is null
```

### 8.2 Data Type Validation

```sql
-- tests/edge_case_data_types.sql
-- Test: Ensure numeric fields contain valid numbers
select meeting_id
from {{ ref('go_meeting_facts') }}
where try_cast(meeting_duration_minutes as number) is null
  and meeting_duration_minutes is not null
```

### 8.3 Boundary Value Tests

```sql
-- tests/edge_case_boundary_values.sql
-- Test: Check for extreme values that might indicate data issues
select 
    'meeting_duration' as metric,
    meeting_id as record_id,
    meeting_duration_minutes as value
from {{ ref('go_meeting_facts') }}
where meeting_duration_minutes > 480 -- More than 8 hours
   or meeting_duration_minutes < 0

union all

select 
    'billing_amount' as metric,
    billing_id as record_id,
    total_amount as value
from {{ ref('go_billing_facts') }}
where total_amount > 100000 -- More than $100k
   or total_amount < 0
```

---

## 9. Test Execution Configuration

### 9.1 dbt_project.yml Configuration

```yaml
# Add to dbt_project.yml
tests:
  zoom_analytics:
    +severity: error
    +store_failures: true
    +schema: test_results
    
    # Custom test configurations
    go_meeting_facts_tests:
      +severity: warn
    
    cross_model_tests:
      +severity: error
    
    edge_case_tests:
      +severity: warn
```

### 9.2 Test Execution Commands

```bash
# Run all tests
dbt test

# Run tests for specific model
dbt test --select go_meeting_facts

# Run only custom SQL tests
dbt test --select test_type:generic

# Run tests with increased verbosity
dbt test --store-failures

# Run tests in parallel
dbt test --threads 4
```

---

## 10. Test Monitoring and Alerting

### 10.1 Test Results Monitoring

```sql
-- Query to monitor test results
select 
    test_name,
    model_name,
    status,
    execution_time,
    failures,
    run_started_at
from test_results.test_execution_summary
where run_started_at >= current_date - 7
order by run_started_at desc;
```

### 10.2 Automated Test Scheduling

```yaml
# GitHub Actions workflow for automated testing
name: dbt-tests
on:
  schedule:
    - cron: '0 6 * * *'  # Daily at 6 AM
  push:
    branches: [main, mapping_modelling_data]

jobs:
  test:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v2
      - name: Setup dbt
        run: pip install dbt-snowflake
      - name: Run dbt tests
        run: |
          dbt deps
          dbt test --store-failures
```

---

## 11. Best Practices and Guidelines

### 11.1 Test Development Guidelines

1. **Comprehensive Coverage**: Ensure all critical business rules are tested
2. **Performance Consideration**: Avoid tests that scan entire large tables
3. **Maintainability**: Keep tests simple and well-documented
4. **Error Messages**: Provide clear, actionable error messages
5. **Test Data**: Use representative test data that covers edge cases

### 11.2 Test Maintenance

1. **Regular Review**: Review and update tests quarterly
2. **Performance Monitoring**: Monitor test execution times
3. **False Positive Management**: Address tests that fail due to data variations
4. **Documentation**: Keep test documentation up to date

### 11.3 Troubleshooting Common Issues

```sql
-- Debug failing tests
select *
from {{ ref('go_meeting_facts') }}
where meeting_duration_minutes < 0
limit 10;

-- Check data freshness
select 
    max(meeting_start_time) as latest_meeting,
    current_timestamp() as current_time,
    datediff('hour', max(meeting_start_time), current_timestamp()) as hours_behind
from {{ ref('go_meeting_facts') }};
```

---

## Conclusion

This comprehensive test suite provides robust validation for the Zoom Gold Layer fact tables, ensuring data quality, business rule compliance, and system reliability. Regular execution of these tests will help maintain high data quality standards and catch issues early in the development cycle.

### Next Steps

1. Implement the test cases in your dbt project
2. Configure automated test execution
3. Set up monitoring and alerting for test failures
4. Establish a regular review cycle for test maintenance
5. Expand test coverage as new business requirements emerge

**Remember**: These tests are living documents that should evolve with your data models and business requirements. Regular review and updates ensure continued effectiveness in maintaining data quality.