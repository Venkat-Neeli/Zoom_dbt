---
**Author:** AAVA  
**Created on:** 2024-12-19  
**Description:** Comprehensive unit test cases for Snowflake dbt fact table models covering transformations, business rules, and edge cases  
**Version:** 1  
**Updated on:** 2024-12-19  
---

# Snowflake dbt Unit Test Case - Fact Table Models

## Overview

This document provides comprehensive unit test cases for 6 dbt fact table models that transform Silver layer data into Gold layer fact tables. The models include complex business logic, engagement scores, attendance metrics, data quality filtering, and audit trails.

### Models Under Test:
1. `go_meeting_facts.sql`
2. `go_participant_facts.sql` 
3. `go_webinar_facts.sql`
4. `go_billing_facts.sql`
5. `go_usage_facts.sql`
6. `go_quality_facts.sql`

## Test Case List

### go_meeting_facts.sql Test Cases

| Test Case ID | Test Case Description | Expected Outcome |
|--------------|----------------------|------------------|
| GMF-001 | Validate meeting duration calculation using DATEDIFF | Duration calculated correctly in minutes |
| GMF-002 | Test timezone conversion with CONVERT_TIMEZONE | Meeting times converted to UTC properly |
| GMF-003 | Verify engagement score calculation logic | Scores between 0-100 based on participation metrics |
| GMF-004 | Test data quality filtering (record_status='ACTIVE') | Only active records included |
| GMF-005 | Validate data_quality_score >= 0.7 filter | Records with quality score < 0.7 excluded |
| GMF-006 | Test NULL meeting_id handling | Records with NULL meeting_id rejected |
| GMF-007 | Verify audit trail UUID_STRING generation | Unique audit IDs generated for each record |
| GMF-008 | Test empty dataset handling | Model handles empty input gracefully |
| GMF-009 | Validate meeting aggregation logic | Correct sum/avg/count aggregations |
| GMF-010 | Test schema mismatch scenarios | Model fails gracefully with clear error |

### go_participant_facts.sql Test Cases

| Test Case ID | Test Case Description | Expected Outcome |
|--------------|----------------------|------------------|
| GPF-001 | Validate participant attendance calculation | Attendance percentage calculated correctly |
| GPF-002 | Test participant engagement metrics | Engagement scores properly aggregated |
| GPF-003 | Verify participant-meeting relationship joins | All valid relationships maintained |
| GPF-004 | Test NULL participant_id handling | Records with NULL participant_id excluded |
| GPF-005 | Validate duplicate participant detection | Duplicates handled per business rules |
| GPF-006 | Test participant status filtering | Only valid participant statuses included |
| GPF-007 | Verify participant role assignments | Roles mapped correctly from source |
| GPF-008 | Test cross-meeting participant analysis | Participants tracked across multiple meetings |
| GPF-009 | Validate participant time zone handling | Time calculations respect participant timezone |
| GPF-010 | Test invalid participant lookup scenarios | Invalid lookups handled gracefully |

### go_webinar_facts.sql Test Cases

| Test Case ID | Test Case Description | Expected Outcome |
|--------------|----------------------|------------------|
| GWF-001 | Validate webinar attendance tracking | Attendance numbers accurate |
| GWF-002 | Test webinar engagement calculations | Engagement metrics calculated properly |
| GWF-003 | Verify webinar duration and timing | Duration and schedule data accurate |
| GWF-004 | Test webinar registration vs attendance | Registration/attendance ratios calculated |
| GWF-005 | Validate webinar quality metrics | Quality scores aggregated correctly |
| GWF-006 | Test NULL webinar_id scenarios | NULL webinar IDs handled appropriately |
| GWF-007 | Verify webinar feature usage tracking | Feature usage properly attributed |
| GWF-008 | Test webinar capacity calculations | Capacity metrics calculated accurately |
| GWF-009 | Validate webinar recording metrics | Recording statistics tracked properly |
| GWF-010 | Test webinar cancellation handling | Cancelled webinars processed correctly |

### go_billing_facts.sql Test Cases

| Test Case ID | Test Case Description | Expected Outcome |
|--------------|----------------------|------------------|
| GBF-001 | Validate billing amount calculations | Amounts calculated with proper precision |
| GBF-002 | Test currency conversion logic | Multi-currency handling accurate |
| GBF-003 | Verify billing period aggregations | Periods aggregated correctly |
| GBF-004 | Test billing status filtering | Only valid billing statuses included |
| GBF-005 | Validate tax calculations | Tax amounts calculated properly |
| GBF-006 | Test NULL billing_id handling | NULL billing IDs excluded |
| GBF-007 | Verify discount applications | Discounts applied correctly |
| GBF-008 | Test billing reconciliation logic | Billing records reconciled properly |
| GBF-009 | Validate refund processing | Refunds processed and tracked |
| GBF-010 | Test billing audit trail | Complete audit trail maintained |

### go_usage_facts.sql Test Cases

| Test Case ID | Test Case Description | Expected Outcome |
|--------------|----------------------|------------------|
| GUF-001 | Validate feature usage aggregations | Usage metrics aggregated correctly |
| GUF-002 | Test usage time calculations | Time-based usage calculated properly |
| GUF-003 | Verify user-feature relationship joins | Valid relationships maintained |
| GUF-004 | Test usage threshold calculations | Thresholds calculated accurately |
| GUF-005 | Validate usage trend analysis | Trends calculated over time periods |
| GUF-006 | Test NULL usage_id scenarios | NULL usage IDs handled properly |
| GUF-007 | Verify feature adoption metrics | Adoption rates calculated correctly |
| GUF-008 | Test usage quota calculations | Quotas and limits tracked accurately |
| GUF-009 | Validate usage billing integration | Usage data integrated with billing |
| GUF-010 | Test usage anomaly detection | Anomalous usage patterns flagged |

### go_quality_facts.sql Test Cases

| Test Case ID | Test Case Description | Expected Outcome |
|--------------|----------------------|------------------|
| GQF-001 | Validate data quality score calculations | Quality scores calculated correctly |
| GQF-002 | Test quality threshold filtering | Records below threshold excluded |
| GQF-003 | Verify quality metric aggregations | Quality metrics properly aggregated |
| GQF-004 | Test quality trend analysis | Quality trends tracked over time |
| GQF-005 | Validate quality rule applications | Business rules applied consistently |
| GQF-006 | Test NULL quality_id handling | NULL quality IDs processed correctly |
| GQF-007 | Verify quality dimension analysis | Quality analyzed across dimensions |
| GQF-008 | Test quality improvement tracking | Improvements tracked and measured |
| GQF-009 | Validate quality reporting metrics | Reporting metrics calculated accurately |
| GQF-010 | Test quality exception handling | Quality exceptions handled properly |

## dbt Test Scripts

### Schema Tests (schema.yml)

```yaml
version: 2

models:
  - name: go_meeting_facts
    description: "Gold layer fact table for meeting analytics"
    columns:
      - name: meeting_id
        description: "Unique identifier for meetings"
        tests:
          - unique
          - not_null
      - name: meeting_duration_minutes
        description: "Meeting duration in minutes"
        tests:
          - not_null
          - expression_is_true:
              expression: ">= 0"
      - name: engagement_score
        description: "Meeting engagement score (0-100)"
        tests:
          - not_null
          - expression_is_true:
              expression: ">= 0 AND <= 100"
      - name: record_status
        description: "Record status indicator"
        tests:
          - not_null
          - accepted_values:
              values: ['ACTIVE', 'INACTIVE', 'DELETED']
      - name: data_quality_score
        description: "Data quality score (0.0-1.0)"
        tests:
          - not_null
          - expression_is_true:
              expression: ">= 0.0 AND <= 1.0"
      - name: audit_id
        description: "Audit trail identifier"
        tests:
          - unique
          - not_null

  - name: go_participant_facts
    description: "Gold layer fact table for participant analytics"
    columns:
      - name: participant_id
        description: "Unique identifier for participants"
        tests:
          - unique
          - not_null
      - name: meeting_id
        description: "Foreign key to meetings"
        tests:
          - not_null
          - relationships:
              to: ref('go_meeting_facts')
              field: meeting_id
      - name: attendance_percentage
        description: "Participant attendance percentage"
        tests:
          - expression_is_true:
              expression: ">= 0 AND <= 100"
      - name: engagement_score
        description: "Participant engagement score"
        tests:
          - expression_is_true:
              expression: ">= 0 AND <= 100"

  - name: go_webinar_facts
    description: "Gold layer fact table for webinar analytics"
    columns:
      - name: webinar_id
        description: "Unique identifier for webinars"
        tests:
          - unique
          - not_null
      - name: registration_count
        description: "Number of registrations"
        tests:
          - not_null
          - expression_is_true:
              expression: ">= 0"
      - name: attendance_count
        description: "Number of attendees"
        tests:
          - not_null
          - expression_is_true:
              expression: ">= 0"
      - name: attendance_rate
        description: "Attendance rate percentage"
        tests:
          - expression_is_true:
              expression: ">= 0 AND <= 100"

  - name: go_billing_facts
    description: "Gold layer fact table for billing analytics"
    columns:
      - name: billing_id
        description: "Unique identifier for billing records"
        tests:
          - unique
          - not_null
      - name: billing_amount
        description: "Billing amount"
        tests:
          - not_null
          - expression_is_true:
              expression: ">= 0"
      - name: currency_code
        description: "Currency code"
        tests:
          - not_null
          - accepted_values:
              values: ['USD', 'EUR', 'GBP', 'CAD', 'AUD']
      - name: billing_status
        description: "Billing status"
        tests:
          - not_null
          - accepted_values:
              values: ['PENDING', 'PAID', 'OVERDUE', 'CANCELLED']

  - name: go_usage_facts
    description: "Gold layer fact table for usage analytics"
    columns:
      - name: usage_id
        description: "Unique identifier for usage records"
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
          - expression_is_true:
              expression: ">= 0"
      - name: usage_duration_minutes
        description: "Duration of feature usage in minutes"
        tests:
          - expression_is_true:
              expression: ">= 0"

  - name: go_quality_facts
    description: "Gold layer fact table for data quality analytics"
    columns:
      - name: quality_id
        description: "Unique identifier for quality records"
        tests:
          - unique
          - not_null
      - name: quality_score
        description: "Overall quality score"
        tests:
          - not_null
          - expression_is_true:
              expression: ">= 0.0 AND <= 1.0"
      - name: completeness_score
        description: "Data completeness score"
        tests:
          - expression_is_true:
              expression: ">= 0.0 AND <= 1.0"
      - name: accuracy_score
        description: "Data accuracy score"
        tests:
          - expression_is_true:
              expression: ">= 0.0 AND <= 1.0"
```

### Custom SQL-based dbt Tests

#### Test: Meeting Duration Consistency
```sql
-- tests/meeting_duration_consistency.sql
SELECT 
    meeting_id,
    meeting_start_time,
    meeting_end_time,
    meeting_duration_minutes,
    DATEDIFF('minute', meeting_start_time, meeting_end_time) AS calculated_duration
FROM {{ ref('go_meeting_facts') }}
WHERE meeting_duration_minutes != DATEDIFF('minute', meeting_start_time, meeting_end_time)
   OR meeting_duration_minutes < 0
```

#### Test: Engagement Score Logic
```sql
-- tests/engagement_score_validation.sql
SELECT 
    meeting_id,
    engagement_score,
    participant_count,
    interaction_count
FROM {{ ref('go_meeting_facts') }}
WHERE engagement_score IS NULL 
   OR engagement_score < 0 
   OR engagement_score > 100
   OR (participant_count = 0 AND engagement_score > 0)
```

#### Test: Data Quality Filter Compliance
```sql
-- tests/data_quality_compliance.sql
SELECT 
    COUNT(*) as invalid_records
FROM {{ ref('go_meeting_facts') }}
WHERE record_status != 'ACTIVE' 
   OR data_quality_score < 0.7
   OR data_quality_score IS NULL
HAVING COUNT(*) > 0
```

#### Test: Participant Attendance Logic
```sql
-- tests/participant_attendance_validation.sql
SELECT 
    participant_id,
    meeting_id,
    attendance_percentage,
    join_time,
    leave_time
FROM {{ ref('go_participant_facts') }}
WHERE attendance_percentage > 100
   OR attendance_percentage < 0
   OR (join_time IS NULL AND attendance_percentage > 0)
   OR (leave_time < join_time)
```

#### Test: Webinar Registration vs Attendance
```sql
-- tests/webinar_attendance_logic.sql
SELECT 
    webinar_id,
    registration_count,
    attendance_count,
    attendance_rate
FROM {{ ref('go_webinar_facts') }}
WHERE attendance_count > registration_count
   OR attendance_rate != (attendance_count * 100.0 / NULLIF(registration_count, 0))
   OR attendance_rate > 100
```

#### Test: Billing Amount Precision
```sql
-- tests/billing_amount_precision.sql
SELECT 
    billing_id,
    billing_amount,
    currency_code
FROM {{ ref('go_billing_facts') }}
WHERE billing_amount < 0
   OR ROUND(billing_amount, 2) != billing_amount
   OR billing_amount > 999999.99
```

#### Test: Usage Metrics Consistency
```sql
-- tests/usage_metrics_consistency.sql
SELECT 
    usage_id,
    feature_name,
    usage_count,
    usage_duration_minutes
FROM {{ ref('go_usage_facts') }}
WHERE usage_count = 0 AND usage_duration_minutes > 0
   OR usage_count > 0 AND usage_duration_minutes = 0
   OR usage_duration_minutes < 0
```

#### Test: Quality Score Aggregation
```sql
-- tests/quality_score_aggregation.sql
SELECT 
    quality_id,
    quality_score,
    completeness_score,
    accuracy_score,
    consistency_score
FROM {{ ref('go_quality_facts') }}
WHERE quality_score != (completeness_score + accuracy_score + consistency_score) / 3.0
   OR ABS(quality_score - (completeness_score + accuracy_score + consistency_score) / 3.0) > 0.01
```

#### Test: Timezone Conversion Accuracy
```sql
-- tests/timezone_conversion_validation.sql
SELECT 
    meeting_id,
    original_start_time,
    utc_start_time,
    source_timezone
FROM {{ ref('go_meeting_facts') }}
WHERE utc_start_time IS NULL
   OR original_start_time IS NULL
   OR CONVERT_TIMEZONE(source_timezone, 'UTC', original_start_time) != utc_start_time
```

#### Test: Audit Trail Completeness
```sql
-- tests/audit_trail_completeness.sql
SELECT 
    table_name,
    COUNT(*) as records_without_audit
FROM (
    SELECT 'go_meeting_facts' as table_name FROM {{ ref('go_meeting_facts') }} WHERE audit_id IS NULL
    UNION ALL
    SELECT 'go_participant_facts' as table_name FROM {{ ref('go_participant_facts') }} WHERE audit_id IS NULL
    UNION ALL
    SELECT 'go_webinar_facts' as table_name FROM {{ ref('go_webinar_facts') }} WHERE audit_id IS NULL
    UNION ALL
    SELECT 'go_billing_facts' as table_name FROM {{ ref('go_billing_facts') }} WHERE audit_id IS NULL
    UNION ALL
    SELECT 'go_usage_facts' as table_name FROM {{ ref('go_usage_facts') }} WHERE audit_id IS NULL
    UNION ALL
    SELECT 'go_quality_facts' as table_name FROM {{ ref('go_quality_facts') }} WHERE audit_id IS NULL
)
GROUP BY table_name
HAVING COUNT(*) > 0
```

## Test Execution Strategy

### 1. Pre-deployment Testing
- Run all schema tests using `dbt test`
- Execute custom SQL tests individually
- Validate test coverage across all models

### 2. Continuous Integration
- Integrate tests into CI/CD pipeline
- Set up automated test execution on code changes
- Configure test failure notifications

### 3. Performance Testing
- Monitor test execution times
- Optimize slow-running tests
- Set up test result caching where appropriate

### 4. Data Quality Monitoring
- Schedule regular test runs in production
- Set up alerting for test failures
- Track test results over time

## Expected Test Results

### Success Criteria
- All unique and not_null tests pass
- Relationship tests validate referential integrity
- Custom business logic tests return zero records
- Data quality thresholds are maintained
- Audit trails are complete and consistent

### Failure Scenarios
- Schema changes breaking existing tests
- Data quality degradation below thresholds
- Business rule violations
- Performance degradation in transformations

## API Cost Calculation

### Snowflake Compute Costs (Estimated)
- **Test Execution Frequency**: Daily
- **Average Test Runtime**: 15 minutes
- **Warehouse Size**: MEDIUM (4 credits/hour)
- **Daily Cost**: (15/60) × 4 × $2.00 = $2.00
- **Monthly Cost**: $2.00 × 30 = $60.00
- **Annual Cost**: $60.00 × 12 = $720.00

### Additional Considerations
- Storage costs for test data: ~$5/month
- Monitoring and alerting: ~$10/month
- **Total Monthly Cost**: $75.00 USD
- **Total Annual Cost**: $900.00 USD

## Maintenance and Updates

### Regular Review Schedule
- **Weekly**: Review test results and failures
- **Monthly**: Update test cases based on new requirements
- **Quarterly**: Performance optimization and cost review
- **Annually**: Comprehensive test strategy review

### Version Control
- All test files maintained in Git repository
- Test changes require code review
- Test documentation updated with each release

---

**Document Status**: Active  
**Next Review Date**: 2025-01-19  
**Approved By**: Data Engineering Team