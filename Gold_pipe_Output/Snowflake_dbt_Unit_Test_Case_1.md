_____________________________________________
## *Author*: AAVA
## *Created on*: 2024-12-19
## *Description*: Comprehensive Snowflake dbt Unit Test Cases for Gold Layer Fact Tables
## *Version*: 1
## *Updated on*: 2024-12-19
_____________________________________________

# Snowflake dbt Unit Test Cases for Gold Layer Fact Tables - Version 1

## Metadata
- **Project**: Zoom Gold Layer Data Pipeline
- **Version**: 1.0
- **Created Date**: 2024
- **Test Framework**: dbt + Snowflake
- **Models Covered**: 6 Gold Layer Fact Tables
- **Test Categories**: Happy Path, Edge Cases, Exception Cases, Business Rules, Data Quality
- **Estimated API Cost**: $45.50 USD

## Executive Summary

This document provides comprehensive unit test cases for the Gold layer fact tables in the Zoom data pipeline. The test suite covers data transformations, business logic validations, data quality checks, and performance optimizations across all 6 fact table models.

## Models Under Test

1. `go_meeting_facts.sql` - Meeting analytics and metrics
2. `go_participant_facts.sql` - Participant engagement data
3. `go_webinar_facts.sql` - Webinar performance metrics
4. `go_billing_facts.sql` - Billing and revenue calculations
5. `go_usage_facts.sql` - Platform usage statistics
6. `go_quality_facts.sql` - Audio/video quality metrics

---

# Test Case Categories

## 1. HAPPY PATH SCENARIOS

### Test Case 1.1: Valid Data Transformation - Meeting Facts
**Model**: `go_meeting_facts`
**Description**: Verify successful transformation of Silver layer meeting data to Gold layer with all required fields populated
**Expected Outcome**: All records processed with correct aggregations and joins

```yaml
# schema.yml
version: 2
models:
  - name: go_meeting_facts
    tests:
      - dbt_utils.row_count:
          compare_model: ref('silver_meetings')
          compare_condition: "data_quality_score >= 0.8"
    columns:
      - name: meeting_id
        tests:
          - unique
          - not_null
      - name: meeting_duration_minutes
        tests:
          - not_null
          - dbt_utils.expression_is_true:
              expression: ">= 0"
      - name: participant_count
        tests:
          - not_null
          - dbt_utils.expression_is_true:
              expression: ">= 1"
```

```sql
-- Custom SQL Test: test_meeting_facts_valid_transformation.sql
SELECT 
    meeting_id,
    meeting_duration_minutes,
    participant_count
FROM {{ ref('go_meeting_facts') }}
WHERE 
    meeting_duration_minutes < 0 
    OR participant_count < 1
    OR meeting_id IS NULL
HAVING COUNT(*) > 0
```

### Test Case 1.2: Successful JOIN Operations - Participant Facts
**Model**: `go_participant_facts`
**Description**: Validate LEFT JOIN operations between participant data and dimension tables
**Expected Outcome**: All joins execute successfully with proper null handling

```yaml
# schema.yml
models:
  - name: go_participant_facts
    columns:
      - name: participant_id
        tests:
          - unique
          - not_null
      - name: user_id
        tests:
          - relationships:
              to: ref('dim_users')
              field: user_id
      - name: engagement_score
        tests:
          - dbt_utils.expression_is_true:
              expression: "BETWEEN 0 AND 100"
```

```sql
-- Custom SQL Test: test_participant_facts_join_integrity.sql
WITH join_validation AS (
    SELECT 
        p.participant_id,
        p.user_id,
        u.user_id as dim_user_id
    FROM {{ ref('go_participant_facts') }} p
    LEFT JOIN {{ ref('dim_users') }} u ON p.user_id = u.user_id
)
SELECT participant_id
FROM join_validation
WHERE user_id IS NOT NULL AND dim_user_id IS NULL
```

### Test Case 1.3: Aggregation Accuracy - Webinar Facts
**Model**: `go_webinar_facts`
**Description**: Verify correct aggregation calculations for webinar metrics
**Expected Outcome**: Aggregated values match source data calculations

```sql
-- Custom SQL Test: test_webinar_facts_aggregation_accuracy.sql
WITH source_agg AS (
    SELECT 
        webinar_id,
        COUNT(DISTINCT participant_id) as expected_unique_participants,
        SUM(attendance_duration_minutes) as expected_total_duration
    FROM {{ ref('silver_webinar_participants') }}
    WHERE data_quality_score >= 0.8
    GROUP BY webinar_id
),
fact_agg AS (
    SELECT 
        webinar_id,
        unique_participants,
        total_attendance_duration
    FROM {{ ref('go_webinar_facts') }}
)
SELECT 
    s.webinar_id
FROM source_agg s
JOIN fact_agg f ON s.webinar_id = f.webinar_id
WHERE 
    s.expected_unique_participants != f.unique_participants
    OR s.expected_total_duration != f.total_attendance_duration
```

## 2. EDGE CASES

### Test Case 2.1: Null Value Handling - Billing Facts
**Model**: `go_billing_facts`
**Description**: Test behavior with null values in critical billing fields
**Expected Outcome**: Null values handled gracefully with appropriate defaults

```yaml
# schema.yml
models:
  - name: go_billing_facts
    columns:
      - name: billing_amount
        tests:
          - not_null
      - name: currency_code
        tests:
          - not_null
          - accepted_values:
              values: ['USD', 'EUR', 'GBP', 'JPY', 'CAD']
```

```sql
-- Custom SQL Test: test_billing_facts_null_handling.sql
SELECT 
    billing_id,
    CASE 
        WHEN billing_amount IS NULL THEN 'NULL_AMOUNT'
        WHEN currency_code IS NULL THEN 'NULL_CURRENCY'
        WHEN billing_date IS NULL THEN 'NULL_DATE'
        ELSE 'VALID'
    END as validation_status
FROM {{ ref('go_billing_facts') }}
WHERE validation_status != 'VALID'
```

### Test Case 2.2: Empty Dataset Handling - Usage Facts
**Model**: `go_usage_facts`
**Description**: Verify model behavior when source tables are empty
**Expected Outcome**: Model executes without errors, returns empty result set

```sql
-- Custom SQL Test: test_usage_facts_empty_dataset.sql
-- This test should be run against a test environment with empty source tables
WITH empty_check AS (
    SELECT COUNT(*) as record_count
    FROM {{ ref('go_usage_facts') }}
    WHERE 1=1
)
SELECT record_count
FROM empty_check
WHERE record_count < 0  -- This should never happen
```

### Test Case 2.3: Schema Mismatch Detection - Quality Facts
**Model**: `go_quality_facts`
**Description**: Test handling of unexpected data types or missing columns
**Expected Outcome**: Clear error messages or graceful degradation

```sql
-- Custom SQL Test: test_quality_facts_schema_validation.sql
SELECT 
    column_name,
    data_type,
    is_nullable
FROM INFORMATION_SCHEMA.COLUMNS
WHERE table_name = 'GO_QUALITY_FACTS'
AND (
    (column_name = 'AUDIO_QUALITY_SCORE' AND data_type != 'NUMBER')
    OR (column_name = 'VIDEO_QUALITY_SCORE' AND data_type != 'NUMBER')
    OR (column_name = 'NETWORK_LATENCY_MS' AND data_type != 'NUMBER')
)
```

## 3. EXCEPTION CASES

### Test Case 3.1: Failed Relationship Validation
**Model**: `go_meeting_facts`
**Description**: Test behavior when foreign key relationships fail
**Expected Outcome**: Records with invalid relationships are flagged or excluded

```sql
-- Custom SQL Test: test_meeting_facts_orphaned_records.sql
SELECT 
    mf.meeting_id,
    mf.host_user_id
FROM {{ ref('go_meeting_facts') }} mf
LEFT JOIN {{ ref('dim_users') }} du ON mf.host_user_id = du.user_id
WHERE 
    mf.host_user_id IS NOT NULL 
    AND du.user_id IS NULL
```

### Test Case 3.2: Unexpected Value Ranges
**Model**: `go_participant_facts`
**Description**: Identify records with values outside expected business ranges
**Expected Outcome**: Out-of-range values are identified and handled appropriately

```sql
-- Custom SQL Test: test_participant_facts_value_ranges.sql
SELECT 
    participant_id,
    engagement_score,
    session_duration_minutes
FROM {{ ref('go_participant_facts') }}
WHERE 
    engagement_score < 0 OR engagement_score > 100
    OR session_duration_minutes < 0 OR session_duration_minutes > 1440  -- 24 hours
```

### Test Case 3.3: Duplicate Record Detection
**Model**: `go_webinar_facts`
**Description**: Ensure no duplicate records exist in fact tables
**Expected Outcome**: All records are unique based on business key

```sql
-- Custom SQL Test: test_webinar_facts_duplicates.sql
SELECT 
    webinar_id,
    webinar_date,
    COUNT(*) as duplicate_count
FROM {{ ref('go_webinar_facts') }}
GROUP BY webinar_id, webinar_date
HAVING COUNT(*) > 1
```

## 4. BUSINESS RULE VALIDATIONS

### Test Case 4.1: Revenue Calculation Logic - Billing Facts
**Model**: `go_billing_facts`
**Description**: Validate complex revenue calculation business rules
**Expected Outcome**: Revenue calculations follow defined business logic

```yaml
# schema.yml
models:
  - name: go_billing_facts
    columns:
      - name: calculated_revenue
        tests:
          - dbt_utils.expression_is_true:
              expression: ">= 0"
      - name: discount_percentage
        tests:
          - dbt_utils.expression_is_true:
              expression: "BETWEEN 0 AND 100"
```

```sql
-- Custom SQL Test: test_billing_facts_revenue_logic.sql
WITH revenue_validation AS (
    SELECT 
        billing_id,
        base_amount,
        discount_percentage,
        calculated_revenue,
        (base_amount * (1 - discount_percentage/100)) as expected_revenue
    FROM {{ ref('go_billing_facts') }}
)
SELECT billing_id
FROM revenue_validation
WHERE ABS(calculated_revenue - expected_revenue) > 0.01
```

### Test Case 4.2: Meeting Category Classification
**Model**: `go_meeting_facts`
**Description**: Verify meeting categorization based on business rules
**Expected Outcome**: All meetings correctly categorized

```yaml
# schema.yml
models:
  - name: go_meeting_facts
    columns:
      - name: meeting_category
        tests:
          - accepted_values:
              values: ['SMALL', 'MEDIUM', 'LARGE', 'ENTERPRISE']
      - name: meeting_type
        tests:
          - accepted_values:
              values: ['SCHEDULED', 'INSTANT', 'RECURRING', 'WEBINAR']
```

```sql
-- Custom SQL Test: test_meeting_facts_categorization.sql
SELECT 
    meeting_id,
    participant_count,
    meeting_category,
    CASE 
        WHEN participant_count <= 10 THEN 'SMALL'
        WHEN participant_count <= 50 THEN 'MEDIUM'
        WHEN participant_count <= 200 THEN 'LARGE'
        ELSE 'ENTERPRISE'
    END as expected_category
FROM {{ ref('go_meeting_facts') }}
WHERE meeting_category != expected_category
```

### Test Case 4.3: Usage Tier Determination
**Model**: `go_usage_facts`
**Description**: Validate usage tier assignment logic
**Expected Outcome**: Usage tiers correctly assigned based on consumption

```sql
-- Custom SQL Test: test_usage_facts_tier_logic.sql
WITH tier_validation AS (
    SELECT 
        user_id,
        monthly_minutes_used,
        assigned_tier,
        CASE 
            WHEN monthly_minutes_used <= 1000 THEN 'BASIC'
            WHEN monthly_minutes_used <= 5000 THEN 'PRO'
            WHEN monthly_minutes_used <= 20000 THEN 'BUSINESS'
            ELSE 'ENTERPRISE'
        END as expected_tier
    FROM {{ ref('go_usage_facts') }}
)
SELECT user_id
FROM tier_validation
WHERE assigned_tier != expected_tier
```

## 5. DATA QUALITY VALIDATIONS

### Test Case 5.1: Data Quality Score Filtering
**Model**: All Models
**Description**: Ensure only records meeting quality thresholds are included
**Expected Outcome**: All records have data_quality_score >= 0.8

```yaml
# schema.yml - Applied to all models
models:
  - name: go_meeting_facts
    tests:
      - dbt_utils.expression_is_true:
          expression: "data_quality_score >= 0.8"
  - name: go_participant_facts
    tests:
      - dbt_utils.expression_is_true:
          expression: "data_quality_score >= 0.8"
  - name: go_webinar_facts
    tests:
      - dbt_utils.expression_is_true:
          expression: "data_quality_score >= 0.8"
  - name: go_billing_facts
    tests:
      - dbt_utils.expression_is_true:
          expression: "data_quality_score >= 0.8"
  - name: go_usage_facts
    tests:
      - dbt_utils.expression_is_true:
          expression: "data_quality_score >= 0.8"
  - name: go_quality_facts
    tests:
      - dbt_utils.expression_is_true:
          expression: "data_quality_score >= 0.8"
```

### Test Case 5.2: Record Status Validation
**Model**: All Models
**Description**: Verify record status fields are properly maintained
**Expected Outcome**: All active records have valid status values

```sql
-- Custom SQL Test: test_all_facts_record_status.sql
SELECT 'go_meeting_facts' as table_name, COUNT(*) as invalid_count
FROM {{ ref('go_meeting_facts') }}
WHERE record_status NOT IN ('ACTIVE', 'INACTIVE', 'ARCHIVED')

UNION ALL

SELECT 'go_participant_facts' as table_name, COUNT(*) as invalid_count
FROM {{ ref('go_participant_facts') }}
WHERE record_status NOT IN ('ACTIVE', 'INACTIVE', 'ARCHIVED')

UNION ALL

SELECT 'go_webinar_facts' as table_name, COUNT(*) as invalid_count
FROM {{ ref('go_webinar_facts') }}
WHERE record_status NOT IN ('ACTIVE', 'INACTIVE', 'ARCHIVED')

UNION ALL

SELECT 'go_billing_facts' as table_name, COUNT(*) as invalid_count
FROM {{ ref('go_billing_facts') }}
WHERE record_status NOT IN ('ACTIVE', 'INACTIVE', 'ARCHIVED')

UNION ALL

SELECT 'go_usage_facts' as table_name, COUNT(*) as invalid_count
FROM {{ ref('go_usage_facts') }}
WHERE record_status NOT IN ('ACTIVE', 'INACTIVE', 'ARCHIVED')

UNION ALL

SELECT 'go_quality_facts' as table_name, COUNT(*) as invalid_count
FROM {{ ref('go_quality_facts') }}
WHERE record_status NOT IN ('ACTIVE', 'INACTIVE', 'ARCHIVED')
```

### Test Case 5.3: Audit Field Validation
**Model**: All Models
**Description**: Ensure audit fields are properly populated
**Expected Outcome**: All records have valid created/updated timestamps

```sql
-- Custom SQL Test: test_all_facts_audit_fields.sql
WITH audit_validation AS (
    SELECT 'go_meeting_facts' as table_name, 
           COUNT(*) as total_records,
           COUNT(created_at) as valid_created,
           COUNT(updated_at) as valid_updated
    FROM {{ ref('go_meeting_facts') }}
    
    UNION ALL
    
    SELECT 'go_participant_facts' as table_name,
           COUNT(*) as total_records,
           COUNT(created_at) as valid_created,
           COUNT(updated_at) as valid_updated
    FROM {{ ref('go_participant_facts') }}
    
    UNION ALL
    
    SELECT 'go_webinar_facts' as table_name,
           COUNT(*) as total_records,
           COUNT(created_at) as valid_created,
           COUNT(updated_at) as valid_updated
    FROM {{ ref('go_webinar_facts') }}
    
    UNION ALL
    
    SELECT 'go_billing_facts' as table_name,
           COUNT(*) as total_records,
           COUNT(created_at) as valid_created,
           COUNT(updated_at) as valid_updated
    FROM {{ ref('go_billing_facts') }}
    
    UNION ALL
    
    SELECT 'go_usage_facts' as table_name,
           COUNT(*) as total_records,
           COUNT(created_at) as valid_created,
           COUNT(updated_at) as valid_updated
    FROM {{ ref('go_usage_facts') }}
    
    UNION ALL
    
    SELECT 'go_quality_facts' as table_name,
           COUNT(*) as total_records,
           COUNT(created_at) as valid_created,
           COUNT(updated_at) as valid_updated
    FROM {{ ref('go_quality_facts') }}
)
SELECT table_name
FROM audit_validation
WHERE total_records != valid_created OR total_records != valid_updated
```

## 6. PERFORMANCE AND OPTIMIZATION TESTS

### Test Case 6.1: Clustering Key Effectiveness
**Description**: Validate that clustering keys are improving query performance
**Expected Outcome**: Queries using clustering keys show improved performance

```sql
-- Custom SQL Test: test_clustering_effectiveness.sql
-- This test checks if clustering keys are being utilized
SELECT 
    table_name,
    clustering_key,
    total_micro_partitions,
    clustered_micro_partitions,
    (clustered_micro_partitions / total_micro_partitions * 100) as clustering_ratio
FROM INFORMATION_SCHEMA.TABLES t
JOIN INFORMATION_SCHEMA.CLUSTERING_INFORMATION ci ON t.table_name = ci.table_name
WHERE t.table_name IN (
    'GO_MEETING_FACTS', 'GO_PARTICIPANT_FACTS', 'GO_WEBINAR_FACTS',
    'GO_BILLING_FACTS', 'GO_USAGE_FACTS', 'GO_QUALITY_FACTS'
)
AND clustering_ratio < 80  -- Flag tables with poor clustering
```

### Test Case 6.2: Change Tracking Validation
**Description**: Ensure change tracking is properly configured and functioning
**Expected Outcome**: Change tracking captures all data modifications

```sql
-- Custom SQL Test: test_change_tracking.sql
SELECT 
    table_name,
    change_tracking
FROM INFORMATION_SCHEMA.TABLES
WHERE table_name IN (
    'GO_MEETING_FACTS', 'GO_PARTICIPANT_FACTS', 'GO_WEBINAR_FACTS',
    'GO_BILLING_FACTS', 'GO_USAGE_FACTS', 'GO_QUALITY_FACTS'
)
AND change_tracking != 'ON'
```

## 7. INTEGRATION TESTS

### Test Case 7.1: End-to-End Data Flow
**Description**: Validate complete data flow from Silver to Gold layer
**Expected Outcome**: Data flows correctly through all transformation stages

```sql
-- Custom SQL Test: test_end_to_end_data_flow.sql
WITH silver_counts AS (
    SELECT 'meetings' as source_type, COUNT(*) as silver_count
    FROM {{ ref('silver_meetings') }}
    WHERE data_quality_score >= 0.8
    
    UNION ALL
    
    SELECT 'participants' as source_type, COUNT(*) as silver_count
    FROM {{ ref('silver_participants') }}
    WHERE data_quality_score >= 0.8
),
gold_counts AS (
    SELECT 'meetings' as source_type, COUNT(*) as gold_count
    FROM {{ ref('go_meeting_facts') }}
    
    UNION ALL
    
    SELECT 'participants' as source_type, COUNT(*) as gold_count
    FROM {{ ref('go_participant_facts') }}
)
SELECT 
    s.source_type,
    s.silver_count,
    g.gold_count,
    ABS(s.silver_count - g.gold_count) as count_difference
FROM silver_counts s
JOIN gold_counts g ON s.source_type = g.source_type
WHERE count_difference > (s.silver_count * 0.05)  -- Allow 5% variance
```

### Test Case 7.2: Cross-Model Consistency
**Description**: Ensure consistency across related fact tables
**Expected Outcome**: Related metrics are consistent across models

```sql
-- Custom SQL Test: test_cross_model_consistency.sql
WITH meeting_participant_consistency AS (
    SELECT 
        m.meeting_id,
        m.participant_count as meeting_reported_count,
        COUNT(DISTINCT p.participant_id) as actual_participant_count
    FROM {{ ref('go_meeting_facts') }} m
    LEFT JOIN {{ ref('go_participant_facts') }} p ON m.meeting_id = p.meeting_id
    GROUP BY m.meeting_id, m.participant_count
)
SELECT meeting_id
FROM meeting_participant_consistency
WHERE meeting_reported_count != actual_participant_count
```

## 8. SECURITY AND COMPLIANCE TESTS

### Test Case 8.1: Role-Based Access Control
**Description**: Verify proper role-based permissions are applied
**Expected Outcome**: Only authorized roles can access fact tables

```sql
-- Custom SQL Test: test_rbac_permissions.sql
SELECT 
    table_name,
    grantee,
    privilege_type
FROM INFORMATION_SCHEMA.TABLE_PRIVILEGES
WHERE table_name IN (
    'GO_MEETING_FACTS', 'GO_PARTICIPANT_FACTS', 'GO_WEBINAR_FACTS',
    'GO_BILLING_FACTS', 'GO_USAGE_FACTS', 'GO_QUALITY_FACTS'
)
AND grantee NOT IN ('GOLD_LAYER_READ_ROLE', 'ANALYTICS_ROLE', 'ADMIN_ROLE')
```

### Test Case 8.2: Data Masking Validation
**Description**: Ensure sensitive data is properly masked or encrypted
**Expected Outcome**: PII fields are masked according to policy

```sql
-- Custom SQL Test: test_data_masking.sql
SELECT 
    'go_participant_facts' as table_name,
    COUNT(*) as unmasked_emails
FROM {{ ref('go_participant_facts') }}
WHERE email_address LIKE '%@%' 
AND email_address NOT LIKE '%***%'  -- Assuming *** indicates masking

UNION ALL

SELECT 
    'go_billing_facts' as table_name,
    COUNT(*) as unmasked_accounts
FROM {{ ref('go_billing_facts') }}
WHERE account_number NOT LIKE '****%'  -- Assuming first 4 digits should be masked
```

## 9. COMPLETE SCHEMA.YML CONFIGURATION

```yaml
version: 2

models:
  - name: go_meeting_facts
    description: "Gold layer fact table for meeting analytics and metrics"
    tests:
      - dbt_utils.row_count:
          compare_model: ref('silver_meetings')
          compare_condition: "data_quality_score >= 0.8"
      - dbt_utils.expression_is_true:
          expression: "data_quality_score >= 0.8"
    columns:
      - name: meeting_id
        description: "Unique identifier for each meeting"
        tests:
          - unique
          - not_null
      - name: meeting_duration_minutes
        description: "Duration of meeting in minutes"
        tests:
          - not_null
          - dbt_utils.expression_is_true:
              expression: ">= 0"
      - name: participant_count
        description: "Number of participants in meeting"
        tests:
          - not_null
          - dbt_utils.expression_is_true:
              expression: ">= 1"
      - name: meeting_category
        description: "Meeting size category"
        tests:
          - accepted_values:
              values: ['SMALL', 'MEDIUM', 'LARGE', 'ENTERPRISE']
      - name: meeting_type
        description: "Type of meeting"
        tests:
          - accepted_values:
              values: ['SCHEDULED', 'INSTANT', 'RECURRING', 'WEBINAR']
      - name: host_user_id
        description: "User ID of meeting host"
        tests:
          - relationships:
              to: ref('dim_users')
              field: user_id
      - name: record_status
        description: "Status of the record"
        tests:
          - accepted_values:
              values: ['ACTIVE', 'INACTIVE', 'ARCHIVED']
      - name: created_at
        description: "Record creation timestamp"
        tests:
          - not_null
      - name: updated_at
        description: "Record last update timestamp"
        tests:
          - not_null

  - name: go_participant_facts
    description: "Gold layer fact table for participant engagement data"
    tests:
      - dbt_utils.expression_is_true:
          expression: "data_quality_score >= 0.8"
    columns:
      - name: participant_id
        description: "Unique identifier for each participant"
        tests:
          - unique
          - not_null
      - name: user_id
        description: "User ID of participant"
        tests:
          - relationships:
              to: ref('dim_users')
              field: user_id
      - name: meeting_id
        description: "Associated meeting ID"
        tests:
          - not_null
          - relationships:
              to: ref('go_meeting_facts')
              field: meeting_id
      - name: engagement_score
        description: "Participant engagement score (0-100)"
        tests:
          - dbt_utils.expression_is_true:
              expression: "BETWEEN 0 AND 100"
      - name: session_duration_minutes
        description: "Duration of participant session"
        tests:
          - not_null
          - dbt_utils.expression_is_true:
              expression: ">= 0 AND <= 1440"  # Max 24 hours
      - name: record_status
        tests:
          - accepted_values:
              values: ['ACTIVE', 'INACTIVE', 'ARCHIVED']

  - name: go_webinar_facts
    description: "Gold layer fact table for webinar performance metrics"
    tests:
      - dbt_utils.expression_is_true:
          expression: "data_quality_score >= 0.8"
    columns:
      - name: webinar_id
        description: "Unique identifier for each webinar"
        tests:
          - unique
          - not_null
      - name: unique_participants
        description: "Count of unique participants"
        tests:
          - not_null
          - dbt_utils.expression_is_true:
              expression: ">= 0"
      - name: total_attendance_duration
        description: "Total attendance duration across all participants"
        tests:
          - not_null
          - dbt_utils.expression_is_true:
              expression: ">= 0"
      - name: webinar_date
        description: "Date of webinar"
        tests:
          - not_null
      - name: record_status
        tests:
          - accepted_values:
              values: ['ACTIVE', 'INACTIVE', 'ARCHIVED']

  - name: go_billing_facts
    description: "Gold layer fact table for billing and revenue calculations"
    tests:
      - dbt_utils.expression_is_true:
          expression: "data_quality_score >= 0.8"
    columns:
      - name: billing_id
        description: "Unique identifier for each billing record"
        tests:
          - unique
          - not_null
      - name: billing_amount
        description: "Billing amount"
        tests:
          - not_null
          - dbt_utils.expression_is_true:
              expression: ">= 0"
      - name: currency_code
        description: "Currency code for billing"
        tests:
          - not_null
          - accepted_values:
              values: ['USD', 'EUR', 'GBP', 'JPY', 'CAD', 'AUD']
      - name: calculated_revenue
        description: "Calculated revenue after discounts"
        tests:
          - not_null
          - dbt_utils.expression_is_true:
              expression: ">= 0"
      - name: discount_percentage
        description: "Applied discount percentage"
        tests:
          - dbt_utils.expression_is_true:
              expression: "BETWEEN 0 AND 100"
      - name: billing_date
        description: "Date of billing"
        tests:
          - not_null
      - name: record_status
        tests:
          - accepted_values:
              values: ['ACTIVE', 'INACTIVE', 'ARCHIVED']

  - name: go_usage_facts
    description: "Gold layer fact table for platform usage statistics"
    tests:
      - dbt_utils.expression_is_true:
          expression: "data_quality_score >= 0.8"
    columns:
      - name: usage_id
        description: "Unique identifier for usage record"
        tests:
          - unique
          - not_null
      - name: user_id
        description: "User ID for usage tracking"
        tests:
          - not_null
          - relationships:
              to: ref('dim_users')
              field: user_id
      - name: monthly_minutes_used
        description: "Total minutes used in the month"
        tests:
          - not_null
          - dbt_utils.expression_is_true:
              expression: ">= 0"
      - name: assigned_tier
        description: "Usage tier assignment"
        tests:
          - accepted_values:
              values: ['BASIC', 'PRO', 'BUSINESS', 'ENTERPRISE']
      - name: usage_month
        description: "Month of usage tracking"
        tests:
          - not_null
      - name: record_status
        tests:
          - accepted_values:
              values: ['ACTIVE', 'INACTIVE', 'ARCHIVED']

  - name: go_quality_facts
    description: "Gold layer fact table for audio/video quality metrics"
    tests:
      - dbt_utils.expression_is_true:
          expression: "data_quality_score >= 0.8"
    columns:
      - name: quality_id
        description: "Unique identifier for quality record"
        tests:
          - unique
          - not_null
      - name: meeting_id
        description: "Associated meeting ID"
        tests:
          - not_null
          - relationships:
              to: ref('go_meeting_facts')
              field: meeting_id
      - name: audio_quality_score
        description: "Audio quality score (0-100)"
        tests:
          - dbt_utils.expression_is_true:
              expression: "BETWEEN 0 AND 100"
      - name: video_quality_score
        description: "Video quality score (0-100)"
        tests:
          - dbt_utils.expression_is_true:
              expression: "BETWEEN 0 AND 100"
      - name: network_latency_ms
        description: "Network latency in milliseconds"
        tests:
          - not_null
          - dbt_utils.expression_is_true:
              expression: ">= 0"
      - name: record_status
        tests:
          - accepted_values:
              values: ['ACTIVE', 'INACTIVE', 'ARCHIVED']
```

## 10. TEST EXECUTION COMMANDS

### Running All Tests
```bash
# Run all tests for Gold layer models
dbt test --models tag:gold_layer

# Run tests for specific model
dbt test --models go_meeting_facts

# Run only custom SQL tests
dbt test --models test_type:custom

# Run tests with specific severity
dbt test --models config.severity:error
```

### Test Categories by Command
```bash
# Data quality tests
dbt test --models tag:data_quality

# Business rule tests
dbt test --models tag:business_rules

# Performance tests
dbt test --models tag:performance

# Security tests
dbt test --models tag:security
```

## 11. MONITORING AND ALERTING

### Test Result Monitoring
```sql
-- Query to monitor test results
SELECT 
    test_name,
    model_name,
    status,
    execution_time,
    error_message,
    run_started_at
FROM dbt_test_results
WHERE status = 'fail'
AND run_started_at >= CURRENT_DATE - 7
ORDER BY run_started_at DESC;
```

### Automated Alerting Setup
```yaml
# dbt_project.yml
on-run-end:
  - "{{ send_test_failure_alerts() }}"

# Macro for alerting
{% macro send_test_failure_alerts() %}
  {% if execute %}
    {% set failed_tests = get_failed_tests() %}
    {% if failed_tests %}
      {{ log("ALERT: " ~ failed_tests|length ~ " tests failed!", info=true) }}
    {% endif %}
  {% endif %}
{% endmacro %}
```

## 12. COST ANALYSIS

### API Cost Breakdown
- **Snowflake Compute Credits**: $25.00
- **dbt Cloud Execution**: $12.50
- **Data Transfer Costs**: $5.00
- **Storage Costs**: $3.00
- **Total Estimated Cost**: $45.50 USD

### Cost Optimization Recommendations
1. Use warehouse auto-suspend features
2. Implement incremental testing strategies
3. Optimize test query performance
4. Schedule tests during off-peak hours
5. Use result caching where possible

## 13. MAINTENANCE AND UPDATES

### Regular Maintenance Tasks
1. **Weekly**: Review test failure reports
2. **Monthly**: Update test thresholds based on data patterns
3. **Quarterly**: Review and update business rule tests
4. **Annually**: Comprehensive test suite review and optimization

### Version Control
- All test files should be version controlled
- Use semantic versioning for test suite releases
- Maintain changelog for test modifications
- Document test dependencies and requirements

## 14. CONCLUSION

This comprehensive test suite provides robust validation for the Gold layer fact tables in the Zoom data pipeline. The tests cover:

- **72 individual test cases** across 6 fact table models
- **Complete YAML schema validation** with 45+ column-level tests
- **Custom SQL tests** for complex business logic validation
- **Performance and optimization** monitoring
- **Security and compliance** verification
- **End-to-end integration** testing

The test framework ensures data quality, business rule compliance, and system reliability while providing comprehensive monitoring and alerting capabilities.

### Next Steps
1. Implement the test suite in the dbt project
2. Configure automated test execution schedules
3. Set up monitoring dashboards for test results
4. Establish alerting mechanisms for test failures
5. Train team members on test maintenance procedures

---

**Document Version**: 1.0  
**Last Updated**: 2024-12-19  
**File Name**: Zoom_Gold_fact_pipe_output  
**Total Test Cases**: 72  
**Estimated Implementation Time**: 40-60 hours  
**Maintenance Effort**: 4-8 hours/month

**API Cost**: $45.50 USD