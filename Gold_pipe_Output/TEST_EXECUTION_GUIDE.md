# Snowflake dbt Unit Test Execution Guide

## Overview
This guide provides comprehensive instructions for executing and maintaining unit tests for the Zoom Gold Fact Tables in Snowflake using dbt.

## Table of Contents
1. [Test Structure](#test-structure)
2. [Prerequisites](#prerequisites)
3. [Test Execution](#test-execution)
4. [Test Categories](#test-categories)
5. [Interpreting Results](#interpreting-results)
6. [Maintenance Guidelines](#maintenance-guidelines)
7. [Troubleshooting](#troubleshooting)

## Test Structure

### Files Overview
```
Gold_pipe_Output/
├── Zoom_Gold_fact_pipe_output.sql    # Main unit test SQL file
├── schema.yml                         # dbt test configurations
├── custom_test_macros.sql            # Custom test macros
├── TEST_EXECUTION_GUIDE.md           # This guide
└── directory_check.md                # Directory validation
```

### Covered Tables
- `go_meeting_facts` - Meeting-level metrics and attributes
- `go_participant_facts` - Participant-level meeting data
- `go_webinar_facts` - Webinar-level metrics
- `go_billing_facts` - Billing information
- `go_usage_facts` - Usage metrics
- `go_quality_facts` - Quality metrics

## Prerequisites

### Environment Setup
1. **dbt Installation**: Ensure dbt-snowflake is installed
   ```bash
   pip install dbt-snowflake
   ```

2. **Snowflake Connection**: Configure profiles.yml
   ```yaml
   zoom_dbt:
     target: dev
     outputs:
       dev:
         type: snowflake
         account: [your_account]
         user: [your_user]
         password: [your_password]
         role: [your_role]
         database: [your_database]
         warehouse: [your_warehouse]
         schema: [your_schema]
   ```

3. **Required Packages**: Add to packages.yml
   ```yaml
   packages:
     - package: dbt-labs/dbt_utils
       version: 1.1.1
   ```

### Data Requirements
- All 6 fact tables must exist in Snowflake
- Tables should have recent data (within 7 days for freshness tests)
- Minimum 100 records per table for meaningful test results

## Test Execution

### 1. Install Dependencies
```bash
dbt deps
```

### 2. Compile Models
```bash
dbt compile
```

### 3. Run All Tests
```bash
# Run all tests
dbt test

# Run tests for specific models
dbt test --models go_meeting_facts
dbt test --models go_participant_facts

# Run tests with specific tags
dbt test --select tag:unit_test
dbt test --select tag:gold_layer
```

### 4. Run Custom SQL Tests
```bash
# Execute the main unit test SQL file
dbt run-operation test_meeting_participant_consistency
dbt run-operation test_quality_score_rating_alignment
dbt run-operation test_billing_amount_consistency
```

### 5. Generate Test Documentation
```bash
dbt docs generate
dbt docs serve
```

## Test Categories

### 1. Data Quality Tests
- **Null Checks**: Validate critical fields are not null
- **Uniqueness**: Ensure primary keys are unique
- **Data Types**: Verify correct data types
- **Format Validation**: Check date formats, string patterns

### 2. Business Rule Tests
- **Range Validation**: Numeric fields within expected ranges
- **Relationship Validation**: Foreign key relationships
- **Logic Consistency**: Business rule compliance
- **Cross-table Validation**: Data consistency across tables

### 3. Performance Tests
- **Record Count Monitoring**: Track table sizes
- **Data Freshness**: Ensure recent data availability
- **Distribution Analysis**: Identify data skew
- **Query Performance**: Monitor test execution times

### 4. Edge Case Tests
- **Timezone Handling**: Consistent timezone usage
- **Large Data Volumes**: Handle high participant counts
- **Boundary Conditions**: Test limit values
- **Error Scenarios**: Validate error handling

## Interpreting Results

### Success Indicators
- ✅ All tests pass (0 failures)
- ✅ Test execution completes without errors
- ✅ Performance metrics within acceptable ranges
- ✅ Data freshness within 7 days

### Warning Indicators
- ⚠️ Minor data quality issues (< 5% failure rate)
- ⚠️ Performance degradation (> 50% increase in execution time)
- ⚠️ Data freshness 7-14 days old

### Failure Indicators
- ❌ Critical business rule violations
- ❌ Referential integrity failures
- ❌ Data freshness > 14 days
- ❌ Test execution errors

### Sample Test Output
```
21:45:32  1 of 25 START test not_null_go_meeting_facts_meeting_id ................ [RUN]
21:45:33  1 of 25 PASS not_null_go_meeting_facts_meeting_id ...................... [PASS in 0.82s]
21:45:33  2 of 25 START test unique_go_meeting_facts_meeting_uuid ................. [RUN]
21:45:34  2 of 25 PASS unique_go_meeting_facts_meeting_uuid ....................... [PASS in 1.23s]
```

## Maintenance Guidelines

### Daily Monitoring
1. **Automated Test Execution**
   ```bash
   # Add to cron job or CI/CD pipeline
   dbt test --select tag:critical
   ```

2. **Alert Configuration**
   - Set up alerts for test failures
   - Monitor data freshness daily
   - Track performance metrics

### Weekly Reviews
1. **Test Coverage Analysis**
   - Review test results trends
   - Identify recurring issues
   - Update test thresholds if needed

2. **Performance Optimization**
   - Analyze slow-running tests
   - Optimize test queries
   - Update test data samples

### Monthly Maintenance
1. **Test Suite Updates**
   - Add new test cases for new requirements
   - Remove obsolete tests
   - Update business rule validations

2. **Documentation Updates**
   - Update test descriptions
   - Refresh examples and guides
   - Document new edge cases

## Troubleshooting

### Common Issues

#### 1. Connection Errors
```
Error: Could not connect to Snowflake
```
**Solution**: Verify Snowflake credentials and network connectivity

#### 2. Missing Tables
```
Error: Relation 'go_meeting_facts' does not exist
```
**Solution**: Ensure all fact tables are created and accessible

#### 3. Test Failures
```
FAILED test not_null_go_meeting_facts_meeting_id
```
**Solution**: 
- Check data quality in source tables
- Review ETL processes
- Validate data transformation logic

#### 4. Performance Issues
```
Test execution taking > 5 minutes
```
**Solution**:
- Add appropriate indexes
- Optimize test queries
- Use data sampling for large tables

### Debug Commands
```bash
# Run tests in debug mode
dbt test --debug

# Show compiled SQL
dbt show --inline "select * from {{ ref('go_meeting_facts') }} limit 10"

# Check model dependencies
dbt list --models go_meeting_facts+

# Validate project configuration
dbt debug
```

### Performance Optimization

#### 1. Test Sampling
```sql
-- Add sampling to large table tests
SELECT * FROM {{ ref('go_meeting_facts') }}
SAMPLE (10)  -- Test on 10% sample
WHERE meeting_id IS NULL
```

#### 2. Incremental Testing
```sql
-- Test only recent data
SELECT * FROM {{ ref('go_meeting_facts') }}
WHERE start_time >= DATEADD('day', -7, CURRENT_DATE())
AND meeting_id IS NULL
```

#### 3. Parallel Execution
```bash
# Run tests in parallel
dbt test --threads 4
```

## Best Practices

### 1. Test Design
- Keep tests simple and focused
- Use descriptive test names
- Document test purpose and expected behavior
- Group related tests together

### 2. Performance
- Use appropriate sampling for large datasets
- Optimize test queries for performance
- Run critical tests more frequently
- Use incremental testing where possible

### 3. Maintenance
- Review and update tests regularly
- Remove obsolete tests
- Keep test documentation current
- Monitor test execution performance

### 4. Collaboration
- Share test results with stakeholders
- Document test failures and resolutions
- Maintain test change log
- Provide training on test interpretation

## Contact Information

For questions or issues with the test suite:
- **Data Engineering Team**: [team-email]
- **dbt Documentation**: https://docs.getdbt.com/
- **Snowflake Support**: https://support.snowflake.com/

---

**Last Updated**: $(date)
**Version**: 1.0
**Maintained By**: Data Engineering Team