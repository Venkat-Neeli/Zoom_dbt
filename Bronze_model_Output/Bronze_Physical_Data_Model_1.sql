_____________________________________________
## *Author*: AAVA
## *Created on*:   
## *Description*: Bronze Physical Data Model for Zoom Platform Analytics System implementing Medallion architecture Bronze layer
## *Version*: 1 
## *Updated on*: 
_____________________________________________

/*
==============================================================================
BRONZE LAYER PHYSICAL DATA MODEL - ZOOM PLATFORM ANALYTICS SYSTEM
==============================================================================
Author: AAVA
Description: Comprehensive Bronze Physical Data Model for Zoom Analytics System
             implementing Medallion architecture Bronze layer with raw data storage
             and metadata tracking capabilities for Snowflake platform
Version: 1.0
Created: 2025-01-21
Database: Snowflake
Schema: Bronze
==============================================================================
*/

-- ============================================================================
-- BRONZE LAYER DDL SCRIPT
-- ============================================================================
-- Purpose: Create Bronze layer tables for raw data ingestion from Zoom platform
-- Architecture: Medallion Bronze Layer - Raw data with metadata
-- Platform: Snowflake
-- ============================================================================

-- Create Bronze Schema if not exists
CREATE SCHEMA IF NOT EXISTS Bronze;

-- ============================================================================
-- BRONZE LAYER DATA TABLES
-- ============================================================================

-- ----------------------------------------------------------------------------
-- 1. Table: bz_users
-- Description: Raw user data from Zoom platform
-- Source: Users table from source system
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS Bronze.bz_users (
    user_id STRING,
    user_name STRING,
    email STRING,
    company STRING,
    plan_type STRING,
    -- Metadata columns
    load_timestamp TIMESTAMP_NTZ,
    update_timestamp TIMESTAMP_NTZ,
    source_system STRING
);

-- ----------------------------------------------------------------------------
-- 2. Table: bz_meetings
-- Description: Raw meeting data from Zoom platform
-- Source: Meetings table from source system
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS Bronze.bz_meetings (
    meeting_id STRING,
    host_id STRING,
    meeting_topic STRING,
    start_time TIMESTAMP_NTZ,
    end_time TIMESTAMP_NTZ,
    duration_minutes NUMBER,
    -- Metadata columns
    load_timestamp TIMESTAMP_NTZ,
    update_timestamp TIMESTAMP_NTZ,
    source_system STRING
);

-- ----------------------------------------------------------------------------
-- 3. Table: bz_participants
-- Description: Raw participant data from Zoom meetings
-- Source: Participants table from source system
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS Bronze.bz_participants (
    participant_id STRING,
    meeting_id STRING,
    user_id STRING,
    join_time TIMESTAMP_NTZ,
    leave_time TIMESTAMP_NTZ,
    -- Metadata columns
    load_timestamp TIMESTAMP_NTZ,
    update_timestamp TIMESTAMP_NTZ,
    source_system STRING
);

-- ----------------------------------------------------------------------------
-- 4. Table: bz_feature_usage
-- Description: Raw feature usage data from Zoom meetings
-- Source: Feature_Usage table from source system
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS Bronze.bz_feature_usage (
    usage_id STRING,
    meeting_id STRING,
    feature_name STRING,
    usage_count NUMBER,
    usage_date DATE,
    -- Metadata columns
    load_timestamp TIMESTAMP_NTZ,
    update_timestamp TIMESTAMP_NTZ,
    source_system STRING
);

-- ----------------------------------------------------------------------------
-- 5. Table: bz_webinars
-- Description: Raw webinar data from Zoom platform
-- Source: Webinars table from source system
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS Bronze.bz_webinars (
    webinar_id STRING,
    host_id STRING,
    webinar_topic STRING,
    start_time TIMESTAMP_NTZ,
    end_time TIMESTAMP_NTZ,
    registrants NUMBER,
    -- Metadata columns
    load_timestamp TIMESTAMP_NTZ,
    update_timestamp TIMESTAMP_NTZ,
    source_system STRING
);

-- ----------------------------------------------------------------------------
-- 6. Table: bz_support_tickets
-- Description: Raw support ticket data from Zoom platform
-- Source: Support_Tickets table from source system
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS Bronze.bz_support_tickets (
    ticket_id STRING,
    user_id STRING,
    ticket_type STRING,
    resolution_status STRING,
    open_date DATE,
    -- Metadata columns
    load_timestamp TIMESTAMP_NTZ,
    update_timestamp TIMESTAMP_NTZ,
    source_system STRING
);

-- ----------------------------------------------------------------------------
-- 7. Table: bz_licenses
-- Description: Raw license data from Zoom platform
-- Source: Licenses table from source system
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS Bronze.bz_licenses (
    license_id STRING,
    license_type STRING,
    assigned_to_user_id STRING,
    start_date DATE,
    end_date DATE,
    -- Metadata columns
    load_timestamp TIMESTAMP_NTZ,
    update_timestamp TIMESTAMP_NTZ,
    source_system STRING
);

-- ----------------------------------------------------------------------------
-- 8. Table: bz_billing_events
-- Description: Raw billing event data from Zoom platform
-- Source: Billing_Events table from source system
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS Bronze.bz_billing_events (
    event_id STRING,
    user_id STRING,
    event_type STRING,
    amount NUMBER(10,2),
    event_date DATE,
    -- Metadata columns
    load_timestamp TIMESTAMP_NTZ,
    update_timestamp TIMESTAMP_NTZ,
    source_system STRING
);

-- ============================================================================
-- BRONZE LAYER AUDIT TABLE
-- ============================================================================

-- ----------------------------------------------------------------------------
-- 9. Table: bz_audit_log
-- Description: Audit table for tracking data processing activities
-- Purpose: Monitor data loads, transformations, and processing status
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS Bronze.bz_audit_log (
    record_id NUMBER AUTOINCREMENT,
    source_table STRING,
    load_timestamp TIMESTAMP_NTZ,
    processed_by STRING,
    processing_time NUMBER,
    status STRING
);

-- ============================================================================
-- TABLE DOCUMENTATION AND COMMENTS
-- ============================================================================

-- Add table comments for documentation
COMMENT ON TABLE Bronze.bz_users IS 'Bronze layer table storing raw user data from Zoom platform with metadata tracking';
COMMENT ON TABLE Bronze.bz_meetings IS 'Bronze layer table storing raw meeting data from Zoom platform with metadata tracking';
COMMENT ON TABLE Bronze.bz_participants IS 'Bronze layer table storing raw participant data from Zoom meetings with metadata tracking';
COMMENT ON TABLE Bronze.bz_feature_usage IS 'Bronze layer table storing raw feature usage data from Zoom meetings with metadata tracking';
COMMENT ON TABLE Bronze.bz_webinars IS 'Bronze layer table storing raw webinar data from Zoom platform with metadata tracking';
COMMENT ON TABLE Bronze.bz_support_tickets IS 'Bronze layer table storing raw support ticket data from Zoom platform with metadata tracking';
COMMENT ON TABLE Bronze.bz_licenses IS 'Bronze layer table storing raw license data from Zoom platform with metadata tracking';
COMMENT ON TABLE Bronze.bz_billing_events IS 'Bronze layer table storing raw billing event data from Zoom platform with metadata tracking';
COMMENT ON TABLE Bronze.bz_audit_log IS 'Audit table for tracking data processing activities and monitoring Bronze layer operations';

-- Add column comments for key metadata fields
COMMENT ON COLUMN Bronze.bz_users.load_timestamp IS 'Timestamp when the record was first loaded into Bronze layer';
COMMENT ON COLUMN Bronze.bz_users.update_timestamp IS 'Timestamp when the record was last updated in Bronze layer';
COMMENT ON COLUMN Bronze.bz_users.source_system IS 'Identifier of the source system that provided the data';

COMMENT ON COLUMN Bronze.bz_meetings.load_timestamp IS 'Timestamp when the record was first loaded into Bronze layer';
COMMENT ON COLUMN Bronze.bz_meetings.update_timestamp IS 'Timestamp when the record was last updated in Bronze layer';
COMMENT ON COLUMN Bronze.bz_meetings.source_system IS 'Identifier of the source system that provided the data';

COMMENT ON COLUMN Bronze.bz_participants.load_timestamp IS 'Timestamp when the record was first loaded into Bronze layer';
COMMENT ON COLUMN Bronze.bz_participants.update_timestamp IS 'Timestamp when the record was last updated in Bronze layer';
COMMENT ON COLUMN Bronze.bz_participants.source_system IS 'Identifier of the source system that provided the data';

COMMENT ON COLUMN Bronze.bz_feature_usage.load_timestamp IS 'Timestamp when the record was first loaded into Bronze layer';
COMMENT ON COLUMN Bronze.bz_feature_usage.update_timestamp IS 'Timestamp when the record was last updated in Bronze layer';
COMMENT ON COLUMN Bronze.bz_feature_usage.source_system IS 'Identifier of the source system that provided the data';

COMMENT ON COLUMN Bronze.bz_webinars.load_timestamp IS 'Timestamp when the record was first loaded into Bronze layer';
COMMENT ON COLUMN Bronze.bz_webinars.update_timestamp IS 'Timestamp when the record was last updated in Bronze layer';
COMMENT ON COLUMN Bronze.bz_webinars.source_system IS 'Identifier of the source system that provided the data';

COMMENT ON COLUMN Bronze.bz_support_tickets.load_timestamp IS 'Timestamp when the record was first loaded into Bronze layer';
COMMENT ON COLUMN Bronze.bz_support_tickets.update_timestamp IS 'Timestamp when the record was last updated in Bronze layer';
COMMENT ON COLUMN Bronze.bz_support_tickets.source_system IS 'Identifier of the source system that provided the data';

COMMENT ON COLUMN Bronze.bz_licenses.load_timestamp IS 'Timestamp when the record was first loaded into Bronze layer';
COMMENT ON COLUMN Bronze.bz_licenses.update_timestamp IS 'Timestamp when the record was last updated in Bronze layer';
COMMENT ON COLUMN Bronze.bz_licenses.source_system IS 'Identifier of the source system that provided the data';

COMMENT ON COLUMN Bronze.bz_billing_events.load_timestamp IS 'Timestamp when the record was first loaded into Bronze layer';
COMMENT ON COLUMN Bronze.bz_billing_events.update_timestamp IS 'Timestamp when the record was last updated in Bronze layer';
COMMENT ON COLUMN Bronze.bz_billing_events.source_system IS 'Identifier of the source system that provided the data';

COMMENT ON COLUMN Bronze.bz_audit_log.record_id IS 'Auto-incrementing unique identifier for audit records';
COMMENT ON COLUMN Bronze.bz_audit_log.source_table IS 'Name of the source table being processed';
COMMENT ON COLUMN Bronze.bz_audit_log.load_timestamp IS 'Timestamp when the processing activity occurred';
COMMENT ON COLUMN Bronze.bz_audit_log.processed_by IS 'Identifier of the process or user that performed the activity';
COMMENT ON COLUMN Bronze.bz_audit_log.processing_time IS 'Time taken to complete the processing activity in seconds';
COMMENT ON COLUMN Bronze.bz_audit_log.status IS 'Status of the processing activity (SUCCESS, FAILED, IN_PROGRESS, etc.)';

-- ============================================================================
-- BRONZE LAYER IMPLEMENTATION NOTES
-- ============================================================================
/*
IMPLEMENTATION GUIDELINES:

1. DATA TYPES:
   - STRING: Used for all text fields to accommodate varying lengths in raw data
   - NUMBER: Used for numeric fields with appropriate precision where specified
   - DATE: Used for date-only fields
   - TIMESTAMP_NTZ: Used for datetime fields without timezone (Snowflake default)
   - AUTOINCREMENT: Used for audit table primary key generation

2. METADATA COLUMNS:
   - load_timestamp: Tracks when data was first ingested
   - update_timestamp: Tracks when data was last modified
   - source_system: Identifies the originating system

3. BRONZE LAYER PRINCIPLES:
   - No primary keys, foreign keys, or constraints as per requirements
   - Raw data storage with minimal transformation
   - Comprehensive metadata tracking for data lineage
   - Audit capabilities for monitoring and troubleshooting

4. SNOWFLAKE BEST PRACTICES:
   - CREATE TABLE IF NOT EXISTS for idempotent execution
   - Appropriate data types for Snowflake optimization
   - Comprehensive documentation through comments
   - Schema organization for clear data architecture

5. NAMING CONVENTIONS:
   - 'bz_' prefix for all Bronze layer tables
   - Descriptive table and column names
   - Consistent naming patterns across all objects

6. AUDIT CAPABILITIES:
   - Dedicated audit table for process monitoring
   - Status tracking for data quality assurance
   - Processing time metrics for performance monitoring
   - Source system tracking for data lineage
*/

-- ============================================================================
-- END OF BRONZE LAYER PHYSICAL DATA MODEL
-- ============================================================================