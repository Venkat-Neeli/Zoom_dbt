_____________________________________________
## *Author*: AAVA
## *Created on*:   
## *Description*: Bronze layer physical data model for Zoom Platform Analytics System supporting medallion architecture with Snowflake SQL DDL scripts
## *Version*: 1 
## *Updated on*: 
_____________________________________________

/*
=============================================================================
BRONZE LAYER PHYSICAL DATA MODEL - ZOOM PLATFORM ANALYTICS SYSTEM
=============================================================================

Author: AAVA
Description: Comprehensive Bronze Layer Physical Data Model for Zoom Platform Analytics System
             implementing Medallion Architecture with raw data storage capabilities
Version: 1.0
Created Date: 2024-01-15
Snowflake Compatibility: Optimized for Snowflake SQL with micro-partitioned storage

Purpose: This Bronze layer serves as the raw data ingestion layer in the Medallion 
         architecture, storing unprocessed data from various Zoom platform sources
         with metadata tracking for data lineage and audit purposes.

Key Features:
- Raw data storage without constraints or transformations
- Metadata columns for data lineage tracking
- Snowflake-optimized data types and structures
- Audit table for processing monitoring
- Compatible with Snowflake's micro-partitioned storage

=============================================================================
*/

-- =============================================================================
-- BRONZE LAYER DDL SCRIPT SECTION
-- =============================================================================

-- 1. Bronze Users Table
-- Stores raw user data from Zoom platform
CREATE TABLE IF NOT EXISTS Bronze.bz_users (
    user_id STRING,
    user_name STRING,
    email STRING,
    company STRING,
    plan_type STRING,
    load_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    update_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    source_system STRING DEFAULT 'ZOOM_API'
);

-- 2. Bronze Meetings Table
-- Stores raw meeting data with host and timing information
CREATE TABLE IF NOT EXISTS Bronze.bz_meetings (
    meeting_id STRING,
    host_id STRING,
    meeting_topic STRING,
    start_time TIMESTAMP_NTZ,
    end_time TIMESTAMP_NTZ,
    duration_minutes NUMBER(10,0),
    load_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    update_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    source_system STRING DEFAULT 'ZOOM_API'
);

-- 3. Bronze Participants Table
-- Stores raw participant data for meeting attendance tracking
CREATE TABLE IF NOT EXISTS Bronze.bz_participants (
    participant_id STRING,
    meeting_id STRING,
    user_id STRING,
    join_time TIMESTAMP_NTZ,
    leave_time TIMESTAMP_NTZ,
    load_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    update_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    source_system STRING DEFAULT 'ZOOM_API'
);

-- 4. Bronze Feature Usage Table
-- Stores raw feature usage data for analytics
CREATE TABLE IF NOT EXISTS Bronze.bz_feature_usage (
    usage_id STRING,
    meeting_id STRING,
    feature_name STRING,
    usage_count NUMBER(10,0),
    usage_date DATE,
    load_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    update_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    source_system STRING DEFAULT 'ZOOM_API'
);

-- 5. Bronze Webinars Table
-- Stores raw webinar data with registration information
CREATE TABLE IF NOT EXISTS Bronze.bz_webinars (
    webinar_id STRING,
    host_id STRING,
    webinar_topic STRING,
    start_time TIMESTAMP_NTZ,
    end_time TIMESTAMP_NTZ,
    registrants NUMBER(10,0),
    load_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    update_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    source_system STRING DEFAULT 'ZOOM_API'
);

-- 6. Bronze Support Tickets Table
-- Stores raw support ticket data for customer service analytics
CREATE TABLE IF NOT EXISTS Bronze.bz_support_tickets (
    ticket_id STRING,
    user_id STRING,
    ticket_type STRING,
    resolution_status STRING,
    open_date DATE,
    load_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    update_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    source_system STRING DEFAULT 'ZOOM_SUPPORT_SYSTEM'
);

-- 7. Bronze Licenses Table
-- Stores raw license data for user entitlement tracking
CREATE TABLE IF NOT EXISTS Bronze.bz_licenses (
    license_id STRING,
    license_type STRING,
    assigned_to_user_id STRING,
    start_date DATE,
    end_date DATE,
    load_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    update_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    source_system STRING DEFAULT 'ZOOM_LICENSE_SYSTEM'
);

-- 8. Bronze Billing Events Table
-- Stores raw billing and financial event data
CREATE TABLE IF NOT EXISTS Bronze.bz_billing_events (
    event_id STRING,
    user_id STRING,
    event_type STRING,
    amount DECIMAL(10,2),
    event_date DATE,
    load_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    update_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    source_system STRING DEFAULT 'ZOOM_BILLING_SYSTEM'
);

-- 9. Bronze Audit Table
-- Stores processing metadata and audit information for data lineage
CREATE TABLE IF NOT EXISTS Bronze.bz_audit_log (
    record_id NUMBER AUTOINCREMENT,
    source_table STRING,
    load_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    processed_by STRING,
    processing_time NUMBER(10,3),
    status STRING,
    error_message STRING,
    records_processed NUMBER(15,0),
    records_failed NUMBER(15,0)
);

-- =============================================================================
-- BRONZE LAYER TABLE COMMENTS FOR DOCUMENTATION
-- =============================================================================

COMMENT ON TABLE Bronze.bz_users IS 'Bronze layer table storing raw user data from Zoom platform including user profiles, company information, and plan types';
COMMENT ON TABLE Bronze.bz_meetings IS 'Bronze layer table storing raw meeting data including meeting details, host information, and duration metrics';
COMMENT ON TABLE Bronze.bz_participants IS 'Bronze layer table storing raw participant data for meeting attendance tracking and join/leave timestamps';
COMMENT ON TABLE Bronze.bz_feature_usage IS 'Bronze layer table storing raw feature usage data for analytics on Zoom platform feature adoption';
COMMENT ON TABLE Bronze.bz_webinars IS 'Bronze layer table storing raw webinar data including webinar details, host information, and registration counts';
COMMENT ON TABLE Bronze.bz_support_tickets IS 'Bronze layer table storing raw support ticket data for customer service analytics and resolution tracking';
COMMENT ON TABLE Bronze.bz_licenses IS 'Bronze layer table storing raw license data for user entitlement tracking and license management';
COMMENT ON TABLE Bronze.bz_billing_events IS 'Bronze layer table storing raw billing and financial event data for revenue analytics';
COMMENT ON TABLE Bronze.bz_audit_log IS 'Bronze layer audit table storing processing metadata and data lineage information for all Bronze layer operations';

-- =============================================================================
-- BRONZE LAYER COLUMN COMMENTS FOR DETAILED DOCUMENTATION
-- =============================================================================

-- Users Table Column Comments
COMMENT ON COLUMN Bronze.bz_users.user_id IS 'Unique identifier for Zoom user';
COMMENT ON COLUMN Bronze.bz_users.user_name IS 'Display name of the Zoom user';
COMMENT ON COLUMN Bronze.bz_users.email IS 'Email address of the Zoom user';
COMMENT ON COLUMN Bronze.bz_users.company IS 'Company or organization name associated with the user';
COMMENT ON COLUMN Bronze.bz_users.plan_type IS 'Zoom subscription plan type (Basic, Pro, Business, Enterprise)';
COMMENT ON COLUMN Bronze.bz_users.load_timestamp IS 'Timestamp when record was first loaded into Bronze layer';
COMMENT ON COLUMN Bronze.bz_users.update_timestamp IS 'Timestamp when record was last updated in Bronze layer';
COMMENT ON COLUMN Bronze.bz_users.source_system IS 'Source system identifier for data lineage tracking';

-- Meetings Table Column Comments
COMMENT ON COLUMN Bronze.bz_meetings.meeting_id IS 'Unique identifier for Zoom meeting';
COMMENT ON COLUMN Bronze.bz_meetings.host_id IS 'User ID of the meeting host';
COMMENT ON COLUMN Bronze.bz_meetings.meeting_topic IS 'Topic or title of the meeting';
COMMENT ON COLUMN Bronze.bz_meetings.start_time IS 'Meeting start timestamp';
COMMENT ON COLUMN Bronze.bz_meetings.end_time IS 'Meeting end timestamp';
COMMENT ON COLUMN Bronze.bz_meetings.duration_minutes IS 'Meeting duration in minutes';
COMMENT ON COLUMN Bronze.bz_meetings.load_timestamp IS 'Timestamp when record was first loaded into Bronze layer';
COMMENT ON COLUMN Bronze.bz_meetings.update_timestamp IS 'Timestamp when record was last updated in Bronze layer';
COMMENT ON COLUMN Bronze.bz_meetings.source_system IS 'Source system identifier for data lineage tracking';

-- Participants Table Column Comments
COMMENT ON COLUMN Bronze.bz_participants.participant_id IS 'Unique identifier for meeting participant';
COMMENT ON COLUMN Bronze.bz_participants.meeting_id IS 'Meeting ID that participant joined';
COMMENT ON COLUMN Bronze.bz_participants.user_id IS 'User ID of the participant';
COMMENT ON COLUMN Bronze.bz_participants.join_time IS 'Timestamp when participant joined the meeting';
COMMENT ON COLUMN Bronze.bz_participants.leave_time IS 'Timestamp when participant left the meeting';
COMMENT ON COLUMN Bronze.bz_participants.load_timestamp IS 'Timestamp when record was first loaded into Bronze layer';
COMMENT ON COLUMN Bronze.bz_participants.update_timestamp IS 'Timestamp when record was last updated in Bronze layer';
COMMENT ON COLUMN Bronze.bz_participants.source_system IS 'Source system identifier for data lineage tracking';

-- Feature Usage Table Column Comments
COMMENT ON COLUMN Bronze.bz_feature_usage.usage_id IS 'Unique identifier for feature usage record';
COMMENT ON COLUMN Bronze.bz_feature_usage.meeting_id IS 'Meeting ID where feature was used';
COMMENT ON COLUMN Bronze.bz_feature_usage.feature_name IS 'Name of the Zoom feature used';
COMMENT ON COLUMN Bronze.bz_feature_usage.usage_count IS 'Number of times feature was used';
COMMENT ON COLUMN Bronze.bz_feature_usage.usage_date IS 'Date when feature usage occurred';
COMMENT ON COLUMN Bronze.bz_feature_usage.load_timestamp IS 'Timestamp when record was first loaded into Bronze layer';
COMMENT ON COLUMN Bronze.bz_feature_usage.update_timestamp IS 'Timestamp when record was last updated in Bronze layer';
COMMENT ON COLUMN Bronze.bz_feature_usage.source_system IS 'Source system identifier for data lineage tracking';

-- Webinars Table Column Comments
COMMENT ON COLUMN Bronze.bz_webinars.webinar_id IS 'Unique identifier for Zoom webinar';
COMMENT ON COLUMN Bronze.bz_webinars.host_id IS 'User ID of the webinar host';
COMMENT ON COLUMN Bronze.bz_webinars.webinar_topic IS 'Topic or title of the webinar';
COMMENT ON COLUMN Bronze.bz_webinars.start_time IS 'Webinar start timestamp';
COMMENT ON COLUMN Bronze.bz_webinars.end_time IS 'Webinar end timestamp';
COMMENT ON COLUMN Bronze.bz_webinars.registrants IS 'Number of webinar registrants';
COMMENT ON COLUMN Bronze.bz_webinars.load_timestamp IS 'Timestamp when record was first loaded into Bronze layer';
COMMENT ON COLUMN Bronze.bz_webinars.update_timestamp IS 'Timestamp when record was last updated in Bronze layer';
COMMENT ON COLUMN Bronze.bz_webinars.source_system IS 'Source system identifier for data lineage tracking';

-- Support Tickets Table Column Comments
COMMENT ON COLUMN Bronze.bz_support_tickets.ticket_id IS 'Unique identifier for support ticket';
COMMENT ON COLUMN Bronze.bz_support_tickets.user_id IS 'User ID who created the support ticket';
COMMENT ON COLUMN Bronze.bz_support_tickets.ticket_type IS 'Category or type of support ticket';
COMMENT ON COLUMN Bronze.bz_support_tickets.resolution_status IS 'Current status of ticket resolution';
COMMENT ON COLUMN Bronze.bz_support_tickets.open_date IS 'Date when support ticket was opened';
COMMENT ON COLUMN Bronze.bz_support_tickets.load_timestamp IS 'Timestamp when record was first loaded into Bronze layer';
COMMENT ON COLUMN Bronze.bz_support_tickets.update_timestamp IS 'Timestamp when record was last updated in Bronze layer';
COMMENT ON COLUMN Bronze.bz_support_tickets.source_system IS 'Source system identifier for data lineage tracking';

-- Licenses Table Column Comments
COMMENT ON COLUMN Bronze.bz_licenses.license_id IS 'Unique identifier for Zoom license';
COMMENT ON COLUMN Bronze.bz_licenses.license_type IS 'Type of Zoom license (Basic, Pro, Business, Enterprise)';
COMMENT ON COLUMN Bronze.bz_licenses.assigned_to_user_id IS 'User ID to whom license is assigned';
COMMENT ON COLUMN Bronze.bz_licenses.start_date IS 'License validity start date';
COMMENT ON COLUMN Bronze.bz_licenses.end_date IS 'License validity end date';
COMMENT ON COLUMN Bronze.bz_licenses.load_timestamp IS 'Timestamp when record was first loaded into Bronze layer';
COMMENT ON COLUMN Bronze.bz_licenses.update_timestamp IS 'Timestamp when record was last updated in Bronze layer';
COMMENT ON COLUMN Bronze.bz_licenses.source_system IS 'Source system identifier for data lineage tracking';

-- Billing Events Table Column Comments
COMMENT ON COLUMN Bronze.bz_billing_events.event_id IS 'Unique identifier for billing event';
COMMENT ON COLUMN Bronze.bz_billing_events.user_id IS 'User ID associated with billing event';
COMMENT ON COLUMN Bronze.bz_billing_events.event_type IS 'Type of billing event (charge, refund, credit, etc.)';
COMMENT ON COLUMN Bronze.bz_billing_events.amount IS 'Monetary amount of billing event';
COMMENT ON COLUMN Bronze.bz_billing_events.event_date IS 'Date when billing event occurred';
COMMENT ON COLUMN Bronze.bz_billing_events.load_timestamp IS 'Timestamp when record was first loaded into Bronze layer';
COMMENT ON COLUMN Bronze.bz_billing_events.update_timestamp IS 'Timestamp when record was last updated in Bronze layer';
COMMENT ON COLUMN Bronze.bz_billing_events.source_system IS 'Source system identifier for data lineage tracking';

-- Audit Log Table Column Comments
COMMENT ON COLUMN Bronze.bz_audit_log.record_id IS 'Auto-incrementing unique identifier for audit record';
COMMENT ON COLUMN Bronze.bz_audit_log.source_table IS 'Name of the Bronze table being processed';
COMMENT ON COLUMN Bronze.bz_audit_log.load_timestamp IS 'Timestamp when processing operation started';
COMMENT ON COLUMN Bronze.bz_audit_log.processed_by IS 'User or system that performed the processing';
COMMENT ON COLUMN Bronze.bz_audit_log.processing_time IS 'Time taken for processing operation in seconds';
COMMENT ON COLUMN Bronze.bz_audit_log.status IS 'Status of processing operation (SUCCESS, FAILED, PARTIAL)';
COMMENT ON COLUMN Bronze.bz_audit_log.error_message IS 'Error message if processing failed';
COMMENT ON COLUMN Bronze.bz_audit_log.records_processed IS 'Number of records successfully processed';
COMMENT ON COLUMN Bronze.bz_audit_log.records_failed IS 'Number of records that failed processing';

-- =============================================================================
-- BRONZE LAYER IMPLEMENTATION NOTES
-- =============================================================================

/*
IMPLEMENTATION NOTES:

1. DATA TYPES MAPPING:
   - All VARCHAR fields from source mapped to STRING for Snowflake optimization
   - DATETIME fields mapped to TIMESTAMP_NTZ for consistent timezone handling
   - INT fields mapped to NUMBER(10,0) for integer values
   - DECIMAL fields mapped to DECIMAL(10,2) for monetary amounts
   - DATE fields remain as DATE type

2. BRONZE LAYER PRINCIPLES:
   - No primary keys, foreign keys, or constraints (raw data storage)
   - All ID fields stored as regular STRING columns
   - Metadata columns added for data lineage and audit tracking
   - Default values set for load_timestamp and source_system

3. SNOWFLAKE OPTIMIZATIONS:
   - Uses Snowflake's micro-partitioned storage (default)
   - TIMESTAMP_NTZ used for consistent timezone handling
   - STRING data type for optimal storage and performance
   - AUTOINCREMENT for audit table record_id

4. AUDIT AND LINEAGE:
   - load_timestamp: When record first entered Bronze layer
   - update_timestamp: When record was last modified
   - source_system: Identifies the originating system
   - bz_audit_log: Tracks all processing operations

5. NAMING CONVENTIONS:
   - All tables prefixed with 'bz_' for Bronze layer identification
   - Schema: Bronze (to be created separately)
   - Column names use snake_case for consistency

6. EXTENSIBILITY:
   - Structure allows for easy addition of new columns
   - Metadata columns support future data governance requirements
   - Audit table supports comprehensive monitoring

This Bronze layer implementation provides a solid foundation for the Medallion
architecture, enabling efficient raw data ingestion while maintaining full
data lineage and audit capabilities.
*/

-- =============================================================================
-- END OF BRONZE LAYER PHYSICAL DATA MODEL
-- =============================================================================