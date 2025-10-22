_____________________________________________
## *Author*: AAVA
## *Created on*:   
## *Description*: Bronze Layer Physical Data Model for Zoom Platform Analytics System supporting raw data ingestion with Snowflake compatibility
## *Version*: 1 
## *Updated on*: 
_____________________________________________

-- =====================================================
-- BRONZE LAYER PHYSICAL DATA MODEL
-- Zoom Platform Analytics System
-- Snowflake Compatible DDL Scripts
-- =====================================================

-- =====================================================
-- 1. BRONZE LAYER DDL SCRIPTS
-- =====================================================

-- -----------------------------------------------------
-- 1.1 Users Table
-- -----------------------------------------------------
CREATE TABLE IF NOT EXISTS Bronze.bz_users (
    user_id STRING,
    user_name STRING,
    email STRING,
    company STRING,
    plan_type STRING,
    registration_date DATE,
    account_status STRING,
    -- Metadata columns
    load_timestamp TIMESTAMP_NTZ,
    update_timestamp TIMESTAMP_NTZ,
    source_system STRING
);

-- -----------------------------------------------------
-- 1.2 Meetings Table
-- -----------------------------------------------------
CREATE TABLE IF NOT EXISTS Bronze.bz_meetings (
    meeting_id STRING,
    host_id STRING,
    meeting_topic STRING,
    meeting_title STRING,
    start_time TIMESTAMP_NTZ,
    end_time TIMESTAMP_NTZ,
    duration_minutes NUMBER,
    host_name STRING,
    meeting_type STRING,
    participant_count NUMBER,
    -- Metadata columns
    load_timestamp TIMESTAMP_NTZ,
    update_timestamp TIMESTAMP_NTZ,
    source_system STRING
);

-- -----------------------------------------------------
-- 1.3 Participants/Attendees Table
-- -----------------------------------------------------
CREATE TABLE IF NOT EXISTS Bronze.bz_participants (
    participant_id STRING,
    meeting_id STRING,
    user_id STRING,
    attendee_name STRING,
    join_time TIMESTAMP_NTZ,
    leave_time TIMESTAMP_NTZ,
    attendance_duration NUMBER,
    attendee_type STRING,
    -- Metadata columns
    load_timestamp TIMESTAMP_NTZ,
    update_timestamp TIMESTAMP_NTZ,
    source_system STRING
);

-- -----------------------------------------------------
-- 1.4 Feature Usage Table
-- -----------------------------------------------------
CREATE TABLE IF NOT EXISTS Bronze.bz_feature_usage (
    usage_id STRING,
    meeting_id STRING,
    feature_name STRING,
    usage_count NUMBER,
    usage_duration NUMBER,
    usage_date DATE,
    feature_category STRING,
    -- Metadata columns
    load_timestamp TIMESTAMP_NTZ,
    update_timestamp TIMESTAMP_NTZ,
    source_system STRING
);

-- -----------------------------------------------------
-- 1.5 Webinars Table
-- -----------------------------------------------------
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

-- -----------------------------------------------------
-- 1.6 Support Tickets Table
-- -----------------------------------------------------
CREATE TABLE IF NOT EXISTS Bronze.bz_support_tickets (
    ticket_id STRING,
    user_id STRING,
    ticket_type STRING,
    resolution_status STRING,
    open_date DATE,
    close_date DATE,
    priority_level STRING,
    issue_description STRING,
    resolution_notes STRING,
    -- Metadata columns
    load_timestamp TIMESTAMP_NTZ,
    update_timestamp TIMESTAMP_NTZ,
    source_system STRING
);

-- -----------------------------------------------------
-- 1.7 Licenses Table
-- -----------------------------------------------------
CREATE TABLE IF NOT EXISTS Bronze.bz_licenses (
    license_id STRING,
    license_type STRING,
    assigned_to_user_id STRING,
    start_date DATE,
    end_date DATE,
    assignment_status STRING,
    license_capacity NUMBER,
    -- Metadata columns
    load_timestamp TIMESTAMP_NTZ,
    update_timestamp TIMESTAMP_NTZ,
    source_system STRING
);

-- -----------------------------------------------------
-- 1.8 Billing Events Table
-- -----------------------------------------------------
CREATE TABLE IF NOT EXISTS Bronze.bz_billing_events (
    event_id STRING,
    user_id STRING,
    event_type STRING,
    amount NUMBER(10,2),
    event_date DATE,
    transaction_date DATE,
    currency STRING,
    payment_method STRING,
    billing_cycle STRING,
    -- Metadata columns
    load_timestamp TIMESTAMP_NTZ,
    update_timestamp TIMESTAMP_NTZ,
    source_system STRING
);

-- -----------------------------------------------------
-- 1.9 Audit Table
-- -----------------------------------------------------
CREATE TABLE IF NOT EXISTS Bronze.bz_audit_log (
    record_id NUMBER AUTOINCREMENT,
    source_table STRING,
    load_timestamp TIMESTAMP_NTZ,
    processed_by STRING,
    processing_time NUMBER,
    status STRING
);

-- =====================================================
-- 2. BRONZE LAYER TABLE DESCRIPTIONS
-- =====================================================

/*
1. bz_users: Stores raw user profile and subscription information
2. bz_meetings: Contains meeting session data and metadata
3. bz_participants: Tracks meeting attendance and participation details
4. bz_feature_usage: Records platform feature utilization during meetings
5. bz_webinars: Stores webinar-specific information and metrics
6. bz_support_tickets: Contains customer support request and resolution data
7. bz_licenses: Manages software license assignments and validity
8. bz_billing_events: Records all financial transactions and billing activities
9. bz_audit_log: Tracks data processing activities and system operations
*/

-- =====================================================
-- 3. BRONZE LAYER DESIGN PRINCIPLES
-- =====================================================

/*
Design Principles Applied:
1. Raw Data Storage: Tables store data as-is from source systems
2. Snowflake Compatibility: Uses Snowflake-native data types (STRING, NUMBER, TIMESTAMP_NTZ, etc.)
3. No Constraints: No primary keys, foreign keys, or constraints for Bronze layer flexibility
4. Metadata Enrichment: All tables include load_timestamp, update_timestamp, and source_system
5. Audit Trail: Dedicated audit table for tracking data processing activities
6. Naming Convention: All tables prefixed with 'bz_' for Bronze layer identification
7. Schema Organization: All tables created in 'Bronze' schema for clear layer separation
8. Micro-partitioned Storage: Leverages Snowflake's default storage format
9. Scalable Architecture: Designed to support high-volume data ingestion
10. Data Lineage: Supports traceability through metadata columns
*/

-- =====================================================
-- END OF BRONZE LAYER PHYSICAL DATA MODEL
-- =====================================================