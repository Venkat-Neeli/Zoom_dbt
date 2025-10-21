_____________________________________________
## *Author*: AAVA
## *Created on*:   
## *Description*: Model Data Constraints for Zoom Platform Analytics System reporting requirements
## *Version*: 1 
## *Updated on*: 
_____________________________________________

# Model Data Constraints - Zoom Platform Analytics System

## 1. Data Expectations

### 1.1 Data Completeness Expectations
1. **User Data Completeness**
   - User_ID must be present for all user records
   - Plan_Type must be specified for all users (Free, Paid, Enterprise)
   - Company information should be available for enterprise users
   - Email addresses must be valid and unique across the platform

2. **Meeting Data Completeness**
   - Meeting_ID must be unique and present for all meeting records
   - Duration_Minutes must be recorded for all completed meetings
   - Start_Time and End_Time must be captured for all meetings
   - Host_ID must reference a valid user in the Users table

3. **Support Ticket Completeness**
   - Ticket_ID must be unique for each support request
   - Ticket_Type must be categorized from predefined values
   - Open_Date must be recorded when ticket is created
   - User_ID must link to existing user records

4. **Billing and License Completeness**
   - Amount must be specified for all billing events
   - License_Type must be defined for all license records
   - Start_Date and End_Date must be present for license validity
   - User assignment must be tracked for license utilization

### 1.2 Data Accuracy Expectations
1. **Temporal Data Accuracy**
   - Meeting Start_Time must precede End_Time
   - License Start_Date must be before End_Date
   - Ticket Open_Date must precede Close_Date when resolved
   - Billing Transaction_Date must reflect actual transaction timing

2. **Numerical Data Accuracy**
   - Duration_Minutes must accurately reflect actual meeting length
   - Amount values must correspond to actual billing charges
   - Usage_Count must represent actual feature utilization
   - Participant_Count must match actual meeting attendance

3. **Reference Data Accuracy**
   - All foreign key relationships must maintain referential integrity
   - User_ID references must exist in Users table
   - Meeting_ID references must exist in Meetings table

### 1.3 Data Format Expectations
1. **Standardized Formats**
   - All timestamps must follow ISO 8601 format (YYYY-MM-DD HH:MM:SS)
   - Email addresses must conform to RFC 5322 standard
   - Currency amounts must include appropriate decimal precision
   - Duration values must be expressed in consistent units (minutes)

2. **Enumerated Value Formats**
   - Plan_Type values: 'Free', 'Paid', 'Enterprise'
   - Ticket_Type values: 'audio', 'video', 'connectivity', 'billing'
   - Resolution_Status values: 'open', 'in-progress', 'resolved', 'closed'
   - License_Status values: 'active', 'expired', 'suspended'

### 1.4 Data Consistency Expectations
1. **Cross-Entity Consistency**
   - User plan types must align with license assignments
   - Meeting participants must exist as valid users
   - Feature usage must correlate with meeting sessions
   - Billing events must correspond to user plan changes

2. **Temporal Consistency**
   - User registration dates must precede meeting hosting
   - License activation must precede usage tracking
   - Support tickets must align with user activity periods

## 2. Constraints

### 2.1 Mandatory Field Constraints
1. **User Entity Mandatory Fields**
   - User_ID (Primary Key) - NOT NULL, UNIQUE
   - Email - NOT NULL, UNIQUE
   - Plan_Type - NOT NULL
   - Registration_Date - NOT NULL

2. **Meeting Entity Mandatory Fields**
   - Meeting_ID (Primary Key) - NOT NULL, UNIQUE
   - Host_ID (Foreign Key) - NOT NULL, REFERENCES Users(User_ID)
   - Start_Time - NOT NULL
   - Duration_Minutes - NOT NULL

3. **Support_Tickets Entity Mandatory Fields**
   - Ticket_ID (Primary Key) - NOT NULL, UNIQUE
   - User_ID (Foreign Key) - NOT NULL, REFERENCES Users(User_ID)
   - Ticket_Type - NOT NULL
   - Open_Date - NOT NULL
   - Resolution_Status - NOT NULL

4. **Billing_Events Entity Mandatory Fields**
   - Event_ID (Primary Key) - NOT NULL, UNIQUE
   - User_ID (Foreign Key) - NOT NULL, REFERENCES Users(User_ID)
   - Event_Type - NOT NULL
   - Amount - NOT NULL
   - Transaction_Date - NOT NULL

5. **Licenses Entity Mandatory Fields**
   - License_ID (Primary Key) - NOT NULL, UNIQUE
   - License_Type - NOT NULL
   - Start_Date - NOT NULL
   - End_Date - NOT NULL
   - License_Status - NOT NULL

### 2.2 Uniqueness Constraints
1. **Primary Key Uniqueness**
   - User_ID must be unique across Users table
   - Meeting_ID must be unique across Meetings table
   - Ticket_ID must be unique across Support_Tickets table
   - Event_ID must be unique across Billing_Events table
   - License_ID must be unique across Licenses table

2. **Business Uniqueness**
   - Email addresses must be unique across all users
   - One active license per user per license type at any given time
   - Meeting titles combined with host and start time should be unique

### 2.3 Data Type Limitations
1. **Numeric Constraints**
   - Duration_Minutes: INTEGER, >= 0, <= 1440 (24 hours max)
   - Amount: DECIMAL(10,2), > 0 for charges, can be negative for refunds
   - Usage_Count: INTEGER, >= 0
   - Participant_Count: INTEGER, >= 1

2. **String Constraints**
   - Email: VARCHAR(255), valid email format
   - Plan_Type: ENUM('Free', 'Paid', 'Enterprise')
   - Ticket_Type: ENUM('audio', 'video', 'connectivity', 'billing')
   - Resolution_Status: ENUM('open', 'in-progress', 'resolved', 'closed')

3. **Date/Time Constraints**
   - All date fields: DATETIME format
   - Registration_Date: <= CURRENT_DATE
   - Start_Time, End_Time: Valid timestamp format

### 2.4 Referential Integrity Constraints
1. **Foreign Key Relationships**
   - Meetings.Host_ID REFERENCES Users.User_ID
   - Attendees.User_ID REFERENCES Users.User_ID
   - Attendees.Meeting_ID REFERENCES Meetings.Meeting_ID
   - Features_Usage.Meeting_ID REFERENCES Meetings.Meeting_ID
   - Support_Tickets.User_ID REFERENCES Users.User_ID
   - Billing_Events.User_ID REFERENCES Users.User_ID
   - Licenses.Assigned_To_User_ID REFERENCES Users.User_ID

2. **Cascade Rules**
   - ON DELETE RESTRICT for Users (cannot delete if referenced)
   - ON UPDATE CASCADE for User_ID changes
   - ON DELETE CASCADE for Meeting deletion (removes attendees and features)

### 2.5 Business Logic Constraints
1. **Temporal Business Constraints**
   - Meeting End_Time must be >= Start_Time
   - License End_Date must be > Start_Date
   - Ticket Close_Date must be >= Open_Date when status is 'resolved' or 'closed'

2. **Value Range Constraints**
   - Meeting duration cannot exceed 24 hours (1440 minutes)
   - License duration cannot exceed 10 years
   - Support ticket priority levels must be within defined range

## 3. Business Rules

### 3.1 User Management Rules
1. **User Registration Rules**
   - New users default to 'Free' plan type unless specified otherwise
   - Email verification required before account activation
   - Company field mandatory for 'Enterprise' plan users
   - User accounts cannot be deleted if they have associated meetings or billing events

2. **User Activity Rules**
   - Users must be active to host meetings
   - Inactive users (no login > 365 days) flagged for review
   - Plan upgrades take effect immediately
   - Plan downgrades take effect at next billing cycle

### 3.2 Meeting Management Rules
1. **Meeting Creation Rules**
   - Only registered users can host meetings
   - Free plan users limited to 40-minute meeting duration
   - Enterprise users have unlimited meeting duration
   - Meeting capacity limits based on plan type

2. **Meeting Participation Rules**
   - Attendees can join meetings without registration (guest access)
   - Meeting hosts must be authenticated users
   - Meeting recordings available based on plan permissions
   - Feature usage tracked only for authenticated participants

### 3.3 Support Ticket Rules
1. **Ticket Creation Rules**
   - Support tickets can only be created by registered users
   - Ticket priority auto-assigned based on user plan type
   - Enterprise users receive 'high' priority by default
   - Free users receive 'low' priority by default

2. **Ticket Resolution Rules**
   - Tickets must progress through defined status workflow
   - Resolution time SLA varies by user plan type
   - Enterprise: 4 hours, Paid: 24 hours, Free: 72 hours
   - Tickets auto-close after 7 days of inactivity in 'resolved' status

### 3.4 Billing and License Rules
1. **Billing Event Rules**
   - Billing events automatically generated for plan changes
   - Refunds require manager approval for amounts > $100
   - Failed payments trigger account suspension after 3 attempts
   - Billing currency determined by user's registered country

2. **License Management Rules**
   - License assignments must not exceed purchased seat count
   - License expiration triggers automatic renewal for active subscriptions
   - Expired licenses result in plan downgrade to 'Free'
   - License transfers between users require admin approval

### 3.5 Data Retention Rules
1. **Meeting Data Retention**
   - Meeting metadata retained for 7 years
   - Meeting recordings retained based on plan: Free (30 days), Paid (1 year), Enterprise (unlimited)
   - Deleted meetings marked as 'archived' rather than physical deletion

2. **Support Data Retention**
   - Support tickets retained for 3 years after closure
   - Ticket attachments purged after 1 year
   - Customer communication logs retained for compliance

3. **Financial Data Retention**
   - Billing events retained for 10 years for tax compliance
   - License history maintained for audit purposes
   - Payment method information encrypted and retained per PCI requirements

### 3.6 Reporting Logic Rules
1. **KPI Calculation Rules**
   - Active users defined as users with at least one meeting in measurement period
   - Meeting minutes calculated from actual start/end times, not scheduled duration
   - Feature adoption rate calculated as unique users using feature / total active users
   - Revenue recognition follows subscription accounting principles

2. **Data Aggregation Rules**
   - Daily metrics calculated using UTC timezone
   - Monthly metrics use calendar month boundaries
   - Year-over-year comparisons use same calendar periods
   - Partial period data clearly labeled in reports

### 3.7 Data Quality Rules
1. **Data Validation Rules**
   - All imported data must pass schema validation
   - Duplicate detection runs daily on user email addresses
   - Orphaned records (missing foreign key references) flagged for cleanup
   - Data anomalies (e.g., meetings longer than 24 hours) require investigation

2. **Data Correction Rules**
   - Historical data corrections require approval workflow
   - Data corrections must maintain audit trail
   - Bulk data updates require backup before execution
   - Critical data changes require dual approval