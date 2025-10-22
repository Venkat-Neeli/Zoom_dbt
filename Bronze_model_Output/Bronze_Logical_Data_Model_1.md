_____________________________________________
## *Author*: AAVA
## *Created on*:   
## *Description*: Bronze layer logical data model for Zoom Platform Analytics System supporting medallion architecture
## *Version*: 1 
## *Updated on*: 
_____________________________________________

# Bronze Layer Logical Data Model for Zoom Platform Analytics System

## 1. PII Classification

### 1.1 Identified PII Fields

| Table Name | Column Name | PII Classification | Reason for PII Classification |
|------------|-------------|-------------------|------------------------------|
| Bz_Users | User_Name | **Sensitive PII** | Contains personal identifiable information - full name of individuals |
| Bz_Users | Email | **Sensitive PII** | Contains personal email addresses which can directly identify individuals |
| Bz_Users | Company | **Non-Sensitive PII** | Company information may indirectly identify individuals in small organizations |
| Bz_Meetings | Meeting_Topic | **Potentially Sensitive** | May contain confidential business information or personal details |
| Bz_Participants | Join_Time | **Non-Sensitive PII** | When combined with other data, can create behavioral patterns |
| Bz_Participants | Leave_Time | **Non-Sensitive PII** | When combined with other data, can create behavioral patterns |
| Bz_Support_Tickets | Ticket_Type | **Potentially Sensitive** | May reveal personal or business issues |
| Bz_Webinars | Webinar_Topic | **Potentially Sensitive** | May contain confidential business information |

## 2. Bronze Layer Logical Model

### 2.1 Bz_Users
**Description:** Bronze layer table storing raw user profile information from source systems without transformation

| Column Name | Business Description | Data Type | 
|-------------|---------------------|-----------|
| User_Name | Full name of the registered user for identification purposes | VARCHAR(255) |
| Email | Primary email address used for account registration | VARCHAR(255) |
| Company | Organization name associated with the user account | VARCHAR(255) |
| Plan_Type | Subscription tier indicating service level and feature access | VARCHAR(50) |
| load_timestamp | Timestamp when record was loaded into Bronze layer | TIMESTAMP |
| update_timestamp | Timestamp when record was last updated | TIMESTAMP |
| source_system | Source system identifier from which data originated | VARCHAR(100) |

### 2.2 Bz_Meetings
**Description:** Bronze layer table capturing raw meeting session data and metadata

| Column Name | Business Description | Data Type |
|-------------|---------------------|-----------|
| Meeting_Topic | Name or subject of the meeting for identification | VARCHAR(255) |
| Start_Time | Timestamp when the meeting began | DATETIME |
| End_Time | Timestamp when the meeting concluded | DATETIME |
| Duration_Minutes | Total length of the meeting in minutes | INT |
| load_timestamp | Timestamp when record was loaded into Bronze layer | TIMESTAMP |
| update_timestamp | Timestamp when record was last updated | TIMESTAMP |
| source_system | Source system identifier from which data originated | VARCHAR(100) |

### 2.3 Bz_Participants
**Description:** Bronze layer table storing raw participant attendance data for meetings

| Column Name | Business Description | Data Type |
|-------------|---------------------|-----------|
| Join_Time | Timestamp when the participant joined the meeting | DATETIME |
| Leave_Time | Timestamp when the participant left the meeting | DATETIME |
| load_timestamp | Timestamp when record was loaded into Bronze layer | TIMESTAMP |
| update_timestamp | Timestamp when record was last updated | TIMESTAMP |
| source_system | Source system identifier from which data originated | VARCHAR(100) |

### 2.4 Bz_Feature_Usage
**Description:** Bronze layer table capturing raw feature utilization data during meetings

| Column Name | Business Description | Data Type |
|-------------|---------------------|-----------|
| Feature_Name | Name of the platform feature used during meeting | VARCHAR(100) |
| Usage_Count | Number of times the feature was utilized | INT |
| Usage_Date | Date when the feature was used | DATE |
| load_timestamp | Timestamp when record was loaded into Bronze layer | TIMESTAMP |
| update_timestamp | Timestamp when record was last updated | TIMESTAMP |
| source_system | Source system identifier from which data originated | VARCHAR(100) |

### 2.5 Bz_Webinars
**Description:** Bronze layer table storing raw webinar session information and registration data

| Column Name | Business Description | Data Type |
|-------------|---------------------|-----------|
| Webinar_Topic | Topic or title of the webinar session | VARCHAR(255) |
| Start_Time | Timestamp when the webinar began | DATETIME |
| End_Time | Timestamp when the webinar ended | DATETIME |
| Registrants | Total number of users who registered for the webinar | INT |
| load_timestamp | Timestamp when record was loaded into Bronze layer | TIMESTAMP |
| update_timestamp | Timestamp when record was last updated | TIMESTAMP |
| source_system | Source system identifier from which data originated | VARCHAR(100) |

### 2.6 Bz_Support_Tickets
**Description:** Bronze layer table containing raw customer support request and issue data

| Column Name | Business Description | Data Type |
|-------------|---------------------|-----------|
| Ticket_Type | Category of the support issue or request | VARCHAR(100) |
| Resolution_Status | Current state of the support ticket | VARCHAR(50) |
| Open_Date | Date when the support ticket was created | DATE |
| load_timestamp | Timestamp when record was loaded into Bronze layer | TIMESTAMP |
| update_timestamp | Timestamp when record was last updated | TIMESTAMP |
| source_system | Source system identifier from which data originated | VARCHAR(100) |

### 2.7 Bz_Licenses
**Description:** Bronze layer table storing raw software license assignment and validity information

| Column Name | Business Description | Data Type |
|-------------|---------------------|-----------|
| License_Type | Category of software license granted | VARCHAR(50) |
| Start_Date | Date when the license became active | DATE |
| End_Date | Date when the license expires | DATE |
| load_timestamp | Timestamp when record was loaded into Bronze layer | TIMESTAMP |
| update_timestamp | Timestamp when record was last updated | TIMESTAMP |
| source_system | Source system identifier from which data originated | VARCHAR(100) |

### 2.8 Bz_Billing_Events
**Description:** Bronze layer table capturing raw billing transaction and financial event data

| Column Name | Business Description | Data Type |
|-------------|---------------------|-----------|
| Event_Type | Type of billing transaction or financial event | VARCHAR(100) |
| Amount | Monetary value of the billing transaction | DECIMAL(10, 2) |
| Event_Date | Date when the billing event occurred | DATE |
| load_timestamp | Timestamp when record was loaded into Bronze layer | TIMESTAMP |
| update_timestamp | Timestamp when record was last updated | TIMESTAMP |
| source_system | Source system identifier from which data originated | VARCHAR(100) |

## 3. Audit Table Design

### 3.1 Bz_Audit_Log
**Description:** Comprehensive audit trail for tracking all data processing activities in the Bronze layer

| Column Name | Business Description | Data Type |
|-------------|---------------------|-----------|
| record_id | Unique identifier for each audit record | VARCHAR(50) |
| source_table | Name of the source table being processed | VARCHAR(100) |
| load_timestamp | Timestamp when the data load process began | TIMESTAMP |
| processed_by | Identifier of the process or user performing the operation | VARCHAR(100) |
| processing_time | Duration of the processing operation in seconds | INT |
| status | Status of the processing operation | VARCHAR(50) |

**Status Domain Values:** 'SUCCESS', 'FAILED', 'IN_PROGRESS', 'PARTIAL_SUCCESS'

## 4. Conceptual Data Model Diagram

### 4.1 Entity Relationship Block Diagram

```
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│   Bz_Users      │────│   Bz_Meetings   │────│ Bz_Participants │
│                 │    │                 │    │                 │
│ • User_Name     │    │ • Meeting_Topic │    │ • Join_Time     │
│ • Email         │    │ • Start_Time    │    │ • Leave_Time    │
│ • Company       │    │ • End_Time      │    │                 │
│ • Plan_Type     │    │ • Duration_Min  │    │                 │
└─────────────────┘    └─────────────────┘    └─────────────────┘
         │                       │
         │                       │
         │              ┌─────────────────┐
         │              │Bz_Feature_Usage │
         │              │                 │
         │              │ • Feature_Name  │
         │              │ • Usage_Count   │
         │              │ • Usage_Date    │
         │              └─────────────────┘
         │
         │
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│Bz_Support_Tickets│    │   Bz_Licenses   │    │Bz_Billing_Events│
│                 │    │                 │    │                 │
│ • Ticket_Type   │    │ • License_Type  │    │ • Event_Type    │
│ • Resolution_St │    │ • Start_Date    │    │ • Amount        │
│ • Open_Date     │    │ • End_Date      │    │ • Event_Date    │
└─────────────────┘    └─────────────────┘    └─────────────────┘

         ┌─────────────────┐
         │   Bz_Webinars   │
         │                 │
         │ • Webinar_Topic │
         │ • Start_Time    │
         │ • End_Time      │
         │ • Registrants   │
         └─────────────────┘
```

### 4.2 Table Relationships

| Source Table | Connected To | Connection Field | Relationship Type |
|--------------|--------------|------------------|-------------------|
| Bz_Users | Bz_Meetings | User_Name → Meeting_Topic | One-to-Many (User hosts multiple meetings) |
| Bz_Meetings | Bz_Participants | Meeting_Topic → Join_Time/Leave_Time | One-to-Many (Meeting has multiple participants) |
| Bz_Meetings | Bz_Feature_Usage | Meeting_Topic → Feature_Name | One-to-Many (Meeting has multiple feature usage records) |
| Bz_Users | Bz_Support_Tickets | User_Name → Ticket_Type | One-to-Many (User creates multiple tickets) |
| Bz_Users | Bz_Licenses | User_Name → License_Type | One-to-Many (User assigned multiple licenses) |
| Bz_Users | Bz_Billing_Events | User_Name → Event_Type | One-to-Many (User has multiple billing events) |
| Bz_Users | Bz_Webinars | User_Name → Webinar_Topic | One-to-Many (User hosts multiple webinars) |

### 4.3 Design Rationale

1. **Bronze Layer Philosophy:** All tables maintain raw data structure from source systems with minimal transformation
2. **Naming Convention:** 'Bz_' prefix clearly identifies Bronze layer tables in the medallion architecture
3. **Metadata Columns:** Standard metadata columns enable data lineage tracking and processing monitoring
4. **PII Classification:** Systematic identification of sensitive data supports compliance and security requirements
5. **Audit Trail:** Comprehensive audit logging ensures data governance and processing transparency
6. **Relationship Preservation:** Logical relationships maintained through business keys rather than technical foreign keys