_____________________________________________
## *Author*: AAVA
## *Created on*: 
## *Description*: Conceptual data model for Zoom Platform Analytics System reporting requirements
## *Version*: 1
## *Updated on*: 
_____________________________________________

# Conceptual Data Model - Zoom Platform Analytics System

## 1. Domain Overview

The Zoom Platform Analytics System encompasses three primary business domains:

1. **Platform Usage & Adoption**: Focuses on monitoring user engagement, platform adoption rates, and feature utilization to identify growth trends and improvement opportunities.

2. **Service Reliability & Support**: Analyzes platform stability and customer support interactions to improve service quality and reduce support ticket volume.

3. **Revenue and License Analysis**: Monitors billing events and license utilization to understand revenue streams, customer value, and identify upselling opportunities.

## 2. List of Entity Names with Descriptions

1. **Users**: Represents individuals who use the Zoom platform, including both free and paid subscribers
2. **Meetings**: Represents video conference sessions hosted on the Zoom platform
3. **Attendees**: Represents participants who join meetings hosted by users
4. **Features Usage**: Tracks the utilization of various Zoom features during meetings
5. **Support Tickets**: Represents customer service requests and issues reported by users
6. **Billing Events**: Captures financial transactions and billing activities related to user accounts
7. **Licenses**: Represents software licenses assigned to users for accessing premium features

## 3. List of Attributes for Each Entity

### Users Entity
- **User Name**: The display name of the user account
- **Email Address**: The email address associated with the user account
- **Plan Type**: The subscription tier (Free, Basic, Pro, Business, Enterprise)
- **Company**: The organization or company the user is affiliated with
- **Registration Date**: The date when the user created their account
- **Account Status**: Current status of the user account (Active, Inactive, Suspended)

### Meetings Entity
- **Meeting Title**: The name or title given to the meeting
- **Duration Minutes**: The total length of the meeting in minutes
- **Start Time**: The date and time when the meeting began
- **End Time**: The date and time when the meeting ended
- **Meeting Type**: The category of meeting (Scheduled, Instant, Recurring)
- **Meeting Status**: Current status of the meeting (Completed, In Progress, Cancelled)

### Attendees Entity
- **Attendee Name**: The name of the meeting participant
- **Join Time**: The time when the attendee joined the meeting
- **Leave Time**: The time when the attendee left the meeting
- **Attendance Duration**: Total time the attendee spent in the meeting
- **Participant Role**: The role of the attendee (Host, Co-host, Participant)

### Features Usage Entity
- **Feature Name**: The name of the Zoom feature being used
- **Usage Count**: The number of times the feature was utilized
- **Usage Duration**: The total time the feature was active during the meeting
- **Feature Category**: The classification of the feature (Audio, Video, Collaboration, Security)

### Support Tickets Entity
- **Ticket Type**: The category of the support issue (Audio Issues, Connectivity Problems, Billing, Account)
- **Resolution Status**: Current status of the ticket (Open, In Progress, Resolved, Closed)
- **Open Date**: The date when the ticket was created
- **Close Date**: The date when the ticket was resolved
- **Priority Level**: The urgency level of the ticket (Low, Medium, High, Critical)
- **Issue Description**: Detailed description of the reported problem

### Billing Events Entity
- **Event Type**: The type of billing transaction (Subscription, Upgrade, Downgrade, Refund)
- **Amount**: The monetary value of the billing event
- **Transaction Date**: The date when the billing event occurred
- **Payment Method**: The method used for payment (Credit Card, PayPal, Bank Transfer)
- **Currency**: The currency used for the transaction
- **Invoice Number**: The unique identifier for the billing invoice

### Licenses Entity
- **License Type**: The category of license (Basic, Pro, Business, Enterprise)
- **Start Date**: The date when the license becomes active
- **End Date**: The date when the license expires
- **License Status**: Current status of the license (Active, Expired, Suspended)
- **Seat Count**: The number of user seats included in the license
- **License Cost**: The cost associated with the license

## 4. KPI List

### Platform Usage & Adoption KPIs
1. **Daily Active Users (DAU)**: Number of unique users who hosted at least one meeting per day
2. **Weekly Active Users (WAU)**: Number of unique users who hosted at least one meeting per week
3. **Monthly Active Users (MAU)**: Number of unique users who hosted at least one meeting per month
4. **Total Meeting Minutes**: Sum of duration of all meetings per day/week/month
5. **Average Meeting Duration**: Average duration across all meetings
6. **Meetings Created Per User**: Number of meetings created per individual user
7. **New User Sign-ups**: Number of new user registrations over time
8. **Feature Adoption Rate**: Proportion of users who have used a specific feature

### Service Reliability & Support KPIs
1. **Daily/Weekly Ticket Volume**: Number of support tickets opened per day/week
2. **Average Ticket Resolution Time**: Average time taken to resolve support tickets
3. **First-Contact Resolution Rate**: Percentage of tickets resolved on first contact
4. **Tickets Per 1,000 Active Users**: Ratio of tickets to active user base
5. **Most Common Ticket Types**: Distribution of ticket categories

### Revenue and License Analysis KPIs
1. **Monthly Recurring Revenue (MRR)**: Predictable monthly revenue from subscriptions
2. **Revenue by Plan Type**: Revenue distribution across different subscription tiers
3. **License Utilization Rate**: Proportion of assigned licenses out of total available
4. **License Expiration Trends**: Pattern of license renewals and expirations
5. **Churn Rate**: Fraction of users who stopped using the platform
6. **Usage-Billing Correlation**: Relationship between usage patterns and billing events

## 5. Conceptual Data Model Diagram in Tabular Form

| Source Entity | Target Entity | Relationship Key Field | Relationship Type | Description |
|---------------|---------------|----------------------|-------------------|-------------|
| Users | Meetings | User (Host) | One-to-Many | One user can host multiple meetings |
| Meetings | Attendees | Meeting | One-to-Many | One meeting can have multiple attendees |
| Meetings | Features Usage | Meeting | One-to-Many | One meeting can have multiple feature usage records |
| Users | Support Tickets | User | One-to-Many | One user can create multiple support tickets |
| Users | Billing Events | User | One-to-Many | One user can have multiple billing events |
| Users | Licenses | User (Assigned To) | One-to-Many | One user can be assigned multiple licenses |

## 6. Common Data Elements in Report Requirements

The following data elements are referenced across multiple reports within the requirements:

### Cross-Report Data Elements
1. **User Information**
   - User identification
   - Plan Type
   - Company affiliation
   - Used in: Platform Usage & Adoption, Service Reliability & Support, Revenue and License Analysis

2. **Meeting Data**
   - Meeting identification
   - Duration Minutes
   - Start Time
   - Used in: Platform Usage & Adoption, Revenue and License Analysis

3. **Temporal Data**
   - Date/Time fields for trend analysis
   - Used across all three report categories for time-based analytics

4. **User Activity Metrics**
   - Active user counts
   - Usage patterns
   - Used in: Platform Usage & Adoption, Service Reliability & Support

5. **Financial Data**
   - Revenue amounts
   - Billing events
   - Used in: Revenue and License Analysis, correlated with Platform Usage

6. **Performance Indicators**
   - Resolution times
   - Utilization rates
   - Adoption metrics
   - Used across all report categories for KPI calculations

These common elements ensure data consistency and enable cross-functional analysis across different business domains within the Zoom Platform Analytics System.