_____________________________________________
## *Author*: AAVA
## *Created on*:   
## *Description*: Conceptual data model for Zoom Platform Analytics System reporting requirements
## *Version*: 1 
## *Updated on*: 
_____________________________________________

# Conceptual Data Model - Zoom Platform Analytics System

## 1. Domain Overview

The Zoom Platform Analytics System operates within the video communications domain, focusing on three primary business areas:

1. **Platform Usage & Adoption**: Monitoring user engagement, meeting activities, and feature utilization
2. **Service Reliability & Support**: Tracking customer support interactions and platform stability
3. **Revenue & License Management**: Managing billing events, license assignments, and revenue analysis

The system captures comprehensive data about user interactions, meeting activities, support tickets, billing events, and license management to support strategic decision-making across product, marketing, support, and finance teams.

## 2. List of Entity Names with Descriptions

1. **Users**: Core entity representing all platform users including their profile information and plan details
2. **Meetings**: Central entity capturing all meeting sessions hosted on the platform
3. **Attendees**: Entity tracking participation in meetings by users
4. **Features_Usage**: Entity recording utilization of specific platform features during meetings
5. **Support_Tickets**: Entity managing customer support requests and issue resolution
6. **Billing_Events**: Entity tracking all financial transactions and billing activities
7. **Licenses**: Entity managing license assignments, types, and expiration tracking

## 3. List of Attributes for Each Entity

### Users Entity
- **User Name**: Full name of the platform user
- **Email**: Email address for user identification and communication
- **Plan Type**: Subscription plan category (Free, Paid, Enterprise)
- **Company**: Organization or company affiliation
- **Registration Date**: Date when user account was created
- **Last Login**: Most recent platform access timestamp

### Meetings Entity
- **Meeting Title**: Descriptive name or subject of the meeting
- **Duration Minutes**: Total length of meeting session in minutes
- **Start Time**: Meeting commencement timestamp
- **End Time**: Meeting conclusion timestamp
- **Meeting Type**: Category of meeting (scheduled, instant, recurring)
- **Participant Count**: Total number of attendees in the meeting

### Attendees Entity
- **Join Time**: Timestamp when participant joined the meeting
- **Leave Time**: Timestamp when participant left the meeting
- **Attendance Duration**: Total time participant was present in minutes
- **Connection Quality**: Network connection stability rating
- **Device Type**: Type of device used to join (desktop, mobile, web)

### Features_Usage Entity
- **Feature Name**: Name of the platform feature utilized
- **Usage Count**: Number of times feature was used during meeting
- **Usage Duration**: Total time feature was active in minutes
- **Usage Timestamp**: When feature was first activated

### Support_Tickets Entity
- **Ticket Type**: Category of support issue (audio, video, connectivity, billing)
- **Resolution Status**: Current state of ticket (open, in-progress, resolved, closed)
- **Open Date**: Date when ticket was created
- **Close Date**: Date when ticket was resolved
- **Priority Level**: Urgency classification (low, medium, high, critical)
- **Description**: Detailed explanation of the issue
- **Resolution Notes**: Summary of actions taken to resolve

### Billing_Events Entity
- **Event Type**: Type of billing transaction (subscription, upgrade, refund, payment)
- **Amount**: Monetary value of the transaction
- **Transaction Date**: Date when billing event occurred
- **Payment Method**: Method used for payment (credit card, bank transfer, etc.)
- **Currency**: Currency denomination of the transaction
- **Invoice Number**: Reference number for billing documentation

### Licenses Entity
- **License Type**: Category of license (basic, professional, enterprise)
- **Start Date**: License activation date
- **End Date**: License expiration date
- **License Status**: Current state (active, expired, suspended)
- **Seat Count**: Number of user seats included in license
- **Renewal Date**: Next scheduled renewal date

## 4. KPI List

### Platform Usage & Adoption KPIs
1. **Daily Active Users (DAU)**: Number of unique users who hosted at least one meeting per day
2. **Weekly Active Users (WAU)**: Number of unique users who hosted at least one meeting per week
3. **Monthly Active Users (MAU)**: Number of unique users who hosted at least one meeting per month
4. **Total Meeting Minutes**: Sum of duration across all meetings
5. **Average Meeting Duration**: Mean duration across all meetings
6. **Meetings Created Per User**: Average number of meetings hosted per user
7. **New User Sign-ups**: Count of new user registrations over time
8. **Feature Adoption Rate**: Percentage of users utilizing specific features

### Service Reliability & Support KPIs
1. **Tickets Opened Per Day/Week**: Volume of new support tickets created
2. **Average Ticket Resolution Time**: Mean time from ticket creation to closure
3. **First-Contact Resolution Rate**: Percentage of tickets resolved on first interaction
4. **Tickets Per 1,000 Active Users**: Support ticket density relative to user base
5. **Ticket Volume by Type**: Distribution of tickets across issue categories

### Revenue & License Analysis KPIs
1. **Monthly Recurring Revenue (MRR)**: Predictable monthly revenue from subscriptions
2. **Revenue by Plan Type**: Revenue distribution across subscription tiers
3. **License Utilization Rate**: Percentage of assigned licenses out of total available
4. **License Expiration Trends**: Pattern of license renewals and expirations
5. **Churn Rate**: Percentage of users who discontinue platform usage
6. **Usage-Billing Correlation**: Relationship between platform usage and upgrade events

## 5. Conceptual Data Model Diagram

| Source Entity | Target Entity | Relationship Key Field | Relationship Type |
|---------------|---------------|----------------------|-------------------|
| Users | Meetings | Host_ID | One-to-Many |
| Meetings | Attendees | Meeting_ID | One-to-Many |
| Meetings | Features_Usage | Meeting_ID | One-to-Many |
| Users | Support_Tickets | User_ID | One-to-Many |
| Users | Billing_Events | User_ID | One-to-Many |
| Users | Licenses | Assigned_To_User_ID | One-to-Many |

### Relationship Descriptions
- **Users to Meetings**: One user can host multiple meetings (Host_ID links to User_ID)
- **Meetings to Attendees**: One meeting can have multiple attendees (Meeting_ID)
- **Meetings to Features_Usage**: One meeting can have multiple feature usage records (Meeting_ID)
- **Users to Support_Tickets**: One user can create multiple support tickets (User_ID)
- **Users to Billing_Events**: One user can have multiple billing transactions (User_ID)
- **Users to Licenses**: One user can be assigned multiple licenses (Assigned_To_User_ID links to User_ID)

## 6. Common Data Elements in Report Requirements

The following data elements are referenced across multiple reports within the requirements:

### Cross-Report Data Elements
1. **User_ID**: Referenced in Platform Usage, Service Reliability, and Revenue Analysis reports
2. **Plan_Type**: Used in Platform Usage and Revenue Analysis reports
3. **Meeting_ID**: Referenced in Platform Usage and Features Usage analysis
4. **Duration_Minutes**: Used in Platform Usage calculations and meeting analysis
5. **Start_Time/End_Time**: Referenced in Platform Usage and meeting pattern analysis
6. **Company**: Used in Service Reliability and Revenue Analysis for organizational grouping
7. **Amount**: Referenced in Revenue Analysis and billing event tracking
8. **Feature_Name**: Used in Platform Usage for adoption analysis
9. **Ticket_Type**: Referenced in Service Reliability for issue categorization
10. **License_Type**: Used in Revenue Analysis and license utilization tracking

### Calculated Metrics Across Reports
1. **Total_Meeting_Minutes**: Aggregated across Platform Usage reports
2. **Active_Users_Count**: Calculated for Platform Usage and Revenue correlation
3. **Average_Resolution_Time**: Computed for Service Reliability analysis
4. **Feature_Adoption_Rate**: Calculated for Platform Usage insights
5. **License_Utilization_Rate**: Computed for Revenue and License Analysis
6. **Ticket_Volume_by_Type**: Aggregated for Service Reliability reporting

These common elements ensure data consistency and enable cross-functional analysis across different business domains within the Zoom Platform Analytics System.