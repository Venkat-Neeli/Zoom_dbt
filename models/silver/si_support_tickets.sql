{{ config(
    materialized='incremental',
    unique_key='ticket_id',
    on_schema_change='fail'
) }}

-- Silver Support Tickets Table Transformation
WITH bronze_support_tickets AS (
    SELECT 
        ticket_id,
        user_id,
        ticket_type,
        resolution_status,
        open_date,
        load_timestamp,
        update_timestamp,
        source_system,
        ROW_NUMBER() OVER (
            PARTITION BY ticket_id 
            ORDER BY update_timestamp DESC, 
                     load_timestamp DESC
        ) AS row_num
    FROM {{ source('bronze', 'bz_support_tickets') }}
    WHERE ticket_id IS NOT NULL
),

data_quality_checks AS (
    SELECT 
        *,
        -- Calculate data quality score
        CASE 
            WHEN ticket_id IS NULL THEN 0.0
            WHEN user_id IS NULL THEN 0.2
            WHEN ticket_type IS NULL OR TRIM(ticket_type) = '' THEN 0.3
            WHEN ticket_type NOT IN ('Audio Issue', 'Video Issue', 'Connectivity', 'Billing Inquiry', 'Feature Request', 'Account Access') THEN 0.4
            WHEN resolution_status IS NULL OR TRIM(resolution_status) = '' THEN 0.5
            WHEN resolution_status NOT IN ('Open', 'In Progress', 'Pending Customer', 'Closed', 'Resolved') THEN 0.6
            WHEN open_date IS NULL THEN 0.7
            ELSE 1.0
        END AS data_quality_score,
        
        -- Set record status
        CASE 
            WHEN ticket_id IS NULL OR user_id IS NULL 
                 OR ticket_type IS NULL OR TRIM(ticket_type) = ''
                 OR resolution_status IS NULL OR TRIM(resolution_status) = ''
                 OR open_date IS NULL THEN 'error'
            ELSE 'active'
        END AS record_status
    FROM bronze_support_tickets
    WHERE row_num = 1
),

final_transform AS (
    SELECT 
        ticket_id,
        user_id,
        CASE 
            WHEN UPPER(TRIM(ticket_type)) = 'AUDIO ISSUE' THEN 'Audio Issue'
            WHEN UPPER(TRIM(ticket_type)) = 'VIDEO ISSUE' THEN 'Video Issue'
            WHEN UPPER(TRIM(ticket_type)) = 'CONNECTIVITY' THEN 'Connectivity'
            WHEN UPPER(TRIM(ticket_type)) = 'BILLING INQUIRY' THEN 'Billing Inquiry'
            WHEN UPPER(TRIM(ticket_type)) = 'FEATURE REQUEST' THEN 'Feature Request'
            WHEN UPPER(TRIM(ticket_type)) = 'ACCOUNT ACCESS' THEN 'Account Access'
            ELSE TRIM(ticket_type)
        END AS ticket_type,
        CASE 
            WHEN UPPER(TRIM(resolution_status)) = 'OPEN' THEN 'Open'
            WHEN UPPER(TRIM(resolution_status)) = 'IN PROGRESS' THEN 'In Progress'
            WHEN UPPER(TRIM(resolution_status)) = 'PENDING CUSTOMER' THEN 'Pending Customer'
            WHEN UPPER(TRIM(resolution_status)) = 'CLOSED' THEN 'Closed'
            WHEN UPPER(TRIM(resolution_status)) = 'RESOLVED' THEN 'Resolved'
            ELSE TRIM(resolution_status)
        END AS resolution_status,
        open_date,
        load_timestamp,
        update_timestamp,
        source_system,
        DATE(load_timestamp) AS load_date,
        DATE(update_timestamp) AS update_date,
        data_quality_score,
        record_status
    FROM data_quality_checks
    WHERE record_status = 'active'  -- Only pass clean records to Silver
)

SELECT 
    ticket_id,
    user_id,
    ticket_type,
    resolution_status,
    open_date,
    load_timestamp,
    update_timestamp,
    source_system,
    load_date,
    update_date,
    data_quality_score,
    record_status
FROM final_transform

{% if is_incremental() %}
    WHERE update_timestamp > (SELECT COALESCE(MAX(update_timestamp), '1900-01-01') FROM {{ this }})
{% endif %}
