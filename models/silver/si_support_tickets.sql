{{ config(
    materialized='incremental',
    unique_key='ticket_id',
    on_schema_change='fail'
) }}

-- Transform bronze support tickets to silver with data quality checks
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
        ROW_NUMBER() OVER (PARTITION BY ticket_id ORDER BY update_timestamp DESC, load_timestamp DESC) as rn
    FROM {{ source('bronze', 'bz_support_tickets') }}
    WHERE ticket_id IS NOT NULL
),

-- Data quality validation and transformation
validated_support_tickets AS (
    SELECT 
        ticket_id,
        user_id,
        CASE 
            WHEN ticket_type IN ('Audio Issue', 'Video Issue', 'Connectivity', 'Billing Inquiry', 'Feature Request', 'Account Access') THEN ticket_type
            ELSE 'Other' -- Standardize unknown ticket types
        END AS ticket_type,
        CASE 
            WHEN resolution_status IN ('Open', 'In Progress', 'Pending Customer', 'Closed', 'Resolved') THEN resolution_status
            ELSE 'Open' -- Default standardization
        END AS resolution_status,
        open_date,
        load_timestamp,
        update_timestamp,
        source_system,
        -- Data quality score calculation
        CASE 
            WHEN user_id IS NOT NULL 
                AND ticket_type IN ('Audio Issue', 'Video Issue', 'Connectivity', 'Billing Inquiry', 'Feature Request', 'Account Access')
                AND resolution_status IN ('Open', 'In Progress', 'Pending Customer', 'Closed', 'Resolved')
                AND open_date IS NOT NULL
            THEN 1.00
            ELSE 0.75
        END AS data_quality_score,
        -- Record status
        CASE 
            WHEN user_id IS NULL OR open_date IS NULL THEN 'error'
            ELSE 'active'
        END AS record_status
    FROM bronze_support_tickets
    WHERE rn = 1  -- Deduplication: keep latest record
        AND user_id IS NOT NULL
        AND open_date IS NOT NULL
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
    DATE(load_timestamp) AS load_date,
    DATE(update_timestamp) AS update_date,
    data_quality_score,
    record_status
FROM validated_support_tickets

{% if is_incremental() %}
    WHERE update_timestamp > (SELECT MAX(update_timestamp) FROM {{ this }})
{% endif %}
