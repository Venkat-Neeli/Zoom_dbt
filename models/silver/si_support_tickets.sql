{{ config(
    materialized='incremental',
    unique_key='ticket_id',
    on_schema_change='sync_all_columns'
) }}

WITH bronze_support_tickets AS (
    SELECT *,
           ROW_NUMBER() OVER (PARTITION BY ticket_id ORDER BY update_timestamp DESC, load_timestamp DESC) AS row_num
    FROM {{ source('bronze', 'bz_support_tickets') }}
    WHERE ticket_id IS NOT NULL
),

deduped_support_tickets AS (
    SELECT *
    FROM bronze_support_tickets
    WHERE row_num = 1
),

data_quality_checks AS (
    SELECT *,
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
           
           CASE 
               WHEN ticket_id IS NULL OR user_id IS NULL OR 
                    ticket_type IS NULL OR TRIM(ticket_type) = '' OR
                    resolution_status IS NULL OR TRIM(resolution_status) = '' OR
                    open_date IS NULL THEN 'error'
               ELSE 'active'
           END AS record_status
    FROM deduped_support_tickets
),

valid_records AS (
    SELECT 
        ticket_id,
        user_id,
        CASE 
            WHEN UPPER(TRIM(ticket_type)) IN ('AUDIO ISSUE', 'VIDEO ISSUE', 'CONNECTIVITY', 'BILLING INQUIRY', 'FEATURE REQUEST', 'ACCOUNT ACCESS') 
            THEN INITCAP(TRIM(ticket_type))
            ELSE 'Other'
        END AS ticket_type,
        CASE 
            WHEN UPPER(TRIM(resolution_status)) IN ('OPEN', 'IN PROGRESS', 'PENDING CUSTOMER', 'CLOSED', 'RESOLVED') 
            THEN INITCAP(TRIM(resolution_status))
            ELSE 'Open'
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
    WHERE record_status = 'active'
      AND data_quality_score >= 0.7
)

SELECT * FROM valid_records

{% if is_incremental() %}
    WHERE update_timestamp > (SELECT COALESCE(MAX(update_timestamp), '1900-01-01'::timestamp) FROM {{ this }})
{% endif %}
