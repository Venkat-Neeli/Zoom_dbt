{{ config(
    materialized='incremental',
    unique_key='ticket_id',
    on_schema_change='sync_all_columns',
    tags=['silver', 'support_tickets'],
    pre_hook="INSERT INTO {{ ref('si_process_audit') }} (execution_id, pipeline_name, start_time, status, source_system, target_system, process_type, load_date, update_date) SELECT '{{ invocation_id }}' || '_support_tickets_start', 'si_support_tickets_transform', CURRENT_TIMESTAMP(), 'STARTED', 'BRONZE', 'SILVER', 'ETL', CURRENT_DATE(), CURRENT_DATE() WHERE '{{ this.name }}' != 'si_process_audit'",
    post_hook="INSERT INTO {{ ref('si_process_audit') }} (execution_id, pipeline_name, end_time, status, records_processed, source_system, target_system, process_type, load_date, update_date) SELECT '{{ invocation_id }}' || '_support_tickets_end', 'si_support_tickets_transform', CURRENT_TIMESTAMP(), 'COMPLETED', (SELECT COUNT(*) FROM {{ this }}), 'BRONZE', 'SILVER', 'ETL', CURRENT_DATE(), CURRENT_DATE() WHERE '{{ this.name }}' != 'si_process_audit'"
) }}

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
                     load_timestamp DESC,
                     CASE WHEN resolution_status IS NOT NULL THEN 1 ELSE 0 END DESC
        ) AS row_rank
    FROM {{ source('bronze', 'bz_support_tickets') }}
    WHERE ticket_id IS NOT NULL
),

deduped_support_tickets AS (
    SELECT *
    FROM bronze_support_tickets
    WHERE row_rank = 1
),

data_quality_checks AS (
    SELECT 
        ticket_id,
        user_id,
        CASE 
            WHEN UPPER(TRIM(ticket_type)) IN ('AUDIO ISSUE', 'VIDEO ISSUE', 'CONNECTIVITY', 'BILLING INQUIRY', 'FEATURE REQUEST', 'ACCOUNT ACCESS') 
            THEN UPPER(TRIM(ticket_type))
            ELSE 'OTHER'
        END AS ticket_type_clean,
        CASE 
            WHEN UPPER(TRIM(resolution_status)) IN ('OPEN', 'IN PROGRESS', 'PENDING CUSTOMER', 'CLOSED', 'RESOLVED') 
            THEN UPPER(TRIM(resolution_status))
            ELSE 'UNKNOWN'
        END AS resolution_status_clean,
        open_date,
        load_timestamp,
        update_timestamp,
        source_system,
        -- Data Quality Score Calculation
        CASE 
            WHEN ticket_id IS NOT NULL 
                AND user_id IS NOT NULL
                AND ticket_type IS NOT NULL
                AND resolution_status IS NOT NULL
                AND open_date IS NOT NULL
            THEN 1.00
            WHEN ticket_id IS NOT NULL 
                AND user_id IS NOT NULL
                AND ticket_type IS NOT NULL
            THEN 0.75
            WHEN ticket_id IS NOT NULL 
                AND user_id IS NOT NULL
            THEN 0.50
            ELSE 0.25
        END AS data_quality_score,
        -- Record Status
        CASE 
            WHEN ticket_id IS NOT NULL 
                AND user_id IS NOT NULL
                AND ticket_type IS NOT NULL
                AND resolution_status IS NOT NULL
                AND open_date IS NOT NULL
            THEN 'ACTIVE'
            ELSE 'ERROR'
        END AS record_status
    FROM deduped_support_tickets
),

final_transform AS (
    SELECT 
        ticket_id,
        user_id,
        ticket_type_clean AS ticket_type,
        resolution_status_clean AS resolution_status,
        open_date,
        load_timestamp,
        update_timestamp,
        source_system,
        DATE(load_timestamp) AS load_date,
        DATE(update_timestamp) AS update_date,
        data_quality_score,
        record_status
    FROM data_quality_checks
    WHERE record_status = 'ACTIVE'
)

SELECT * FROM final_transform

{% if is_incremental() %}
    WHERE update_timestamp > (SELECT COALESCE(MAX(update_timestamp), '1900-01-01') FROM {{ this }})
{% endif %}
