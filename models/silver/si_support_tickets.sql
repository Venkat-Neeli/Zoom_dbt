{{
    config(
        materialized='incremental',
        unique_key='ticket_id',
        on_schema_change='fail',
        pre_hook="{% if this.name != 'si_process_audit' %}INSERT INTO {{ ref('si_process_audit') }} (execution_id, pipeline_name, start_time, status, source_system, target_system, process_type, load_date, update_date) SELECT '{{ invocation_id }}', 'si_support_tickets_transform', current_timestamp(), 'STARTED', 'BRONZE', 'SILVER', 'ETL', current_date(), current_date(){% endif %}",
        post_hook="{% if this.name != 'si_process_audit' %}INSERT INTO {{ ref('si_process_audit') }} (execution_id, pipeline_name, end_time, status, records_processed, source_system, target_system, process_type, load_date, update_date) SELECT '{{ invocation_id }}', 'si_support_tickets_transform', current_timestamp(), 'COMPLETED', (SELECT count(*) FROM {{ this }}), 'BRONZE', 'SILVER', 'ETL', current_date(), current_date(){% endif %}"
    )
}}

-- Transform bronze support tickets to silver support tickets with data quality checks
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
    FROM {{ ref('bz_support_tickets') }}
    WHERE ticket_id IS NOT NULL
        AND user_id IS NOT NULL
        AND ticket_type IS NOT NULL
        AND resolution_status IS NOT NULL
        AND open_date IS NOT NULL
),

-- Data quality validation and cleansing
cleaned_support_tickets AS (
    SELECT
        ticket_id,
        user_id,
        CASE 
            WHEN ticket_type IN ('Audio Issue', 'Video Issue', 'Connectivity', 'Billing Inquiry', 'Feature Request', 'Account Access') THEN ticket_type
            ELSE 'Other'
        END AS ticket_type,
        CASE 
            WHEN resolution_status IN ('Open', 'In Progress', 'Pending Customer', 'Closed', 'Resolved') THEN resolution_status
            ELSE 'Open'
        END AS resolution_status,
        open_date,
        load_timestamp,
        update_timestamp,
        source_system,
        -- Calculate data quality score
        CASE 
            WHEN ticket_type IN ('Audio Issue', 'Video Issue', 'Connectivity', 'Billing Inquiry', 'Feature Request', 'Account Access')
                AND resolution_status IN ('Open', 'In Progress', 'Pending Customer', 'Closed', 'Resolved')
            THEN 1.00
            ELSE 0.50
        END AS data_quality_score,
        'active' AS record_status
    FROM bronze_support_tickets
    WHERE rn = 1  -- Deduplication: keep latest record
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
FROM cleaned_support_tickets

{% if is_incremental() %}
  WHERE update_timestamp > (SELECT COALESCE(MAX(update_timestamp), '1900-01-01') FROM {{ this }})
{% endif %}
