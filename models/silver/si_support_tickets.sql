{{
    config(
        materialized='incremental',
        unique_key='ticket_id',
        on_schema_change='sync_all_columns',
        pre_hook="{% if not (this.name == 'si_process_audit') %}INSERT INTO {{ ref('si_process_audit') }} (execution_id, pipeline_name, start_time, status, source_system, target_system, process_type, load_date, update_date) VALUES ('{{ invocation_id }}_{{ this.name }}', '{{ this.name }}', CURRENT_TIMESTAMP(), 'STARTED', 'BRONZE', 'SILVER', 'ETL', CURRENT_DATE(), CURRENT_DATE()){% endif %}",
        post_hook="{% if not (this.name == 'si_process_audit') %}UPDATE {{ ref('si_process_audit') }} SET end_time = CURRENT_TIMESTAMP(), status = 'SUCCESS', records_processed = (SELECT COUNT(*) FROM {{ this }}), records_successful = (SELECT COUNT(*) FROM {{ this }} WHERE record_status = 'active'), records_failed = (SELECT COUNT(*) FROM {{ this }} WHERE record_status = 'error'), processing_duration_seconds = DATEDIFF(second, start_time, CURRENT_TIMESTAMP()) WHERE execution_id = '{{ invocation_id }}_{{ this.name }}'{% endif %}"
    )
}}

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
        ) as row_num
    FROM {{ ref('bz_support_tickets') }}
    WHERE ticket_id IS NOT NULL
),

deduped_support_tickets AS (
    SELECT *
    FROM bronze_support_tickets
    WHERE row_num = 1
),

data_quality_checks AS (
    SELECT 
        *,
        -- Null checks
        CASE WHEN ticket_id IS NOT NULL AND user_id IS NOT NULL AND ticket_type IS NOT NULL AND resolution_status IS NOT NULL AND open_date IS NOT NULL THEN 1 ELSE 0 END as null_check,
        -- Domain checks
        CASE WHEN ticket_type IN ('Audio Issue', 'Video Issue', 'Connectivity', 'Billing Inquiry', 'Feature Request', 'Account Access') THEN 1 ELSE 0 END as ticket_domain_check,
        CASE WHEN resolution_status IN ('Open', 'In Progress', 'Pending Customer', 'Closed', 'Resolved') THEN 1 ELSE 0 END as status_domain_check,
        -- Referential integrity
        CASE WHEN EXISTS (SELECT 1 FROM {{ ref('si_users') }} u WHERE u.user_id = deduped_support_tickets.user_id) THEN 1 ELSE 0 END as ref_check
    FROM deduped_support_tickets
),

transformed_support_tickets AS (
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
            ELSE ticket_type
        END as ticket_type,
        CASE 
            WHEN UPPER(TRIM(resolution_status)) = 'OPEN' THEN 'Open'
            WHEN UPPER(TRIM(resolution_status)) = 'IN PROGRESS' THEN 'In Progress'
            WHEN UPPER(TRIM(resolution_status)) = 'PENDING CUSTOMER' THEN 'Pending Customer'
            WHEN UPPER(TRIM(resolution_status)) = 'CLOSED' THEN 'Closed'
            WHEN UPPER(TRIM(resolution_status)) = 'RESOLVED' THEN 'Resolved'
            ELSE resolution_status
        END as resolution_status,
        open_date,
        load_timestamp,
        update_timestamp,
        source_system,
        DATE(load_timestamp) as load_date,
        DATE(update_timestamp) as update_date,
        {{ calculate_data_quality_score('null_check = 1', 'ticket_domain_check = 1 AND status_domain_check = 1', 'ref_check = 1') }} as data_quality_score,
        CASE 
            WHEN null_check = 1 AND ticket_domain_check = 1 AND status_domain_check = 1 AND ref_check = 1 THEN 'active'
            ELSE 'error'
        END as record_status
    FROM data_quality_checks
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
FROM transformed_support_tickets
WHERE record_status = 'active'

{% if is_incremental() %}
    AND update_timestamp > (SELECT MAX(update_timestamp) FROM {{ this }})
{% endif %}
