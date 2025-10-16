{{ config(
    materialized='incremental',
    unique_key='ticket_id',
    pre_hook="INSERT INTO {{ ref('si_process_audit') }} (execution_id, pipeline_name, start_time, status, source_system, target_system, process_type, load_date, update_date) SELECT '{{ dbt_utils.generate_surrogate_key([invocation_id, 'support_tickets']) }}', 'si_support_tickets_transform', CURRENT_TIMESTAMP(), 'RUNNING', 'BRONZE', 'SILVER', 'TRANSFORMATION', CURRENT_DATE(), CURRENT_DATE() WHERE '{{ this.name }}' != 'si_process_audit'",
    post_hook="UPDATE {{ ref('si_process_audit') }} SET end_time = CURRENT_TIMESTAMP(), status = 'SUCCESS', records_processed = (SELECT COUNT(*) FROM {{ this }}), records_successful = (SELECT COUNT(*) FROM {{ this }}), processing_duration_seconds = DATEDIFF('second', start_time, CURRENT_TIMESTAMP()) WHERE execution_id = '{{ dbt_utils.generate_surrogate_key([invocation_id, 'support_tickets']) }}' AND '{{ this.name }}' != 'si_process_audit'"
) }}

-- Data Quality and Transformation Logic for Support Tickets
WITH bronze_support_tickets AS (
    SELECT *
    FROM {{ source('bronze', 'bz_support_tickets') }}
    {% if is_incremental() %}
        WHERE update_timestamp > (SELECT MAX(update_timestamp) FROM {{ this }})
    {% endif %}
),

-- Data Quality Checks and Cleansing
cleansed_support_tickets AS (
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
        -- Derived columns
        DATE(load_timestamp) AS load_date,
        DATE(update_timestamp) AS update_date,
        -- Data Quality Score Calculation
        CASE 
            WHEN ticket_id IS NOT NULL 
                AND user_id IS NOT NULL 
                AND ticket_type IS NOT NULL
                AND resolution_status IS NOT NULL
                AND open_date IS NOT NULL
            THEN 1.00
            ELSE 0.50
        END AS data_quality_score,
        -- Record Status
        CASE 
            WHEN ticket_id IS NULL OR user_id IS NULL OR open_date IS NULL THEN 'error'
            ELSE 'active'
        END AS record_status,
        -- Deduplication ranking
        ROW_NUMBER() OVER (
            PARTITION BY ticket_id 
            ORDER BY update_timestamp DESC, 
                     (CASE WHEN user_id IS NOT NULL THEN 1 ELSE 0 END +
                      CASE WHEN ticket_type IS NOT NULL THEN 1 ELSE 0 END +
                      CASE WHEN resolution_status IS NOT NULL THEN 1 ELSE 0 END) DESC,
                     ticket_id DESC
        ) AS row_rank
    FROM bronze_support_tickets
    WHERE ticket_id IS NOT NULL
),

-- Final deduplicated and validated data
final_support_tickets AS (
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
    FROM cleansed_support_tickets
    WHERE row_rank = 1
        AND record_status = 'active'
)

SELECT * FROM final_support_tickets
