{{ config(
    materialized='incremental',
    unique_key='ticket_id',
    on_schema_change='fail',
    tags=['silver', 'support_tickets'],
    pre_hook="INSERT INTO {{ ref('si_process_audit') }} (execution_id, pipeline_name, start_time, status, source_system, target_system, process_type, load_date, update_date) SELECT '{{ dbt_utils.generate_surrogate_key(['CURRENT_TIMESTAMP()', 'RANDOM()']) }}', 'si_support_tickets_transformation', CURRENT_TIMESTAMP(), 'STARTED', 'BRONZE', 'SILVER', 'ETL', CURRENT_DATE(), CURRENT_DATE() WHERE '{{ this.name }}' != 'si_process_audit'",
    post_hook="INSERT INTO {{ ref('si_process_audit') }} (execution_id, pipeline_name, end_time, status, records_processed, source_system, target_system, process_type, load_date, update_date) SELECT '{{ dbt_utils.generate_surrogate_key(['CURRENT_TIMESTAMP()', 'RANDOM()']) }}', 'si_support_tickets_transformation', CURRENT_TIMESTAMP(), 'COMPLETED', (SELECT COUNT(*) FROM {{ this }}), 'BRONZE', 'SILVER', 'ETL', CURRENT_DATE(), CURRENT_DATE() WHERE '{{ this.name }}' != 'si_process_audit'"
) }}

WITH bronze_support_tickets AS (
    SELECT 
        bst.ticket_id,
        bst.user_id,
        bst.ticket_type,
        bst.resolution_status,
        bst.open_date,
        bst.load_timestamp,
        bst.update_timestamp,
        bst.source_system,
        ROW_NUMBER() OVER (
            PARTITION BY bst.ticket_id 
            ORDER BY bst.update_timestamp DESC, 
                     bst.load_timestamp DESC
        ) AS row_num
    FROM {{ source('bronze', 'bz_support_tickets') }} bst
    WHERE bst.ticket_id IS NOT NULL
      AND bst.user_id IS NOT NULL
      AND bst.ticket_type IS NOT NULL
      AND bst.resolution_status IS NOT NULL
      AND bst.ticket_type IN ('Audio Issue', 'Video Issue', 'Connectivity', 'Billing Inquiry', 'Feature Request', 'Account Access')
      AND bst.resolution_status IN ('Open', 'In Progress', 'Pending Customer', 'Closed', 'Resolved')
),

cleaned_support_tickets AS (
    SELECT 
        ticket_id,
        user_id,
        CASE 
            WHEN ticket_type = 'Audio Problems' THEN 'Audio Issue'
            WHEN ticket_type = 'Video Problems' THEN 'Video Issue'
            WHEN ticket_type = 'Connection Issues' THEN 'Connectivity'
            ELSE ticket_type
        END AS ticket_type,
        CASE 
            WHEN resolution_status = 'Pending' THEN 'Pending Customer'
            WHEN resolution_status = 'Complete' THEN 'Resolved'
            ELSE resolution_status
        END AS resolution_status,
        open_date,
        load_timestamp,
        update_timestamp,
        source_system,
        DATE(load_timestamp) AS load_date,
        DATE(update_timestamp) AS update_date,
        -- Data Quality Score
        CASE 
            WHEN ticket_id IS NOT NULL 
                 AND user_id IS NOT NULL 
                 AND ticket_type IS NOT NULL 
                 AND resolution_status IS NOT NULL
            THEN 1.0
            ELSE 0.8
        END AS data_quality_score,
        'active' AS record_status
    FROM bronze_support_tickets
    WHERE row_num = 1
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
FROM cleaned_support_tickets

{% if is_incremental() %}
  WHERE update_timestamp > (SELECT MAX(update_timestamp) FROM {{ this }})
{% endif %}
