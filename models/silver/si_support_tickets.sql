{{
    config(
        materialized='incremental',
        unique_key='ticket_id',
        on_schema_change='sync_all_columns',
        pre_hook="INSERT INTO {{ ref('si_process_audit') }} (execution_id, pipeline_name, start_time, status, source_system, target_system, process_type, load_date, update_date) SELECT '{{ dbt_utils.generate_surrogate_key(['current_timestamp()']) }}', 'si_support_tickets', CURRENT_TIMESTAMP(), 'STARTED', 'Bronze', 'Silver', 'ETL', CURRENT_DATE(), CURRENT_DATE() WHERE '{{ this.name }}' != 'si_process_audit'",
        post_hook="INSERT INTO {{ ref('si_process_audit') }} (execution_id, pipeline_name, end_time, status, records_processed, source_system, target_system, process_type, load_date, update_date) SELECT '{{ dbt_utils.generate_surrogate_key(['current_timestamp()']) }}', 'si_support_tickets', CURRENT_TIMESTAMP(), 'COMPLETED', (SELECT COUNT(*) FROM {{ this }}), 'Bronze', 'Silver', 'ETL', CURRENT_DATE(), CURRENT_DATE() WHERE '{{ this.name }}' != 'si_process_audit'"
    )
}}

-- Silver Support Tickets transformation with data quality checks
WITH source_data AS (
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
    FROM {{ source('bronze', 'bz_support_tickets') }}
    WHERE ticket_id IS NOT NULL
      AND user_id IS NOT NULL
      AND ticket_type IS NOT NULL
      AND resolution_status IS NOT NULL
),

deduped_data AS (
    SELECT 
        ticket_id,
        user_id,
        CASE 
            WHEN ticket_type IN ('Audio Issue', 'Video Issue', 'Connectivity', 'Billing Inquiry', 'Feature Request', 'Account Access') 
            THEN ticket_type
            ELSE 'Other'
        END AS ticket_type,
        CASE 
            WHEN resolution_status IN ('Open', 'In Progress', 'Pending Customer', 'Closed', 'Resolved') 
            THEN resolution_status
            ELSE 'Open'
        END AS resolution_status,
        open_date,
        load_timestamp,
        update_timestamp,
        source_system,
        DATE(load_timestamp) AS load_date,
        DATE(update_timestamp) AS update_date,
        -- Data quality score calculation
        CASE 
            WHEN ticket_id IS NOT NULL 
                 AND user_id IS NOT NULL 
                 AND ticket_type IS NOT NULL 
                 AND resolution_status IS NOT NULL
            THEN 1.00
            ELSE 0.75
        END AS data_quality_score,
        'active' AS record_status
    FROM source_data
    WHERE row_num = 1
)

SELECT * FROM deduped_data

{% if is_incremental() %}
    WHERE update_timestamp > (SELECT MAX(update_timestamp) FROM {{ this }})
{% endif %}
