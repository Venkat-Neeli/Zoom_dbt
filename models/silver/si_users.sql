{{
    config(
        materialized='incremental',
        unique_key='user_id',
        on_schema_change='sync_all_columns',
        pre_hook="INSERT INTO {{ ref('si_process_audit') }} (execution_id, pipeline_name, start_time, status, source_system, target_system, process_type, load_date, update_date) SELECT '{{ dbt_utils.generate_surrogate_key(['current_timestamp()']) }}', 'si_users', CURRENT_TIMESTAMP(), 'STARTED', 'Bronze', 'Silver', 'ETL', CURRENT_DATE(), CURRENT_DATE() WHERE '{{ this.name }}' != 'si_process_audit'",
        post_hook="INSERT INTO {{ ref('si_process_audit') }} (execution_id, pipeline_name, end_time, status, records_processed, source_system, target_system, process_type, load_date, update_date) SELECT '{{ dbt_utils.generate_surrogate_key(['current_timestamp()']) }}', 'si_users', CURRENT_TIMESTAMP(), 'COMPLETED', (SELECT COUNT(*) FROM {{ this }}), 'Bronze', 'Silver', 'ETL', CURRENT_DATE(), CURRENT_DATE() WHERE '{{ this.name }}' != 'si_process_audit'"
    )
}}

-- Silver Users transformation with data quality checks
WITH source_data AS (
    SELECT 
        user_id,
        user_name,
        email,
        company,
        plan_type,
        load_timestamp,
        update_timestamp,
        source_system,
        ROW_NUMBER() OVER (
            PARTITION BY user_id 
            ORDER BY update_timestamp DESC, 
                     load_timestamp DESC
        ) as row_num
    FROM {{ source('bronze', 'bz_users') }}
    WHERE user_id IS NOT NULL
      AND user_name IS NOT NULL
      AND email IS NOT NULL
      AND REGEXP_LIKE(email, '^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\\.[A-Za-z]{2,}$')
),

deduped_data AS (
    SELECT 
        user_id,
        TRIM(user_name) AS user_name,
        LOWER(TRIM(email)) AS email,
        CASE 
            WHEN TRIM(company) = '' THEN '000'
            ELSE TRIM(company)
        END AS company,
        CASE 
            WHEN plan_type IN ('Free', 'Pro', 'Business', 'Enterprise') THEN plan_type
            ELSE 'Free'
        END AS plan_type,
        load_timestamp,
        update_timestamp,
        source_system,
        DATE(load_timestamp) AS load_date,
        DATE(update_timestamp) AS update_date,
        -- Data quality score calculation
        CASE 
            WHEN user_id IS NOT NULL 
                 AND user_name IS NOT NULL 
                 AND email IS NOT NULL 
                 AND REGEXP_LIKE(email, '^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\\.[A-Za-z]{2,}$')
                 AND plan_type IN ('Free', 'Pro', 'Business', 'Enterprise')
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
