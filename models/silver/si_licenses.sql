{{ config(
    materialized='incremental',
    unique_key='license_id',
    on_schema_change='sync_all_columns',
    tags=['silver', 'licenses'],
    pre_hook="INSERT INTO {{ ref('si_process_audit') }} (execution_id, pipeline_name, start_time, status, source_system, target_system, process_type, load_date, update_date) SELECT '{{ invocation_id }}' || '_licenses_start', 'si_licenses_transform', CURRENT_TIMESTAMP(), 'STARTED', 'BRONZE', 'SILVER', 'ETL', CURRENT_DATE(), CURRENT_DATE() WHERE '{{ this.name }}' != 'si_process_audit'",
    post_hook="INSERT INTO {{ ref('si_process_audit') }} (execution_id, pipeline_name, end_time, status, records_processed, source_system, target_system, process_type, load_date, update_date) SELECT '{{ invocation_id }}' || '_licenses_end', 'si_licenses_transform', CURRENT_TIMESTAMP(), 'COMPLETED', (SELECT COUNT(*) FROM {{ this }}), 'BRONZE', 'SILVER', 'ETL', CURRENT_DATE(), CURRENT_DATE() WHERE '{{ this.name }}' != 'si_process_audit'"
) }}

WITH bronze_licenses AS (
    SELECT 
        license_id,
        license_type,
        assigned_to_user_id,
        start_date,
        end_date,
        load_timestamp,
        update_timestamp,
        source_system,
        ROW_NUMBER() OVER (
            PARTITION BY license_id 
            ORDER BY update_timestamp DESC, 
                     load_timestamp DESC,
                     CASE WHEN assigned_to_user_id IS NOT NULL THEN 1 ELSE 0 END DESC
        ) AS row_rank
    FROM {{ source('bronze', 'bz_licenses') }}
    WHERE license_id IS NOT NULL
),

deduped_licenses AS (
    SELECT *
    FROM bronze_licenses
    WHERE row_rank = 1
),

data_quality_checks AS (
    SELECT 
        license_id,
        CASE 
            WHEN UPPER(TRIM(license_type)) IN ('PRO', 'BUSINESS', 'ENTERPRISE', 'EDUCATION') 
            THEN UPPER(TRIM(license_type))
            ELSE 'UNKNOWN'
        END AS license_type_clean,
        assigned_to_user_id,
        start_date,
        end_date,
        load_timestamp,
        update_timestamp,
        source_system,
        -- Data Quality Score Calculation
        CASE 
            WHEN license_id IS NOT NULL 
                AND license_type IS NOT NULL
                AND start_date IS NOT NULL
                AND end_date IS NOT NULL
                AND end_date > start_date
            THEN 1.00
            WHEN license_id IS NOT NULL 
                AND license_type IS NOT NULL
                AND start_date IS NOT NULL
            THEN 0.75
            WHEN license_id IS NOT NULL 
                AND license_type IS NOT NULL
            THEN 0.50
            ELSE 0.25
        END AS data_quality_score,
        -- Record Status
        CASE 
            WHEN license_id IS NOT NULL 
                AND license_type IS NOT NULL
                AND start_date IS NOT NULL
                AND end_date IS NOT NULL
                AND end_date > start_date
            THEN 'ACTIVE'
            ELSE 'ERROR'
        END AS record_status
    FROM deduped_licenses
),

final_transform AS (
    SELECT 
        license_id,
        license_type_clean AS license_type,
        assigned_to_user_id,
        start_date,
        end_date,
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
