{{
  config(
    materialized='incremental',
    unique_key='participant_fact_key',
    incremental_strategy='merge',
    on_schema_change='sync_all_columns',
    tags=['gold', 'fact', 'participant'],
    pre_hook="{{ log('Starting participant facts transformation', info=True) }}"
  )
}}

-- Participant Facts Transformation from Silver to Gold Layer
WITH participant_base AS (
  SELECT 
    participant_id,
    meeting_id,
    user_id,
    user_name,
    user_email,
    join_time,
    leave_time,
    duration,
    attentiveness_score,
    camera_on_duration,
    microphone_on_duration,
    screen_share_duration,
    participant_type,
    device_type,
    ip_address,
    location,
    created_at,
    updated_at,
    -- Generate surrogate key
    {{ dbt_utils.generate_surrogate_key(['participant_id', 'meeting_id', 'user_id']) }} AS participant_fact_key,
    -- Audit fields
    CURRENT_TIMESTAMP() AS dbt_loaded_at,
    CURRENT_USER() AS dbt_loaded_by
  FROM {{ ref('silver_participants') }}
  WHERE participant_id IS NOT NULL
    AND meeting_id IS NOT NULL
    AND join_time IS NOT NULL
  
  {% if is_incremental() %}
    -- Incremental logic: only process new or updated records
    AND (created_at > (SELECT MAX(created_at) FROM {{ this }})
         OR updated_at > (SELECT MAX(updated_at) FROM {{ this }}))
  {% endif %}
),

participant_metrics AS (
  SELECT 
    pb.*,
    -- Calculate engagement metrics
    CASE 
      WHEN duration > 0 THEN camera_on_duration / duration * 100
      ELSE 0 
    END AS camera_usage_percentage,
    
    CASE 
      WHEN duration > 0 THEN microphone_on_duration / duration * 100
      ELSE 0 
    END AS microphone_usage_percentage,
    
    CASE 
      WHEN duration > 0 THEN screen_share_duration / duration * 100
      ELSE 0 
    END AS screen_share_percentage,
    
    -- Engagement categories
    CASE 
      WHEN attentiveness_score >= 0.8 THEN 'High'
      WHEN attentiveness_score >= 0.6 THEN 'Medium'
      WHEN attentiveness_score >= 0.4 THEN 'Low'
      ELSE 'Very Low'
    END AS engagement_level,
    
    -- Time-based dimensions
    DATE(join_time) AS participation_date,
    EXTRACT(HOUR FROM join_time) AS join_hour,
    EXTRACT(DOW FROM join_time) AS day_of_week
    
  FROM participant_base pb
),

final AS (
  SELECT 
    participant_fact_key,
    participant_id,
    meeting_id,
    user_id,
    user_name,
    user_email,
    join_time,
    leave_time,
    duration,
    attentiveness_score,
    camera_on_duration,
    microphone_on_duration,
    screen_share_duration,
    participant_type,
    device_type,
    ip_address,
    location,
    camera_usage_percentage,
    microphone_usage_percentage,
    screen_share_percentage,
    engagement_level,
    participation_date,
    join_hour,
    day_of_week,
    created_at,
    updated_at,
    dbt_loaded_at,
    dbt_loaded_by,
    -- Data quality score
    CASE 
      WHEN user_name IS NOT NULL 
           AND duration > 0 
           AND attentiveness_score IS NOT NULL 
           AND device_type IS NOT NULL 
      THEN 1.0
      ELSE 0.7
    END AS data_quality_score
  FROM participant_metrics
  WHERE data_quality_score >= {{ var('min_quality_score') }}
)

SELECT * FROM final
