{% macro generate_audit_id() %}
  {{ dbt_utils.generate_surrogate_key(['CURRENT_TIMESTAMP()', 'RANDOM()']) }}
{% endmacro %}
