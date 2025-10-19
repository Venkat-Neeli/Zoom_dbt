{% macro generate_audit_columns() %}
    CURRENT_TIMESTAMP() AS created_at,
    CURRENT_TIMESTAMP() AS updated_at,
    'ACTIVE' AS process_status
{% endmacro %}
