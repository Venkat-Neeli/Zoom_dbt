{% macro generate_audit_columns() %}
    CURRENT_TIMESTAMP() as load_timestamp,
    CURRENT_TIMESTAMP() as update_timestamp,
    CURRENT_DATE() as load_date,
    CURRENT_DATE() as update_date,
    'DBT_PIPELINE' as source_system
{% endmacro %}
